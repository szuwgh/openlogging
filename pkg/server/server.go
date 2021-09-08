package server

import (
	"encoding/json"
	"log"
	"time"

	"github.com/sophon-lab/temsearch/pkg/lib/prompb"
	"github.com/sophon-lab/temsearch/pkg/temql"

	"github.com/sophon-lab/temsearch/pkg/analysis"
	"github.com/sophon-lab/temsearch/pkg/engine/tem"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/labels"
)

type Config struct {
	RetentionDuration uint64

	IndexBufferNum int

	IndexBufferLength int

	DataDir string

	MaxBlockDuration int64

	FlushWritecoldDuration int

	DefaultCacheSnapshotMemorySize int

	SkipInterval int
}

//服务
type Server struct {
	eg *tem.Engine
}

func New() *Server {
	s := &Server{}
	var err error
	opts := &tem.Options{}
	opts.RetentionDuration = 60 * 2
	opts.MaxBlockDuration = 30
	opts.BlockRanges = exponentialBlockRanges(opts.MaxBlockDuration, 10, 3)
	opts.IndexBufferNum = 1
	opts.IndexBufferLength = 16
	opts.DataDir = "E:\\goproject\\temsearch\\data" //config.DataDir
	//config.MaxBlockDuration
	opts.FlushWritecoldDuration = 25 * 1024 * 1024 //config.FlushWritecoldDuration
	opts.DefaultCacheSnapshotMemorySize = 60       //config.DefaultCacheSnapshotMemorySize
	s.eg, err = tem.NewEngine(opts, analysis.NewAnalyzer("gojieba"))
	if err != nil {
		return nil
	}
	return s
}

func (s *Server) Index(b []byte) error {
	err := s.eg.Index(b)
	if err != nil {
		return err
	}
	return nil
}

type Log struct {
	T int64  `json:"timestamp"`
	V uint64 `json:"id"`
	B string `json:"log"`
}

type Series struct {
	Metric     labels.Labels `json:"metric"`
	Logs       []Log         `json:"logs"`
	TotalCount int64         `json:"total_count"`
}

func highlight(pos []int, b string) string {
	if len(pos) == 0 {
		return b
	}
	return b
}

func (s *Server) Search(input string, mint, maxt, count int64) ([]byte, error) {

	expr := temql.ParseExpr(input)
	if expr == nil {
		return nil, nil
	}
	now := time.Now().Unix()
	if maxt == 0 {
		maxt = now + 24*24*60*60
	}
	if mint == 0 {
		mint = now - 24*24*60*60
	}
	searcher, err := s.eg.Searcher(mint, maxt)
	if err != nil {
		return nil, err
	}
	e, ok := expr.(*temql.VectorSelector)
	if !ok {
		return nil, nil
	}
	log.Println(e.LabelMatchers)
	defer searcher.Close()
	var series []Series
	seriesSet := searcher.Search(e.LabelMatchers, e.Expr, mint, maxt)
	for seriesSet.Next() {
		s := seriesSet.At()
		metric := Series{Metric: s.Labels()}
		iter := s.Iterator()
		for iter.Next() {
			if metric.TotalCount < count {
				t, v, pos, b := iter.At()
				metric.Logs = append(metric.Logs, Log{t, v, highlight(pos, byteutil.Byte2Str(b))})
			}
			metric.TotalCount++
		}
		series = append(series, metric)
	}
	res, err := json.Marshal(series)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Server) Read(req *prompb.ReadRequest) *prompb.ReadResponse {
	// resp := prompb.ReadResponse{
	// 	Results: make([]*prompb.QueryResult, len(req.Queries)),
	// }
	// for _, q := range req.Queries {

	// }
	return nil
}

func (s *Server) query(q *prompb.Query) ([]*prompb.TimeSeries, error) {
	// searcher, err := s.eg.Searcher(q.StartTimestampMs, q.EndTimestampMs)
	// if err != nil {
	// 	return nil, nil
	// }
	// var series []Series
	// seriesSet := searcher.Search(q.Matchers, nil, q.StartTimestampMs, q.EndTimestampMs)
	// for seriesSet.Next() {
	// 	s := seriesSet.At()
	// 	labels := s.Labels()
	// 	promLabels := make([]prompb.Label, 0, len(labels))
	// 	for i, v := range labels {
	// 		promLabels[i].Name = v.Name
	// 		promLabels[i].Value = v.Value
	// 	}
	// 	iter := s.Iterator()
	// 	for iter.Next() {

	// 	}
	// 	series = append(series, metric)
	// }
	return nil, nil
}

func exponentialBlockRanges(minSize int64, steps, stepSize int) []int64 {
	ranges := make([]int64, 0, steps)
	curRange := minSize
	for i := 0; i < steps; i++ {
		ranges = append(ranges, curRange)
		curRange = curRange * int64(stepSize)
	}
	return ranges
}
