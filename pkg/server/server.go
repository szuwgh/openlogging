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
	opts.RetentionDuration = 12 * 60 * 60
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
	//now := time.Now().Unix()
	// if maxt == 0 {
	// 	maxt = now + 24*24*60*60
	// }
	// if mint == 0 {
	// 	mint = now - 24*24*60*60
	// }
	log.Println(mint, maxt) //1631165081 1631175881
	searcher, err := s.eg.Searcher(mint, maxt)
	if err != nil {
		return nil, err
	}
	defer searcher.Close()
	e, ok := expr.(*temql.VectorSelector)
	if !ok {
		return nil, nil
	}
	log.Println(e.LabelMatchers)

	var series []Series
	seriesSet := searcher.Search(e.LabelMatchers, e.Expr, mint, maxt)
	for seriesSet.Next() {
		se := seriesSet.At()
		metric := Series{Metric: se.Labels()}
		iter := se.Iterator()
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
	resp := prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}
	for i, q := range req.Queries {
		v, err := s.query(q)
		if err == nil {
			resp.Results[i] = &prompb.QueryResult{}
			resp.Results[i].Timeseries = v
		}
	}
	log.Println(resp.String())
	return &resp
}

//0--10
//step = 2
//0-2 2-4 4-6 6-8 8-10
func (s *Server) query(q *prompb.Query) ([]*prompb.TimeSeries, error) {
	start := q.StartTimestampMs / 1000
	end := q.EndTimestampMs / 1000
	stept := q.Hints.StepMs / 1000
	log.Println(start, time.Unix(start, 0).Format("2006-01-02 15:04:05"), end, time.Unix(end, 0).Format("2006-01-02 15:04:05"))
	searcher, err := s.eg.Searcher(start, end)
	if err != nil {
		return nil, err
	}
	defer searcher.Close()
	//var series []Series
	var series []*prompb.TimeSeries
	rt := rangeTime(start, end, stept)
	seriesSet := searcher.Search(q.Matchers, nil, start, end)
	for seriesSet.Next() {
		se := seriesSet.At()
		labels := se.Labels()
		iter := se.Iterator()
		promLabels := make([]*prompb.Label, len(labels))
		for i, v := range labels {
			promLabels[i] = &prompb.Label{}
			promLabels[i].Name = v.Name
			promLabels[i].Value = v.Value
		}
		tss := &prompb.TimeSeries{}
		tss.Labels = promLabels
		var samples []prompb.Sample
		i := 0

		for iter.Next() {

			t, _, _, _ := iter.At()
			if t < rt[i] {

			}
			//log.Println(time.Unix(t, 0).Format("2006-01-02 15:04:05"), v, pos, string(b))
			//metric.Logs = append(metric.Logs, Log{t, v, highlight(pos, byteutil.Byte2Str(b))})

		}
		tss.Samples = samples
		series = append(series, tss)
	}

	return series, nil
}

func rangeTime(start, end int64, step int64) []int64 {
	r := (end - start)
	stepTime := step
	s := r / stepTime
	var rt []int64
	for i := int64(0); i <= s; i++ {
		start = start + stepTime
		rt = append(rt, start)
	}
	return rt
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
