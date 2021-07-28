package server

import (
	"encoding/json"
	"log"
	"time"

	"github.com/sophon-lab/temsearch/pkg/temql"

	"github.com/sophon-lab/temsearch/pkg/analysis"
	"github.com/sophon-lab/temsearch/pkg/engine/tem"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/labels"
)

//服务
type Server struct {
	eg *tem.Engine
}

func New() *Server {
	s := &Server{}
	var err error
	s.eg, err = tem.NewEngine(analysis.NewAnalyzer("gojieba"))
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
	Metric labels.Labels `json:"metric"`
	Logs   []Log         `json:"logs"`
}

func highlight(pos []int, b string) string {
	if len(pos) == 0 {
		return b
	}
	return b
}

func (s *Server) Search(input string, mint, maxt, count int64) ([]byte, error) {

	// var sql *search.QueryESL
	// err = json.Unmarshal(b, &sql)
	// if err != nil {
	// 	w.Write(toErrResult(500, err.Error()))
	// 	return
	// }
	expr := temql.ParseExpr(input)
	log.Println("expr", expr)
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
	var series []Series
	seriesSet := searcher.Search(expr.(temql.Expr), mint, maxt)
	for seriesSet.Next() {
		s := seriesSet.At()
		metric := Series{Metric: s.Labels()}
		iter := s.Iterator()
		for iter.Next() {
			t, v, pos, b := iter.At()
			metric.Logs = append(metric.Logs, Log{t, v, highlight(pos, byteutil.Byte2Str(b))})
		}
		series = append(series, metric)
	}
	res, err := json.Marshal(series)
	if err != nil {
		return nil, err
	}
	return res, nil
}
