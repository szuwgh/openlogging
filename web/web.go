package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"text/template"
	"time"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"

	"github.com/sophon-lab/temsearch/pkg/analysis"
	"github.com/sophon-lab/temsearch/pkg/engine/tem"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/labels"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/search"
)

var ok = []byte("ok")

type Handler struct {
	eg *tem.Engine
}

func New() *Handler {
	var err error
	h := &Handler{}
	h.eg, err = tem.NewEngine(analysis.NewAnalyzer("gojieba"))
	if err != nil {
		return nil
	}
	return h
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

func (h *Handler) index(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write(toErrResult(500, err.Error()))
		return
	}
	// var logs logmsg.LogMsgArray
	// err = json.Unmarshal(b, &logs)
	// if err != nil {
	// 	w.Write(toErrResult(500, err.Error()))
	// 	return
	// }
	err = h.eg.Index(b)
	if err != nil {
		w.Write(toErrResult(500, err.Error()))
		return
	}
	w.Write(ok)
	//	fmt.Fprintf(w, "Hello Index!") //这个写入到w的是输出到客户端的
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	//r.ParseForm()
	// s := r.FormValue("start_time")
	// mint, err := parseTime(s)
	// if err != nil {
	// 	w.Write(toErrResult(500, err.Error()))
	// 	return
	// }
	// e := r.FormValue("end_time")
	// maxt, err := parseTime(e)
	// if err != nil {
	// 	w.Write(toErrResult(500, err.Error()))
	// 	return
	// }

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write(toErrResult(500, err.Error()))
		return
	}
	// fmt.Fprintf(w, string(b))
	// return
	var sql *search.QueryESL
	err = json.Unmarshal(b, &sql)
	if err != nil {
		w.Write(toErrResult(500, err.Error()))
		return
	}
	// sql:=search.QueryESL{Tags: map[string]string{
	// 			"job": "wo2",
	// 		},
	// 			Term:    [][]byte{[]byte("a")},
	// 			Opt:     "and",
	// 			MinTime: now - 1000000,
	// 			MaxTime: end,
	// 		})
	now := time.Now().Unix()
	if sql.MaxTime == 0 {
		sql.MaxTime = now + 24*60*60
	}
	if sql.MinTime == 0 {
		sql.MinTime = now - 24*60*60
	}
	searcher, err := h.eg.Searcher(sql.MinTime, sql.MaxTime)
	if err != nil {
		w.Write(toErrResult(500, err.Error()))
		return
	}
	var series []Series
	//sql.MinTime = now - 1000000
	//sql.MaxTime = now
	seriesSet := searcher.Search(sql)
	for seriesSet.Next() {
		s := seriesSet.At()
		metric := Series{Metric: s.Labels()}
		log.Println(s.Labels())
		iter := s.Iterator()
		for iter.Next() {
			t, v, pos, b := iter.At()
			metric.Logs = append(metric.Logs, Log{t, v, highlight(pos, byteutil.Byte2Str(b))})
			//log.Println(t, v, pos, string(b))
		}
		series = append(series, metric)
	}
	res, err := json.Marshal(series)
	if err != nil {
		w.Write(toErrResult(500, err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(res)
	//fmt.Fprintf(w, "Hello Search!") //这个写入到w的是输出到客户端的
}

func highlight(pos []int, b string) string {
	if len(pos) == 0 {
		return b
	}
	return b
	// for _, v := range pos {
	// 	//strings.ReplaceAll(s, old, new)
	// }
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func graph(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.ParseFiles("./web/ui/page/graph.html"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(w, nil)
}

func (h *Handler) Run() {
	http.Handle("/static/", http.StripPrefix("/static", http.FileServer(http.Dir("./web/ui/static"))))
	http.HandleFunc("/graph", graph)     //设置访问的路由
	http.HandleFunc("/index", h.index)   //设置访问的路由
	http.HandleFunc("/search", h.search) //设置访问的路由
	log.Println("server start:", 9400)
	err := http.ListenAndServe(":9400", nil) //设置监听的端口
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
