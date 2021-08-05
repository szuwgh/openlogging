package web

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/sophon-lab/temsearch/util"

	"github.com/sophon-lab/temsearch/pkg/server"
)

var ok = []byte("ok")

type Handler struct {
	s *server.Server
}

func New() *Handler {
	h := &Handler{}
	h.s = server.New()
	return h
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
	err = h.s.Index(b)
	if err != nil {
		w.Write(toErrResult(500, err.Error()))
		return
	}
	w.Write(ok)
	//	fmt.Fprintf(w, "Hello Index!") //这个写入到w的是输出到客户端的
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	temql := r.Form.Get("temql")
	mint, maxt := util.Str2Int64(r.Form.Get("mint")), util.Str2Int64(r.Form.Get("maxt"))
	count := util.Str2Int64(r.Form.Get("count"))
	b, err := h.s.Search(temql, mint, maxt, count)
	if err != nil {
		w.Write(toErrResult(500, err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
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
	// b, err := Asset("web/ui/graph.html")
	// if err != nil {

	// }
	// tmpl := template.Must(template.New("base").Parse(byteutil.Byte2Str(b)))
	// w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// tmpl.Execute(w, nil)
	// b, err := Asset("web/ui/graph.html")
	// if err != nil {

	// }
	tmpl := template.Must(template.ParseFiles("web/ui/graph.html"))
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
