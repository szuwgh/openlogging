package tem

import (
	"log"
	"math"
	"sync"
	"sync/atomic"

	"github.com/szuwgh/temsearch/pkg/temql"

	"github.com/szuwgh/temsearch/pkg/analysis"
	"github.com/szuwgh/temsearch/pkg/engine/tem/byteutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/mem"
	"github.com/szuwgh/temsearch/pkg/lib/logproto"
	"github.com/szuwgh/temsearch/pkg/tokenizer"
)

type Head struct {
	rwControl
	mint         int64
	MaxT         int64
	indexMem     *mem.MemTable
	logsMem      *mem.LogsTable
	indexControl sync.WaitGroup
	chunkRange   int64
	lastSegNum   uint64
	a            *analysis.Analyzer
	isWaitfroze  bool
	logSize      uint64
}

func NewHead(alloc byteutil.Allocator, chunkRange int64, a *analysis.Analyzer, skiplistLevel, skipListInterval int) *Head {
	h := &Head{
		mint: math.MinInt64,
		MaxT: math.MinInt64,
	}
	h.indexMem = mem.NewMemTable(byteutil.NewInvertedBytePool(alloc), skiplistLevel, skipListInterval)
	h.logsMem = mem.NewLogsTable(byteutil.NewForwardBytePool(alloc))
	h.chunkRange = chunkRange
	h.a = a
	return h
}

//add some logs
func (h *Head) addLogs(r logproto.Stream) error {
	log.Println("add logs", r.Labels)
	//return h.stat.addLogs(r)
	context := mem.Context{}
	series := h.serieser(r.Labels)
	for _, e := range r.Entries {
		h.logsMem.WriteLog([]byte(e.Line))
		tokens := h.tokener(&e)
		h.indexMem.Index(&context, e.LogID, e.Timestamp.UnixNano()/1e6, series, tokens)
		h.indexMem.Flush()
	}
	return nil
}

func (h *Head) serieser(labels string) *mem.MemSeries {
	lset, _ := temql.ParseLabels(labels)
	s, _ := h.indexMem.GetOrCreate(lset.Hash(), lset)
	return s
}

func (h *Head) tokener(entry *logproto.Entry) tokenizer.Tokens {
	msg := byteutil.Str2bytes(entry.Line)
	tokens := h.a.Analyze(msg)
	return tokens
}

func (h *Head) setMinTime(t int64) {
	if h.mint == math.MinInt64 {
		atomic.StoreInt64(&h.mint, t)
		h.indexMem.SetBaseTimeStamp(t)
	}
}

func (h *Head) setMaxTime(t int64) {
	ht := h.MaxT
	atomic.CompareAndSwapInt64(&h.MaxT, ht, t)
}

func (h *Head) reset() {
	h.mint = math.MinInt64
	h.lastSegNum = 0
	h.logSize = 0
}

func (h *Head) ReadDone() {
	h.pendingReaders.Done()
}

func (h *Head) Index() IndexReader {
	h.startRead()
	return &blockIndexReader{h.indexMem, h}
}

func (h *Head) Logs() LogReader {
	h.startRead()
	return &blockLogReader{h.logsMem, h}
}

func (h *Head) MinTime() int64 {
	return atomic.LoadInt64(&h.mint)
}

func (h *Head) MaxTime() int64 {
	return atomic.LoadInt64(&h.MaxT)
}

func (h *Head) LogNum() uint64 {
	return h.indexMem.LogNum()
}

func (h *Head) LastSegNum() uint64 {
	return h.lastSegNum
}

func (h *Head) size() uint64 {
	return atomic.LoadUint64(&h.logSize)
}

func (h *Head) Close() {
	h.waitRead()
}

func (h *Head) open() {
	h.closing = false
	h.indexMem.Init()

}

func (h *Head) release(recycle, alloced *int) error {
	h.waitRead()
	h.reset()
	h.indexMem.ReleaseBuff(recycle, alloced)
	h.logsMem.ReleaseBuff(recycle, alloced)
	return nil
}
