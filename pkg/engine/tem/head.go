package tem

import (
	"log"
	"math"
	"sync"
	"sync/atomic"

	"github.com/sophon-lab/temsearch/pkg/temql"

	"github.com/sophon-lab/temsearch/pkg/analysis"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/mem"
	"github.com/sophon-lab/temsearch/pkg/lib/logproto"
)

type Head struct {
	rwControl
	mint          int64
	MaxT          int64
	indexMem      *mem.MemTable
	logsMem       *mem.LogsTable
	indexControl  sync.WaitGroup
	chunkRange    int64
	lastSegNum    uint64
	stat          *station
	a             *analysis.Analyzer
	EndID         uint64
	startChan     chan struct{}
	headStartChan chan struct{}
	isWaitfroze   bool
	logSize       uint64
}

func NewHead(alloc byteutil.Allocator, chunkRange int64, num, bufLen int, compactChan chan struct{}, a *analysis.Analyzer) *Head {
	h := &Head{
		mint: math.MinInt64,
		MaxT: math.MinInt64,
	}
	h.indexMem = mem.NewMemTable(byteutil.NewInvertedBytePool(alloc))
	h.logsMem = mem.NewLogsTable(byteutil.NewForwardBytePool(alloc))
	h.chunkRange = chunkRange
	h.a = a
	h.stat = newStation(num, bufLen, h.serieser, h.tokener)
	h.startChan = make(chan struct{}, 1)
	go h.process(compactChan)
	return h
}

//add some logs
func (h *Head) addLogs(r logproto.Stream) error {
	return h.stat.addLogs(r)
}

//read a log
func (h *Head) readLog(id uint64) []byte {
	return h.logsMem.ReadLog(id)
}

func (h *Head) getLog() mem.LogSummary {
	return h.stat.Pull()
}

func (h *Head) process(compactChan chan struct{}) {
	context := mem.Context{}
	for {
		if h.isWaitfroze {
			<-h.startChan
			h.isWaitfroze = false
		}
		logSumm := h.getLog()
		if logSumm.DocID == h.EndID {
			h.headStartChan <- struct{}{}
			compactChan <- struct{}{}
			continue
		}
		h.logsMem.WriteLog(logSumm.Msg)
		h.indexMem.Index(&context, logSumm)
		h.indexMem.Flush()
	}
}

func (h *Head) serieser(labels string) *mem.MemSeries {
	log.Println(labels)
	lset, _ := temql.ParseLabels(labels)
	s, _ := h.indexMem.GetOrCreate(lset.Hash(), lset)
	//lset := labels.FromMap(log.Tags)
	return s
}

func (h *Head) tokener(entry *logproto.Entry) mem.LogSummary {
	//lset := labels.FromMap(log.Tags)
	//s, _ := h.indexMem.GetOrCreate(lset.Hash(), lset)
	msg := byteutil.Str2bytes(entry.Line)
	tokens := h.a.Analyze(msg)
	return mem.LogSummary{
		DocID: entry.LogID,
		//Series:    s,
		Tokens:    tokens,
		TimeStamp: entry.Timestamp.UnixNano() / 1e6,
		//Lset:      lset,
		Msg: msg,
	}
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
	h.EndID = 0
	h.isWaitfroze = false
	h.logSize = 0
	h.stat.forwardID = 1
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

func (h *Head) startIndex() {
	h.indexControl.Add(1)
}

func (h *Head) doneIndex() {
	h.indexControl.Done()
}

func (h *Head) waitIndex() {
	h.indexControl.Wait()
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
