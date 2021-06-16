package lsm

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/sophon-lab/temsearch/core/indexer/lsm/byteutil"
	"github.com/sophon-lab/temsearch/core/indexer/lsm/mem"
)

type Head struct {
	mint         int64 `json:"minTime"`
	MaxT         int64 `json:"maxTime"`
	indexMem     *mem.MemTable
	logsMem      *mem.LogsTable
	indexControl sync.WaitGroup
	rwControl
	chunkRange int64
	lastSegNum uint64
}

func NewHead(alloc byteutil.Allocator, chunkRange int64) *Head {
	//now := time.Now().Unix()
	h := &Head{
		mint: math.MinInt64,
		MaxT: math.MinInt64,
		//rwControl.
	}
	h.indexMem = mem.NewMemTable(byteutil.NewInvertedBytePool(alloc))
	h.logsMem = mem.NewLogsTable(byteutil.NewForwardBytePool(alloc))
	h.chunkRange = chunkRange
	return h
}

func (h *Head) readLog(id uint64) []byte {
	return h.logsMem.ReadLog(id)
}

func (h *Head) setMinTime(t int64) {
	if h.mint == math.MinInt64 {
		atomic.StoreInt64(&h.mint, t)
		//h.mint = h.chunkRange * (t / h.chunkRange)
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
}

func (h *Head) Index() IndexReader {
	return h.indexMem
}

func (h *Head) Logs() LogReader {
	return h.logsMem
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
	return h.indexMem.Size()
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
