package tem

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/sophon-lab/temsearch/pkg/concept/logmsg"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/mem"
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
	stat         *station
}

func NewHead(alloc byteutil.Allocator, chunkRange int64, num, bufLen int) *Head {
	h := &Head{
		mint: math.MinInt64,
		MaxT: math.MinInt64,
	}
	h.indexMem = mem.NewMemTable(byteutil.NewInvertedBytePool(alloc))
	h.logsMem = mem.NewLogsTable(byteutil.NewForwardBytePool(alloc))
	h.chunkRange = chunkRange
	h.stat = newStation(num, bufLen, nil)
	return h
}

//add some logs
func (h *Head) addLogs(l logmsg.LogMsgArray) error {
	return h.stat.addLogs(l)
}

//read a log
func (h *Head) readLog(id uint64) []byte {
	return h.logsMem.ReadLog(id)
}

func (h *Head) getLog() *logmsg.LogMsg {
	return h.stat.Pull()
}

func (h *Head) process() {
	for {
		// log := h.getLog()
	}
}

func (h *Head) chew() {
	for {

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
