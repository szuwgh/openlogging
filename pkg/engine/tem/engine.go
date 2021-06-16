package tem

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/sophon-lab/temsearch/pkg/analysis"
	"github.com/sophon-lab/temsearch/pkg/concept/logmsg"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/disk"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/mem"
)

const (
	//NOMOREDOCS   = 0x7fffffff
	skipInterval = 3

	DefaultCacheSnapshotMemorySize = 25 * 1024 * 1024 // 25MB

	metaFilename = "meta.json"

	maxBlockDuration = 60 //1 * 60 * 60 //1 * 60 * 60 //2h

	flushWritecoldDuration = 60
)

// Options of the DB storage.
type Options struct {
	// The interval at which the write ahead log is flushed to disk.
	WALFlushInterval time.Duration

	// Duration of persisted data to keep.
	RetentionDuration uint64

	// The sizes of the Blocks.
	BlockRanges []int64

	// NoLockfile disables creation and consideration of a lock file.
	NoLockfile bool
}

type Engine struct {
	tOps      *disk.TableOps
	blocks    []*Block
	lastLogID uint64
	opts      *Options
	a         *analysis.Analyzer
	mu        sync.RWMutex

	memMu           sync.RWMutex
	nextID          uint64
	alloc           byteutil.Allocator
	head, frozeHead *Head
	walFile         []string
	frozeWalFiles   []string
	dataDir         string
	compactor       *leveledCompactor
	wg              sync.WaitGroup
	indexChan       chan logmsg.LogMsgArray
	done            chan struct{}
	headPool        chan *Head
	walDir          string
	wal             Wal
}

func NewEngine(a *analysis.Analyzer) (*Engine, error) {
	//读取域元信息 这里先不读取
	e := &Engine{}
	e.a = a
	e.alloc = byteutil.NewByteBlockAllocator()
	e.tOps = disk.NewTableOps()
	e.dataDir = "E:\\goproject\\temsearch2\\src\\data"
	e.opts = &Options{RetentionDuration: 12 * 60 * 60, BlockRanges: exponentialBlockRanges(maxBlockDuration, 10, 3)} //15d
	e.walDir = filepath.Join(e.dataDir, "wal")
	e.headPool = make(chan *Head, 1)
	e.head = NewHead(e.alloc, e.opts.BlockRanges[0])
	e.head.open()
	err := e.recoverWal()
	if err != nil {
		return nil, err
	}
	e.compactor = newLeveledCompactor(e.opts.BlockRanges) //&leveledCompactor{}
	err = e.reload()
	if err != nil {
		return nil, err
	}

	e.done = make(chan struct{})

	//go e.process()
	lastWal, err := disk.LastSequenceFile(e.walDir)
	if err != nil {
		return nil, err
	}
	e.wal, err = newWalWriter(lastWal)
	if err != nil {
		return nil, err
	}
	go e.compact()
	return e, nil
}

func (e *Engine) recoverWal() error {
	//walDir := filepath.Join(e.dataDir, "wal")
	err := os.MkdirAll(e.walDir, 0777)
	if err != nil {
		return err
	}
	walFiles, err := disk.SequenceFiles(e.walDir)
	if err != nil {
		return err
	}
	buf := &byteutil.Buffer{}
	for _, fname := range walFiles {
		e.walFile = append(e.walFile, fname)
		f, err := os.OpenFile(fname, os.O_RDONLY, 0644)
		if err != nil {
			continue
		}
		walr := newWalReader()
		walr.reset(f)
		for walr.next() {
			buf.Reset()
			n, err := buf.ReadFrom(walr)
			if err != nil || n == 0 {
				fmt.Println(err, n)
				break
			}
			e.recoverMemDB(buf.Bytes())
		}
		f.Close()
		e.shouldCompact()
	}
	return nil
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

func (e *Engine) openBlock(dir string) (*Block, error) {
	meta, err := readMetaFile(dir)
	if err != nil {
		return nil, err
	}

	ir := e.tOps.CreateIndexReader(dir, meta.ULID, meta.MinTime)
	//	ir.SetValueOffset(meta.Invert)
	wr := e.tOps.CreateLogReader(dir, meta.ULID, meta.LogID)
	b := &Block{
		meta:   *meta,
		indexr: ir,
		logr:   wr,
	}
	return b, nil
}

func (e *Engine) getBlock(id ulid.ULID) (*Block, bool) {
	for _, b := range e.blocks {
		if b.meta.ULID == id {
			return b, true
		}
	}
	return nil, false
}

func (e *Engine) compactBlock(dirs []string) ([]BlockReader, []*BlockMeta) {
	var br []BlockReader
	var metas []*BlockMeta
	for _, d := range dirs {

		meta, err := readMetaFile(d)
		if err != nil {
			return nil, nil
		}

		b, ok := e.getBlock(meta.ULID)
		if !ok {
			b, err = e.openBlock(d)
			if err != nil {
				return nil, nil
			}
			//defer b.Close()
		}

		metas = append(metas, meta)
		br = append(br, b)
	}
	return br, metas
}

func (e *Engine) reload() error {
	dirs, err := blockDirs(e.dataDir)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return nil
	}
	var (
		blocks     []*Block
		corrupted  = map[ulid.ULID]error{}
		deleteable = map[ulid.ULID]struct{}{}
		opened     = map[ulid.ULID]struct{}{}
	)
	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		if err != nil {
			ulid, err2 := ulid.Parse(filepath.Base(dir))
			if err2 != nil {
				continue
			}
			corrupted[ulid] = err
			continue
		}
		if e.beyondRetention(meta) {
			deleteable[meta.ULID] = struct{}{}
			continue
		}
		for _, b := range meta.Compaction.Parents {
			deleteable[b] = struct{}{}
		}
	}

	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		if err != nil {
			return errors.Wrapf(err, "read meta information %s", dir)
		}
		// Don't load blocks that are scheduled for deletion.
		if _, ok := deleteable[meta.ULID]; ok {
			continue
		}
		b, ok := e.getBlock(meta.ULID)
		if !ok {
			b, err = e.openBlock(dir)
			if err != nil {
				return errors.Wrapf(err, "open block %s", dir)
			}
		}
		blocks = append(blocks, b)
		opened[meta.ULID] = struct{}{}
	}

	e.mu.Lock()
	e.releaseFroze()
	oldBlocks := e.blocks
	e.blocks = blocks
	e.mu.Unlock()
	for _, b := range oldBlocks {
		if _, ok := opened[b.meta.ULID]; ok {
			continue
		}
		b.Close()
	}
	for ulid := range deleteable {
		if err := e.tOps.RemoveIndexReader(filepath.Join(e.dataDir, ulid.String()), ulid); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) beyondRetention(meta *BlockMeta) bool {
	if e.opts.RetentionDuration == 0 {
		return false
	}

	e.mu.RLock()
	blocks := e.blocks[:]
	e.mu.RUnlock()

	if len(blocks) == 0 {
		return false
	}

	last := blocks[len(e.blocks)-1]
	mint := last.meta.MaxTime - int64(e.opts.RetentionDuration)

	return meta.MaxTime < mint
}

func (e *Engine) releaseFroze() {
	if e.frozeHead == nil {
		return
	}
	recycle := e.alloc.Len()
	var alloc int
	e.frozeHead.release(&recycle, &alloc)
	e.recoHead(e.frozeHead)
	e.frozeHead = nil
	recycle = e.alloc.Len()

}

func (e *Engine) GetNextID() uint64 {
	e.nextID++
	return e.nextID
}

func TraceAll() {
	for i := 1; ; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fmt.Println(file, line)
	}
}

func (e *Engine) Index(b []byte) error {
	return e.index(b)
}

func (e *Engine) index(b []byte) error {
	defer func() {
		if err := recover(); err != nil {
			TraceAll()
		}
	}()
	var logs logmsg.LogMsgArray
	err := json.Unmarshal(b, &logs)
	if err != nil {
		return err
	}
	nowt := time.Now().Unix()
	context := mem.Context{}
	e.memMu.Lock()
	defer e.memMu.Unlock()
	err = e.wal.log(b)
	if err != nil {
		return err
	}
	return e.addToMemDB(nowt, &context, logs)
}

func (e *Engine) recoverMemDB(b []byte) error {
	var logs logmsg.LogMsgArray
	err := json.Unmarshal(b, &logs)
	if err != nil {
		return err
	}
	nowt := time.Now().Unix()
	context := mem.Context{}
	return e.addToMemDB(nowt, &context, logs)
}

func (e *Engine) addToMemDB(nowt int64, context *mem.Context, logs logmsg.LogMsgArray) error {
	head := e.getIndexHead()
	head.setMinTime(nowt)
	for _, log := range logs {
		log.TimeStamp = nowt
		head.logsMem.WriteLog(byteutil.Str2bytes(log.Msg))
		head.indexMem.Index(context, e.a, log)
	}
	e.indexCommit(head, nowt)
	return nil
}

func (e *Engine) getIndexHead() *Head {
	//e.head.startIndex()
	return e.head
}

func (e *Engine) getHeads() (h, f *Head) {
	e.memMu.RLock()
	defer e.memMu.RUnlock()
	return e.head, e.frozeHead
}

//
func (e *Engine) indexCommit(h *Head, maxt int64) {
	e.flush(h)
	h.setMaxTime(maxt)
}

func (e *Engine) allocHead() *Head {
	var h *Head
	select {
	case h = <-e.headPool:
	default:
	}
	if h == nil {
		h = NewHead(e.alloc, e.opts.BlockRanges[0])
	}
	h.open()
	return h
}

func (e *Engine) recoHead(h *Head) {
	select {
	case e.headPool <- h:
	default:
	}
}

func (e *Engine) compact() {
	defer e.wg.Done()
	for {
		select {
		case <-e.done:
			return
		default:
			e.shouldCompact()

		}
		time.Sleep(time.Second)
	}
}

func (e *Engine) shouldCompact() error {
	if !e.ShouldCompactMem(e.head) {
		return nil
	}
	e.memMu.Lock()
	if e.wal != nil {
		err := e.wal.close()
		if err != nil {
			e.memMu.Unlock()
			return err
		}
		nextFile, _, err := disk.NextSequenceFile(e.walDir)
		if err != nil {
			e.memMu.Unlock()
			return err
		}
		err = e.wal.reset(nextFile)
		if err != nil {
			e.memMu.Unlock()
			return err
		}
	}
	e.frozeWalFiles = e.walFile //append(e.frozeWalFiles, e.walFile)
	e.walFile = e.walFile[:0]
	e.frozeHead = e.head
	e.head = e.allocHead() //NewHead(e.alloc)
	e.head.lastSegNum = e.frozeHead.lastSegNum + e.frozeHead.LogNum()
	e.memMu.Unlock()
	err := e.mcompact()
	if err != nil {
		return err
	}
	e.deleteFrozeWal()
	//索引完成
	e.compactionCommit()
	err = e.tcompact()
	if err != nil {
		return err
	}
	return nil
}

func (e *Engine) deleteFrozeWal() {
	for _, f := range e.frozeWalFiles {
		os.RemoveAll(f)
	}
}

func (e *Engine) tcompact() error {
	for {
		plan, err := e.compactor.Plan(e.dataDir)
		if err != nil {
			return errors.Wrap(err, "plan compaction")
		}
		if len(plan) == 0 {
			break
		}
		b, m := e.compactBlock(plan)
		if err = e.compactor.Compact(e.dataDir, b, m); err != nil {
			return errors.Wrapf(err, "compact %s", plan)
		}
		e.compactionCommit()
	}
	return nil
}

func (e *Engine) compactionCommit() {
	runtime.GC()
	e.reload()
	runtime.GC()
}

func (e *Engine) ShouldCompactMem(h *Head) bool {
	if h.MinTime() == math.MinInt64 {
		return false
	}
	sz := e.head.size()
	if sz == 0 {
		return false
	}
	if sz > DefaultCacheSnapshotMemorySize {
		return true
	}
	return h.MaxTime()-h.MinTime() > maxBlockDuration || time.Now().Unix()-h.MaxTime() > flushWritecoldDuration
}

// func (e *Engine) Add(logs logmsg.LogMsgArray) int64 {
// 	//先写入
// 	now := e.writeWal(logs)
// 	e.indexChan <- logs
// 	return now
// }

func (e *Engine) Searcher(mint, maxt int64) (Searcher, error) {
	var blocks []BlockReader
	var segNums []uint64
	e.mu.RLock()
	defer e.mu.RUnlock()
	var segNum uint64
	for _, b := range e.blocks {
		m := b.meta
		if intervalOverlap(mint, maxt, m.MinTime, m.MaxTime) {
			blocks = append(blocks, b)
			segNums = append(segNums, segNum)
		}
		segNum += b.LogNum()
	}
	h, f := e.getHeads()
	if f != nil {
		if intervalOverlap(mint, maxt, e.frozeHead.mint, e.frozeHead.MaxT) {
			blocks = append(blocks, f)
			segNums = append(segNums, segNum)
		}
		segNum += f.LogNum()

	}
	if maxt >= h.MinTime() {
		blocks = append(blocks, h)
		segNums = append(segNums, segNum)
	}
	s := &searcher{bs: make([]Searcher, 0, len(blocks))}
	for i, b := range blocks {
		q := NewBloctemsearcher(b)
		q.lastSegNum = segNums[i]
		if q != nil {
			s.bs = append(s.bs, q)
		}
	}
	return s, nil
}

func intervalOverlap(amin, amax, bmin, bmax int64) bool {
	//from prometheus
	return amin <= bmax && bmin <= amax
}

func (e *Engine) mcompact() error {
	if e.frozeHead == nil {
		return nil
	}
	//等待索引完成
	e.frozeHead.waitIndex()
	//meta := newMeta(e.frozeHead.mint, e.frozeHead.MaxT)
	return e.compactor.Write(e.dataDir, e.frozeHead, e.frozeHead.mint, e.frozeHead.MaxT)
	//return e.compactor.write(e.dataDir, meta, e.frozeHead)
}

func newMeta(minT, maxT int64) *BlockMeta {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)
	meta := &BlockMeta{
		ULID:    uid,
		MinTime: minT,
		MaxTime: maxT,
	}
	return meta
}

func chunkDir(dir string) string {
	return filepath.Join(dir, "logs")
}

func indexDir(dir string) string {
	return filepath.Join(dir, "index")
}

func (e *Engine) flush(h *Head) {
	h.indexMem.Flush()
	//e.rotateMem()
}

// The MultiError type implements the error interface, and contains the
// Errors used to construct it.
type MultiError []error

// Returns a concatenated string of the contained errors
func (es MultiError) Error() string {
	var buf bytes.Buffer

	if len(es) > 1 {
		fmt.Fprintf(&buf, "%d errors: ", len(es))
	}

	for i, err := range es {
		if i != 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(err.Error())
	}

	return buf.String()
}

// Add adds the error to the error list if it is not nil.
func (es *MultiError) Add(err error) {
	if err == nil {
		return
	}
	if merr, ok := err.(MultiError); ok {
		*es = append(*es, merr...)
	} else {
		*es = append(*es, err)
	}
}

// Err returns the error list as an error or nil if it is empty.
func (es MultiError) Err() error {
	if len(es) == 0 {
		return nil
	}
	return es
}