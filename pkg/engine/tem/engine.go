package tem

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/szuwgh/temsearch/pkg/engine/tem/byteutil"
	"github.com/szuwgh/temsearch/pkg/lib/logproto"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/szuwgh/temsearch/pkg/analysis"
	"github.com/szuwgh/temsearch/pkg/concept/logmsg"
	"github.com/szuwgh/temsearch/pkg/engine/tem/disk"
)

const (
	//NOMOREDOCS   = 0x7fffffff
	//skipInterval = 3

	DefaultCacheSnapshotMemorySize = 25 * 1024 * 1024 // 25MB

	metaFilename = "meta.json"

	//MaxBlockDuration = 30 //1 * 60 * 60 //1 * 60 * 60 //2h

	//flushWritecoldDuration = 60
)

type Options struct {
	RetentionDuration uint64

	BlockRanges []int64

	IndexBufferNum int

	IndexBufferLength int

	DataDir string

	MaxBlockDuration int64

	FlushWritecoldDuration int64

	DefaultCacheSnapshotMemorySize int

	SkipInterval int
}

type Engine struct {
	tOps   *disk.TableOps
	blocks []*Block
	//lastLogID uint64
	//opts      *Options
	a  *analysis.Analyzer
	mu sync.RWMutex

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
	compactChan     chan struct{}
	walDir          string
	wal             Wal
	opts            *Options
}

func NewEngine(opt *Options, a *analysis.Analyzer) (*Engine, error) {
	e := &Engine{}
	e.a = a
	e.opts = opt
	e.alloc = byteutil.NewByteBlockAllocator()
	e.tOps = disk.NewTableOps()
	e.dataDir = opt.DataDir
	e.walDir = filepath.Join(e.dataDir, "wal")
	e.headPool = make(chan *Head, 1)
	e.compactChan = make(chan struct{}, 1)
	e.compactor = newLeveledCompactor(e.opts.BlockRanges)
	e.head = e.newHead()
	e.head.open()
	err := e.recoverWal()
	if err != nil {
		return nil, err
	}

	err = e.reload()
	if err != nil {
		return nil, err
	}

	e.done = make(chan struct{})

	lastWal, err := disk.LastSequenceFile(e.walDir)
	if err != nil {
		return nil, err
	}
	e.wal, err = newWalWriter(lastWal)
	if err != nil {
		return nil, err
	}
	e.walFile = append(e.walFile, lastWal)
	go e.compact()
	return e, nil
}

func (e *Engine) newHead() *Head {
	return NewHead(e.alloc, e.opts.BlockRanges[0], e.opts.IndexBufferNum, e.opts.IndexBufferLength, e.compactChan, e.a)
}

func (e *Engine) recoverWal() error {

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
				log.Println(err, n)
				break
			}
			e.recoverMemDB(buf.Bytes())
		}
		f.Close()
		e.shouldCompact()
	}
	return nil
}

func (e *Engine) openBlock(dir string) (*Block, error) {
	meta, err := readMetaFile(dir)
	if err != nil {
		return nil, err
	}

	ir := e.tOps.CreateIndexReader(dir, meta.ULID, meta.MinTime)
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
			log.Println("beyondRetention", meta.ULID.String())
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
	oldHead := e.frozeHead
	e.frozeHead = nil
	oldBlocks := e.blocks
	e.blocks = blocks
	e.mu.Unlock()
	e.releaseFroze(oldHead)
	for _, b := range oldBlocks {
		if _, ok := opened[b.meta.ULID]; ok {
			continue
		}
		b.Close()
	}
	for ulid := range deleteable {
		log.Println("delete", ulid.String())
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

func (e *Engine) releaseFroze(frozeHead *Head) {
	if frozeHead == nil {
		return
	}
	recycle := e.alloc.Len()
	var alloc int
	frozeHead.release(&recycle, &alloc)
	e.recoHead(frozeHead)
	frozeHead = nil
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

func (e *Engine) Index(compressed []byte) error {
	return e.index(compressed)
}

func (e *Engine) index(compressed []byte) error {

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Println("msg", "Decode error", "err", err.Error())
		return err
	}

	var req logproto.PushRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Println("msg", "Unmarshal error", "err", err.Error())
		return err
	}
	e.memMu.Lock()
	defer e.memMu.Unlock()
	err = e.wal.log(compressed)
	if err != nil {
		log.Println(err)
		return err
	}
	return e.addToMemDB(req)

}

func (e *Engine) recoverMemDB(b []byte) error {
	reqBuf, err := snappy.Decode(nil, b)
	if err != nil {
		log.Println("msg", "Decode error", "err", err.Error())
		return err
	}
	var req logproto.PushRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Println("msg", "Unmarshal error", "err", err.Error())
		return err
	}
	return e.addToMemDB(req)
}

func (e *Engine) addToMemDB(r logproto.PushRequest) error {
	head := e.getIndexHead()

	for i := range r.Streams {
		for j := range r.Streams[i].Entries {
			r.Streams[i].Entries[j].LogID = e.GetNextID()
		}
		head.setMinTime(r.Streams[i].Entries[0].Timestamp.UnixNano() / 1e6)
		head.addLogs(r.Streams[i])
		head.setMaxTime(r.Streams[i].Entries[len(r.Streams[i].Entries)-1].Timestamp.UnixNano() / 1e6)
	}
	atomic.AddUint64(&head.logSize, uint64(r.XXX_Size()))
	return nil
}

func (e *Engine) getIndexHead() *Head {
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
		h = e.newHead()
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
		time.Sleep(1 * time.Second)
	}
}

func (e *Engine) shouldCompact() error {
	if !e.ShouldCompactMem(e.head) {
		return nil
	}
	var endID uint64
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
		e.frozeWalFiles = append(e.frozeWalFiles, e.walFile...)
		e.walFile = e.walFile[:0]
		e.walFile = append(e.walFile, nextFile)
	} else {
		e.frozeWalFiles = append(e.frozeWalFiles, e.walFile...)
		e.walFile = e.walFile[:0]
	}
	e.frozeHead = e.head
	e.head = e.allocHead() //NewHead(e.alloc)
	e.head.isWaitfroze = true
	e.frozeHead.headStartChan = e.head.startChan
	e.head.lastSegNum = e.frozeHead.lastSegNum + e.frozeHead.LogNum()
	endID = e.GetNextID()
	e.frozeHead.EndID = endID
	e.nextID = 0
	e.memMu.Unlock()
	logs := logproto.Stream{Entries: []logproto.Entry{logproto.Entry{LogID: endID}}}
	//notification needs to be written to disk
	log.Println("add end logs")
	e.frozeHead.addLogs(logs)
	for {
		select {
		case <-e.compactChan:
			log.Println("do compact")
			return e.doCompact()

		}
	}
}

func (e *Engine) doCompact() error {
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
	e.frozeWalFiles = e.frozeWalFiles[:0]
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
	return h.MaxTime()-h.MinTime() > e.opts.MaxBlockDuration || time.Now().UnixNano()/1e6-h.MaxTime() > e.opts.FlushWritecoldDuration
}

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
	return e.compactor.Write(e.dataDir, e.frozeHead, e.frozeHead.mint, e.frozeHead.MaxT)
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
