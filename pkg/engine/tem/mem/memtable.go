package mem

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/index"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/index/skiplist"
	"github.com/sophon-lab/temsearch/pkg/temql"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/chunks"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/disk"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/series"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"

	"github.com/sophon-lab/temsearch/pkg/analysis"
	"github.com/sophon-lab/temsearch/pkg/concept/logmsg"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/global"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/labels"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/posting"
	temqlLabels "github.com/sophon-lab/temsearch/pkg/temql/labels"
)

type MetaIndex []int

//内存数据库
type MemTable struct {
	mutex          sync.RWMutex
	indexs         indexGroup
	bytePool       byteutil.Inverted                //byteArrayPool
	bytePoolReader *byteutil.InvertedBytePoolReader //chunks.ChunkReader //*byteutil.InvertedBytePoolReader
	skLen          int
	baseTimeStamp  int64
	msgIndex       engineFunc
	nextWriteLogID int
	flushPosting   []*RawPosting
	series         *stripeSeries
	lastSeriesID   uint64
	size           uint64
	logID          uint64
}

const (
	stripeSize = 1 << 14
	stripeMask = stripeSize - 1
)

func NewMemTable(bytePool byteutil.Inverted) *MemTable {
	mt := &MemTable{}
	mt.bytePool = bytePool
	mt.bytePoolReader = byteutil.NewInvertedBytePoolReader(bytePool, 0) //newByteBlockReader(bytePool)
	mt.indexs = NewDefalutTagGroup()
	mt.series = newStripeSeries()
	c := NewChain()
	c.Use(TermMiddleware())
	c.Use(LogFreqMiddleware())
	mt.msgIndex = c.Last(Position)
	return mt
}

func (mt *MemTable) newIndex(isTag bool) index.Index {
	return skiplist.New(isTag) //gorax.New(isTag) // //rax.New(isTag)
}

func (mt *MemTable) Init() {
	mt.indexs.Set(global.MESSAGE, mt.newIndex(false))
}

func (mt *MemTable) SetBaseTimeStamp(t int64) {
	mt.baseTimeStamp = t
	mt.bytePoolReader.SetBaseTime(t)
}

func (mt *MemTable) reset() {
	mt.size = 0
	mt.lastSeriesID = 0
	mt.logID = 0
}

//NewBlock 申请一个新的内存块
func (mt *MemTable) NewBlock() uint64 {
	return mt.bytePool.InitBytes()
}

func (mt *MemTable) GetDataStructDB() indexGroup {
	return mt.indexs
}

func (mt *MemTable) ChunkReader() chunks.ChunkReader {
	return mt.bytePoolReader
}

//WriteString 写入字符串数据 返回写入的偏移
func (mt *MemTable) WriteString(i uint64, s string) (uint64, int) {
	return mt.bytePool.WriteString(i, s)
}

//WriteBytes 写入bytes
func (mt *MemTable) WriteBytes(i uint64, b []byte) (uint64, int) {
	return mt.bytePool.WriteBytes(i, b)
}

//WriteVInt 写入可变长编码
func (mt *MemTable) WriteVInt(i uint64, b int) (uint64, int) {
	return mt.bytePool.WriteVInt(i, b)
}

func (mt *MemTable) WriteVInt64(i uint64, b int64) (uint64, int) {
	return mt.bytePool.WriteVInt64(i, b)
}

func (mt *MemTable) WriteVUint64(i uint64, b uint64) (uint64, int) {
	return mt.bytePool.WriteVUint64(i, b)
}

func (mt *MemTable) ShowSeries() {

}

//回收内存
func (mt *MemTable) Release() error {
	// mt.bytePool.Release(recycle, alloced)
	// mt.indexs.Release()
	// mt.series.gc()
	// mt.reset()
	return nil
	//mt.indexs.
}

func (mt *MemTable) ReleaseBuff(recycle, alloced *int) error {
	mt.bytePool.Release(recycle, alloced)
	mt.indexs.Release()
	mt.series.gc()
	mt.reset()
	return nil
	//mt.indexs.
}

func (mt *MemTable) addTerm(context *Context, ref uint64, lset labels.Labels, pList index.Index) {
	pointer, ok := pList.Find(context.Term)
	//未出现过的词
	var posting *TermPosting
	if !ok {
		posting = newTermPosting()
		pList.Insert(context.Term, posting)
	} else {
		posting = pointer.(*TermPosting)
	}
	p, ok := posting.series[ref]
	if !ok {
		p = newRawPosting()
		p.lset = lset
		posting.series[ref] = p
	}
	context.P = p
	mt.msgIndex(context, mt)
	if !p.IsCommit {
		mt.flushPosting = append(mt.flushPosting, p)
		p.IsCommit = true
	}
}

func (mt *MemTable) getNextLogID() uint64 {
	mt.logID++
	return mt.logID
}

func (mt *MemTable) LogNum() uint64 {
	return mt.logID
}

//索引文档
func (mt *MemTable) Index(context *Context, a *analysis.Analyzer, log *logmsg.LogMsg) {
	logID := mt.getNextLogID()
	context.LogID = logID //mt.getNextLogID()
	context.TimeStamp = log.TimeStamp

	lset := labels.FromMap(log.Tags)
	if len(lset) == 0 {
		return
	}
	s, _ := mt.getOrCreate(lset.Hash(), lset)
	mt.addLabel(s, log.TimeStamp, logID)

	tokens := a.Analyze([]byte(log.Msg))
	postingList, ok := mt.indexs.Get(global.MESSAGE) //e.processMap[global.MESSAGE]
	if !ok {
		return
	}
	//分词 全文索引
	for _, t := range tokens {
		if strings.TrimSpace(t.Term) == "" {
			continue
		}
		context.Term = bytes.TrimSpace([]byte(t.Term)) //词
		context.Position = t.Position
		mt.addTerm(context, s.ref, lset, postingList)
	}
	atomic.AddUint64(&mt.size, uint64(log.Size()))
}

func (mt *MemTable) Size() uint64 {
	return atomic.LoadUint64(&mt.size)
}

func (mt *MemTable) addLabel(s *memSeries, t int64, v uint64) error {
	var offset uint64
	var size, length int
	//var size int = s.seriesLen
	offset, length = mt.bytePool.WriteVInt64(s.seriesIndex, t-s.lastTimeStamp)
	size += length
	offset, length = mt.bytePool.WriteVUint64(offset, v-s.lastLogID)
	size += length
	if s.minT == -1 {
		s.minT = t
	}
	s.maxT = t
	atomic.StoreUint64(&s.seriesIndex, offset)
	atomic.AddUint64(&s.seriesLen, uint64(size))

	s.lastTimeStamp = t
	s.lastLogID = v
	s.logNum++
	return nil
}

func (mt *MemTable) getOrCreate(hash uint64, lset labels.Labels) (*memSeries, bool) {
	s := mt.series.getByHash(hash, lset)
	if s != nil {
		return s, false
	}
	id := atomic.AddUint64(&mt.lastSeriesID, 1)

	return mt.getOrCreateWithID(id, hash, lset)
}

func (mt *MemTable) getOrCreateWithID(id, hash uint64, lset labels.Labels) (*memSeries, bool) {

	s := newMemSeries(lset, id)
	s, created := mt.series.getOrSet(hash, s)
	if !created {
		return s, false
	}
	s.byteStart = mt.bytePool.InitBytes()
	s.seriesIndex = s.byteStart
	for _, l := range lset {
		postingList, ok := mt.indexs.Get(l.Name)
		if !ok {
			postingList = mt.newIndex(true) //newSkipList(true)
			mt.indexs.Set(l.Name, postingList)
		}
		b := byteutil.Str2bytes(l.Value)
		pointer, ok := postingList.Find(b)
		var posting *LabelPosting
		if !ok {
			posting = &LabelPosting{}
			postingList.Insert(b, posting)
		} else {
			posting = pointer.(*LabelPosting)
		}
		posting.seriesID = append(posting.seriesID, id)
	}
	return s, true
}

func (mt *MemTable) Flush() {
	for _, p := range mt.flushPosting {
		WriteLogFreq(p, mt)
		ResetPosting(p, mt)
	}
	mt.flushPosting = mt.flushPosting[:0]
}

func (mt *MemTable) Iterator() disk.IteratorLabel {
	return mt.indexs.Iterator(mt.bytePoolReader, mt.series)
}

func (mt *MemTable) Search(lset []*temqlLabels.Matcher, expr temql.Expr) (posting.Postings, []series.Series) { // ([]*search.SeriesSnapShot, []*search.SnapShot) {
	var its []posting.Postings
	for _, v := range lset {
		postingList, ok := mt.indexs.Get(v.Name)
		if !ok {
			continue
		}
		its = append(its, selectSingle(postingList, byteutil.Str2bytes(v.Value)))
	}

	if expr == nil {
		p := posting.Intersect(its...)
		return p, []series.Series{mt.series}
	}
	postingList, ok := mt.indexs.Get(global.MESSAGE)
	if !ok {
		return posting.EmptyPostings, nil
	}
	var series []series.Series
	if len(its) > 0 {
		return posting.Intersect(queryTerm(expr, postingList, &series), posting.Intersect(its...)), series
	}
	return posting.Intersect(queryTerm(expr, postingList, &series)), series
}

func queryTerm(e temql.Expr, postingList index.Index, series *[]series.Series) posting.Postings {
	switch e.(type) {
	case *temql.TermBinaryExpr:
		expr := e.(*temql.TermBinaryExpr)
		p1 := queryTerm(expr.LHS, postingList, series)
		p2 := queryTerm(expr.RHS, postingList, series)
		switch expr.Op {
		case temql.LAND:
			return posting.Intersect(p1, p2)
		case temql.LOR:
			return posting.Merge(p1, p2)
		}
	case *temql.TermExpr:
		e := e.(*temql.TermExpr)
		pointer, _ := postingList.Find(byteutil.Str2bytes(e.Name))
		if pointer == nil {
			return posting.EmptyPostings
		}
		termList := pointer.(*TermPosting)
		*series = append(*series, termList)
		return posting.NewListPostings(termList.seriesID())
	}
	return nil
}

func selectSingle(p index.Index, b []byte) posting.Postings {
	m, _ := p.Find(b)
	if m == nil {
		return posting.EmptyPostings
	}
	list := m.(*LabelPosting)
	return posting.NewListPostings(list.seriesID)
}
