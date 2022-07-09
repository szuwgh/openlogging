package disk

import (
	"bytes"
	"encoding/binary"

	"github.com/szuwgh/temsearch/pkg/engine/tem/cache"

	"github.com/szuwgh/temsearch/pkg/engine/tem/iterator"
	"github.com/szuwgh/temsearch/pkg/engine/tem/posting"
	"github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"
)

type dir int

const (
	dirReleased dir = iota - 1
	dirSOI
	dirEOI
	dirBackward
	dirForward
)

type blockIterator struct {
	block         *blockReader
	blockReleaser cache.Releaser
	k, v          []byte
	offset        int
	limit         int
}

func newBlockIterator(block *blockReader, rel cache.Releaser) *blockIterator {
	bi := &blockIterator{
		block:         block,
		blockReleaser: rel,
		limit:         block.free,
	}
	return bi
}

func (bi *blockIterator) seekWithRestart(key []byte) bool {
	offset, err := bi.block.search(key)
	if err != nil {
		return false
	}
	bi.offset = offset
	for bi.Next() {
		if bytes.Compare(bi.Key(), key) >= 0 {
			return true
		}
	}
	return false
}

//查找数据
func (bi *blockIterator) seek(key []byte) bool {
	for bi.Next() {
		if bytes.Compare(bi.Key(), key) >= 0 {
			return true
		}
	}
	return false
}

// func (bi *blockIterator) printRestart() {
// 	bi.block.printRestart()
// }

func (bi *blockIterator) First() bool {
	return bi.Next()
}

func (bi *blockIterator) Next() bool {
	if bi.offset >= bi.limit {
		return false
	}
	key, value, nShared, n, err := bi.block.entry(bi.offset)
	if err != nil {
		return false
	}
	bi.k = append(bi.k[:nShared], key...)
	bi.v = value
	bi.offset += n
	return true
}

func (bi *blockIterator) Key() []byte {
	return bi.k
}

func (bi *blockIterator) Value() []byte {
	return bi.v
}

func (bi *blockIterator) Release() {
	bi.k = nil
	bi.v = nil
	if bi.blockReleaser != nil {
		bi.blockReleaser.Release()
	}
}

type indexIterator struct {
	*blockIterator
	reader *IndexReader //fileReader
}

func newIndexIterator(iter *blockIterator, tr *IndexReader) iterator.IteratorIndex {
	indexIter := &indexIterator{}
	indexIter.blockIterator = iter
	indexIter.reader = tr
	return indexIter
}

func (i *indexIterator) Get() iterator.SingleIterator {
	value := i.Value()
	if value == nil {
		return nil
	}
	bh, _ := decodeBlockHandle(value)
	if bh.length == 0 {
		return nil
	}
	return i.reader.indexr.getDataIter(bh)
}

//域
type labelIterator struct {
	index iterator.IteratorIndex //去取下一个block
	data  iterator.SingleIterator
	//chunkr   *chunkReader
	//postingr *postingReader
	seriesr *seriesReader
	buf     [binary.MaxVarintLen32]byte
}

func newLabelIterator(index iterator.IteratorIndex, postingr *postingReader, seriesr *seriesReader, chunkr *chunkReader) *labelIterator {
	iter := &labelIterator{}
	iter.index = index
	//iter.chunkr = chunkr
	//iter.postingr = postingr
	iter.seriesr = seriesr
	return iter
}

func (f *labelIterator) Next() bool {
	if f.data != nil && !f.data.Next() {
		//释放
		f.data = nil
	}
	if f.data == nil {
		if !f.index.Next() {
			return false
		}
		f.setData()
		return f.Next()
	}
	return true
}

func (f *labelIterator) setData() {
	if f.data != nil {
		//f.data.R
		//释放
	}
	f.data = f.index.Get()
}

//First 获取第一个
func (f *labelIterator) First() bool {
	if !f.index.First() {
		return false
	}
	f.setData()
	return f.Next()
}

func (f *labelIterator) Key() []byte {
	return f.data.Key()
}

func (f *labelIterator) Value() []byte {
	return f.data.Value()
}

func (f *labelIterator) Chunks(w IndexWriter, segmentNum uint64) ([]TimeChunk, []uint64, error) {
	ref, _ := binary.Uvarint(f.Value())
	seriesRef, termRef := f.seriesr.readPosting2(ref)
	seriesPosting := make([]uint64, 0, len(seriesRef))
	timeChunk := make([]TimeChunk, 0, len(seriesRef))
	if termRef != nil {
		for _, v := range termRef {
			lset, chunkMeta, err := f.seriesr.getByID(v)
			if err != nil {
				return nil, nil, err
			}
			chk := TimeChunk{Lset: lset}
			for i, c := range chunkMeta {
				c.LastLogNum = c.LastLogNum + segmentNum
				chunkMeta[i] = c
			}
			chk.Meta = chunkMeta
			seriesRef, _ := w.GetSeries(lset)
			seriesPosting = append(seriesPosting, seriesRef)
			timeChunk = append(timeChunk, chk)
		}
		return timeChunk, seriesPosting, nil
	}
	for _, v := range seriesRef {
		lset, chunkMeta, err := f.seriesr.getByID(v)
		if err != nil {
			return nil, nil, err
		}
		seriesRef, exist := w.GetSeries(lset)
		if !exist {
			chk := TimeChunk{Lset: lset}
			for i, c := range chunkMeta {
				c.LastLogNum = c.LastLogNum + segmentNum
				chunkMeta[i] = c
			}
			chk.Meta = chunkMeta
			timeChunk = append(timeChunk, chk)
		} else {
			seriesPosting = append(seriesPosting, seriesRef)
		}
	}
	return timeChunk, seriesPosting, nil
}

func (f *labelIterator) Write(w IndexWriter, segmentNum uint64, baseTime int64) ([]TimeChunk, []uint64, error) {
	ref, _ := binary.Uvarint(f.Value())
	seriesRef, termRef := f.seriesr.readPosting2(ref)
	seriesPosting := make([]uint64, 0, len(seriesRef))
	timeChunk := make([]TimeChunk, 0, len(seriesRef))

	if termRef != nil {
		for _, v := range termRef {
			lset, chunkMeta, err := f.seriesr.getByID(v)
			if err != nil {
				return nil, nil, err
			}
			chk := TimeChunk{Lset: lset}
			for i, c := range chunkMeta {
				chunkEnc := c.ChunkEnc(true, f.seriesr)
				chunkRef, err := w.WriteChunks(chunkEnc.Bytes())
				if err != nil {
					return nil, nil, err
				}
				c.Ref = chunkRef
				c.LastLogNum = c.LastLogNum + segmentNum
				chunkMeta[i] = c
			}
			chk.Meta = chunkMeta
			seriesRef, _ := w.GetSeries(lset)
			seriesPosting = append(seriesPosting, seriesRef)
			timeChunk = append(timeChunk, chk)
		}
		return timeChunk, seriesPosting, nil
	}
	for _, v := range seriesRef {
		lset, chunkMeta, err := f.seriesr.getByID(v)
		if err != nil {
			return nil, nil, err
		}
		seriesRef, exist := w.GetSeries(lset)
		if !exist {
			chk := TimeChunk{Lset: lset}
			for i, c := range chunkMeta {
				chunkEnc := c.ChunkEnc(false, f.seriesr)
				chunkRef, err := w.WriteChunks(chunkEnc.Bytes())
				if err != nil {
					return nil, nil, err
				}
				c.Ref = chunkRef
				c.LastLogNum = c.LastLogNum + segmentNum
				chunkMeta[i] = c
			}
			chk.Meta = chunkMeta
			timeChunk = append(timeChunk, chk)
		} else {
			seriesPosting = append(seriesPosting, seriesRef)
		}
	}
	return timeChunk, seriesPosting, nil
}

//表迭代
type tableIterator struct {
	labelIter iterator.SingleIterator //每個索引的label块
	reader    *IndexReader
	chunkr    *chunkReader
	postingr  *postingReader
	seriesr   *seriesReader
}

func (t *tableIterator) Next() bool {
	return t.labelIter.Next()
}

func (t *tableIterator) First() bool {
	return t.Next()
}

func (t *tableIterator) Key() []byte {
	return t.labelIter.Key()
}

func (t *tableIterator) Value() []byte {
	return t.labelIter.Value()
}

//返回每个label的iterator
func (t *tableIterator) Iter() WriterIterator {
	value := t.Value()
	bh, _ := decodeBlockHandle(value)
	if bh.length == 0 {
		return nil
	}
	indexBlock, err := t.reader.indexr.readBlock(bh, true)
	if err != nil {
		return nil
	}
	iterator := newBlockIterator(indexBlock, nil)
	return newLabelIterator(newIndexIterator(iterator, t.reader), t.postingr, t.seriesr, t.chunkr)
}

type IteratorLabel interface {
	iterator.SingleIterator
	Iter() WriterIterator
}

type WriterIterator interface {
	iterator.SingleIterator
	Write(IndexWriter, uint64, int64) ([]TimeChunk, []uint64, error)
	Chunks(IndexWriter, uint64) ([]TimeChunk, []uint64, error)
	//Values() [][]byte
}

//多路归并排序
type MergedIterator struct {
	iters []iterator.SingleIterator //tableIterator
	//iters      []Iterator
	keys   [][]byte
	indexs []int
	index  int
	dir    dir //
}

func NewMergedIterator(iters ...iterator.SingleIterator) *MergedIterator {
	iter := &MergedIterator{}
	iter.iters = iters
	iter.keys = make([][]byte, len(iters))
	iter.dir = dirSOI
	return iter
}

func (m *MergedIterator) next() bool {
	var key []byte
	for x, tKey := range m.keys {
		if tKey != nil && (key == nil || bytes.Compare(tKey, key) < 0) {
			key = tKey
			m.index = x
		}
	}
	if key == nil {
		m.dir = dirEOI
		return false
	}

	m.indexs = m.indexs[:0]
	m.indexs = append(m.indexs, m.index)
	for i := m.index + 1; i < len(m.keys); i++ {
		if bytes.Equal(m.keys[i], key) {
			m.indexs = append(m.indexs, i)
		}
	}
	m.dir = dirForward
	return true
}

func (m *MergedIterator) First() bool {
	for i, iter := range m.iters {
		switch {
		case iter.First():
			m.keys[i] = iter.Key()
		}
	}
	return true
}

//
func (m *MergedIterator) Next() bool {
	if m.dir == dirEOI {
		return false
	}
	switch m.dir {
	case dirSOI:
		m.First()
	case dirForward:
		for _, x := range m.indexs {
			iter := m.iters[x]
			if iter.Next() {
				m.keys[x] = iter.Key()
			} else {
				m.keys[x] = nil
			}
		}
	}
	return m.next()
}

func (m *MergedIterator) Key() []byte {
	return m.keys[m.index]
}

func (m *MergedIterator) Values() [][]byte {

	values := make([][]byte, len(m.indexs))
	for i, x := range m.indexs {
		values[i] = m.iters[x].Value()
	}
	return values
}

type compactionSet interface {
	Next() bool
	At() (labels.Labels, []ChunkMeta)
	Err() error
}

type compactionMerger struct {
	a, b compactionSet

	aok, bok bool
	l        labels.Labels
	c        []ChunkMeta
}

func newCompactionMerger(a, b compactionSet) (*compactionMerger, error) {
	c := &compactionMerger{
		a: a,
		b: b,
	}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	c.aok = c.a.Next()
	c.bok = c.b.Next()

	return c, c.Err()
}

func (c *compactionMerger) compare() int {
	if !c.aok {
		return 1
	}
	if !c.bok {
		return -1
	}
	a, _ := c.a.At()
	b, _ := c.b.At()
	return labels.Compare(a, b)
}

func (c *compactionMerger) Next() bool {
	if !c.aok && !c.bok || c.Err() != nil {
		return false
	}
	var lset labels.Labels
	var chks []ChunkMeta
	d := c.compare()
	if d > 0 {
		lset, chks = c.b.At()
		c.l = append(c.l[:0], lset...)
		c.c = append(c.c[:0], chks...)

		c.bok = c.b.Next()
	} else if d < 0 {
		lset, chks = c.a.At()
		c.l = append(c.l[:0], lset...)
		c.c = append(c.c[:0], chks...)

		c.aok = c.a.Next()
	} else {
		l, ca := c.a.At()
		_, cb := c.b.At()
		c.l = append(c.l[:0], l...)
		c.c = append(append(c.c[:0], ca...), cb...)

		c.aok = c.a.Next()
		c.bok = c.b.Next()
	}

	return true
}

func (c *compactionMerger) Err() error {
	if c.a.Err() != nil {
		return c.a.Err()
	}
	return c.b.Err()
}

func (c *compactionMerger) At() (labels.Labels, []ChunkMeta) {
	return c.l, c.c
}

type emptySet struct{}

var emptyChunkSet = emptySet{}

func (it emptySet) At() (labels.Labels, []ChunkMeta) { return nil, nil }
func (it emptySet) Next() bool                       { return false }
func (it emptySet) Err() error                       { return nil }

type chunkSet struct {
	chks []TimeChunk
	cur  TimeChunk
}

func NewListPostings(chks []TimeChunk) *chunkSet {
	return &chunkSet{chks: chks}
}

func (it *chunkSet) At() (labels.Labels, []ChunkMeta) {
	return it.cur.Lset, it.cur.Meta
}

func (it *chunkSet) Next() bool {
	if len(it.chks) > 0 {
		it.cur = it.chks[0]
		it.chks = it.chks[1:]
		return true
	}
	//it.cur = nil
	return false
}

func (it *chunkSet) Err() error {
	return nil
}

type MergeWriterIterator struct {
	MergedIterator
	writerIters []WriterIterator
	segmentNum  []uint64
	baseTime    []int64
	set         compactionSet
	posting     posting.Postings
	msgTagName  string
}

func NewMergeWriterIterator(segmentNum []uint64, baseTime []int64, msgTagName string, iters ...WriterIterator) *MergeWriterIterator {
	iter := make([]iterator.SingleIterator, len(iters))
	for i, x := range iters {
		iter[i] = x
	}
	mergeWriterIter := &MergeWriterIterator{
		MergedIterator: MergedIterator{
			iters: iter,
			keys:  make([][]byte, len(iter)),
			dir:   dirSOI,
		},
		writerIters: iters,
		segmentNum:  segmentNum,
		baseTime:    baseTime,
		msgTagName:  msgTagName,
	}

	return mergeWriterIter
}

func (m *MergeWriterIterator) Write2(labelName string, w IndexWriter) error {

	var segmentNum uint64
	//合并
	m.set = emptyChunkSet
	m.posting = posting.EmptyPostings
	for _, x := range m.indexs {
		chunks, p, err := m.writerIters[x].Chunks(w, segmentNum)
		if err != nil {
			return err
		}
		if len(chunks) > 0 {
			m.set, err = newCompactionMerger(m.set, &chunkSet{chks: chunks})
			if err != nil {
				return err
			}
		}
		if len(p) > 0 {
			m.posting = posting.NewMergedPostings(m.posting, posting.NewListPostings(p))
		}
		segmentNum += m.segmentNum[x]
	}

	var p []uint64
	for m.posting.Next() {
		p = append(p, m.posting.At())
	}
	var pRef []uint64
	for m.set.Next() {
		lset, chunk := m.set.At()
		if len(chunk) > 12 {

		}
		ref, err := w.AddSeries(labelName != m.msgTagName, lset, chunk...)
		if err != nil {
			return err
		}
		pRef = append(pRef, ref)
	}
	var ref uint64
	var err error
	switch labelName {
	case m.msgTagName:
		ref, err = w.WritePostings(p, pRef)
	default:
		ref, err = w.WritePostings(append(p, pRef...))
	}
	if err != nil {
		return err
	}
	return w.AppendKey(m.Key(), ref)
}

func (m *MergeWriterIterator) Write(labelName string, w IndexWriter) error {

	var segmentNum uint64
	//合并
	m.set = emptyChunkSet
	m.posting = posting.EmptyPostings
	for _, x := range m.indexs {
		chunks, p, err := m.writerIters[x].Write(w, segmentNum, m.baseTime[x])
		if err != nil {
			return err
		}
		if len(chunks) > 0 {
			m.set, err = newCompactionMerger(m.set, &chunkSet{chks: chunks})
			if err != nil {
				return err
			}
		}
		if len(p) > 0 {
			m.posting = posting.NewMergedPostings(m.posting, posting.NewListPostings(p))
		}
		segmentNum += m.segmentNum[x]
	}

	var p []uint64
	for m.posting.Next() {
		p = append(p, m.posting.At())
	}
	var pRef []uint64
	for m.set.Next() {
		lset, chunk := m.set.At()
		ref, err := w.AddSeries(labelName != m.msgTagName, lset, chunk...)
		if err != nil {
			return err
		}
		pRef = append(pRef, ref)
	}
	var ref uint64
	var err error
	switch labelName {
	case m.msgTagName:
		ref, err = w.WritePostings(p, pRef)
	default:
		ref, err = w.WritePostings(append(p, pRef...))
	}
	if err != nil {
		return err
	}
	return w.AppendKey(m.Key(), ref)
}

type MergeLabelIterator struct {
	MergedIterator
	iterTables []IteratorLabel
}

func NewMergeLabelIterator(iters ...IteratorLabel) *MergeLabelIterator {
	iter := make([]iterator.SingleIterator, len(iters))
	for i, x := range iters {
		iter[i] = x
	}
	mergeLabelIter := &MergeLabelIterator{
		MergedIterator: MergedIterator{
			iters: iter,
			keys:  make([][]byte, len(iter)),
			dir:   dirSOI,
		},
		iterTables: iters,
	}

	return mergeLabelIter
}

//返回同一个label的块
func (t *MergeLabelIterator) Iters() []WriterIterator {
	iters := make([]WriterIterator, len(t.iterTables))
	for i := range iters {
		iters[i] = t.iterTables[i].Iter()
	}
	return iters
}

type MergeLogIterator struct {
	iters []LogIterator
	i     int
}

func NewMergeLogIterator(iters ...LogIterator) *MergeLogIterator {
	mergeLogIter := &MergeLogIterator{}
	mergeLogIter.iters = iters
	return mergeLogIter
}

func (t *MergeLogIterator) Next() bool {
	if t.i >= len(t.iters) {
		return false
	}
	hasNext := t.iters[t.i].Next()
	if hasNext {
		return hasNext
	}
	t.i++
	return t.Next()
}

func (t *MergeLogIterator) Write(w LogWriter) (uint64, error) {
	return t.iters[t.i].Write(w)
}

type LogIterator interface {
	Next() bool
	Write(LogWriter) (uint64, error) // []byte
}

type LogWriter interface {
	FinishLog(uint64) (uint64, error)
	WriteBytes([]byte) error
	WriteIndex() error
	Close() (uint64, error)
}

type DiskLogIterator struct {
	logr  *LogReader
	j     uint64
	share [8]byte
}

func newDiskLogIterator(logr *LogReader) *DiskLogIterator {
	iter := &DiskLogIterator{}
	iter.logr = logr
	iter.j = 0
	return iter
}

func (t *DiskLogIterator) Next() bool {
	t.j++
	if t.j > t.logr.logCount {
		return false
	}
	return true
}

func (t *DiskLogIterator) Value() []byte {
	return t.logr.ReadLog(t.j)
}

func (t *DiskLogIterator) Write(w LogWriter) (uint64, error) {
	b := t.logr.ReadLog(t.j)
	l := uint64(len(b))
	n := binary.PutUvarint(t.share[0:], l)
	w.WriteBytes(t.share[:n])
	w.WriteBytes(b)
	return w.FinishLog(uint64(n) + l)

}
