package tem

import (
	"container/heap"

	"github.com/szuwgh/temsearch/pkg/engine/tem/chunks"
	"github.com/szuwgh/temsearch/pkg/engine/tem/posting"
	"github.com/szuwgh/temsearch/pkg/engine/tem/series"
	"github.com/szuwgh/temsearch/pkg/lib/prompb"
	"github.com/szuwgh/temsearch/pkg/temql"

	"github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"
)

type Searcher interface {
	Search(lset []*prompb.LabelMatcher, expr temql.Expr, mint, maxt int64) SeriesSet
	Close() error
}

type searcher struct {
	bs []Searcher
}

func (s *searcher) Search(lset []*prompb.LabelMatcher, expr temql.Expr, mint, maxt int64) SeriesSet {
	return s.search(s.bs, lset, expr, mint, maxt)
}

func (s *searcher) Close() error {
	var merr MultiError
	for _, b := range s.bs {
		merr.Add(b.Close())
	}
	return merr.Err()
}

func (s *searcher) search(bs []Searcher, lset []*prompb.LabelMatcher, expr temql.Expr, mint, maxt int64) SeriesSet {
	if len(bs) == 0 {
		return nopSeriesSet{}
	}
	if len(bs) == 1 {
		return bs[0].Search(lset, expr, mint, maxt)
	}
	l := len(bs) / 2
	a := s.search(bs[l:], lset, expr, mint, maxt)
	b := s.search(bs[:l], lset, expr, mint, maxt)
	return newMergedSeriesSet(a, b)
}

type mergedSeriesSet struct {
	a, b SeriesSet

	cur          Series
	adone, bdone bool
}

// NewMergedSeriesSet takes two series sets as a single series set. The input series sets
// must be sorted and sequential in time, i.e. if they have the same label set,
// the datapoints of a must be before the datapoints of b.
func NewMergedSeriesSet(a, b SeriesSet) SeriesSet {
	return newMergedSeriesSet(a, b)
}

func newMergedSeriesSet(a, b SeriesSet) *mergedSeriesSet {
	s := &mergedSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedSeriesSet) At() Series {
	return s.cur
}

func (s *mergedSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	return labels.Compare(s.a.At().Labels(), s.b.At().Labels())
}

func (s *mergedSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()

	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		s.cur = s.b.At()
		s.bdone = !s.b.Next()
	} else if d < 0 {
		s.cur = s.a.At()
		s.adone = !s.a.Next()
	} else {
		s.cur = &chainedSeries{series: []Series{s.a.At(), s.b.At()}}
		s.adone = !s.a.Next()
		s.bdone = !s.b.Next()
	}
	return true
}

type chainedSeries struct {
	series []Series
}

func (s *chainedSeries) Labels() labels.Labels {
	return s.series[0].Labels()
}

func (s *chainedSeries) Iterator() SeriesIterator {
	return newChainedSeriesIterator(s.series...)
}

// chainedSeriesIterator implements a series iterater over a list
// of time-sorted, non-overlapping iterators.
type chainedSeriesIterator struct {
	series []Series // series in time order

	i   int
	cur SeriesIterator
}

func newChainedSeriesIterator(s ...Series) *chainedSeriesIterator {
	return &chainedSeriesIterator{
		series: s,
		i:      len(s) - 1,
		cur:    s[len(s)-1].Iterator(),
	}
}

// func (it *chainedSeriesIterator) Seek(t int64) bool {
// 	// We just scan the chained series sequentially as they are already
// 	// pre-selected by relevant time and should be accessed sequentially anyway.
// 	for i, s := range it.series[it.i:] {
// 		cur := s.Iterator()
// 		if !cur.Seek(t) {
// 			continue
// 		}
// 		it.cur = cur
// 		it.i += i
// 		return true
// 	}
// 	return false
// }

func (it *chainedSeriesIterator) Next() bool {
	if it.cur.Next() {
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	// if it.i == len(it.series)-1 {
	// 	return false
	// }

	// it.i++
	if it.i == 0 {
		return false
	}

	it.i--
	it.cur = it.series[it.i].Iterator()

	return it.cur.Next()
}

func (it *chainedSeriesIterator) At() (int64, uint64, []int, []byte) {
	return it.cur.At()
}

func (it *chainedSeriesIterator) Err() error {
	return it.cur.Err()
}

func NewBloctemsearcher(b BlockReader) *blockSearcher {
	bs := &blockSearcher{
		indexr:        b.Index(),
		logr:          b.Logs(),
		baseTimeStamp: b.MinTime(),
	}
	return bs
}

// SeriesSet contains a set of series.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

type nopSeriesSet struct{}

func (nopSeriesSet) Next() bool { return false }
func (nopSeriesSet) At() Series { return nil }
func (nopSeriesSet) Err() error { return nil }

// Series exposes a single time series.
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	Iterator() SeriesIterator
}

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the given timestamp.
	// If there's no value exactly at t, it advances to the first value
	// after t.
	//Seek(t int64) bool
	// At returns the current timestamp/value pair.
	//Byte(v uint64) []byte

	//SeriesIter() SeriesIterator

	At() (t int64, v uint64, pos []int, b []byte)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}

type ChunkSeriesSet interface {
	Next() bool
	At() (labels.Labels, [][]chunks.Chunk)
	Err() error
}

type blockSeriesSet struct {
	set        ChunkSeriesSet
	chunkr     chunks.ChunkReader
	logr       LogReader
	err        error
	cur        Series
	isTerm     bool
	mint, maxt int64
	lastSegNum uint64
}

func (s *blockSeriesSet) Next() bool {
	for s.set.Next() {
		lset, chunks := s.set.At()
		s.cur = &chunkSeries{
			labels:     lset,
			chunkr:     s.chunkr,
			logr:       s.logr,
			chunks:     chunks,
			mint:       s.mint,
			maxt:       s.maxt,
			isTerm:     s.isTerm,
			lastSegNum: s.lastSegNum,
		}
		return true
	}
	if s.set.Err() != nil {
		s.err = s.set.Err()
	}
	return false
}

func (s *blockSeriesSet) At() Series { return s.cur }
func (s *blockSeriesSet) Err() error { return s.err }

type chunkSeries struct {
	labels     labels.Labels
	logr       LogReader
	chunkr     chunks.ChunkReader
	chunks     [][]chunks.Chunk // in-order chunk refs
	mint, maxt int64
	isTerm     bool
	lastSegNum uint64
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *chunkSeries) Iterator() SeriesIterator {
	return newChunkSeriesIterator(s.chunks, s.isTerm, s.logr, s.chunkr, s.mint, s.maxt, s.lastSegNum)
	//return newFlashbackChunkSeriesIterator(s.chunks, s.isTerm, s.logr, s.chunkr, s.mint, s.maxt, s.lastSegNum)
}

type logResult struct {
	t   int64
	v   uint64
	pos []int
}

type logChunk []logResult

func (h logChunk) Len() int           { return len(h) }
func (h logChunk) Less(i, j int) bool { return h[i].v > h[j].v }
func (h logChunk) Swap(i, j int)      { h[i].v, h[j].v = h[j].v, h[i].v }

func (h *logChunk) Push(x interface{}) {
	*h = append(*h, x.(logResult))
}

func (h *logChunk) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type flashbackChunkSeriesIterator struct {
	//cur        chunks.Postings
	h          *logChunk
	logr       LogReader
	t          int64
	v          uint64
	pos        []int
	err        error
	lastSegNum uint64
}

func newFlashbackChunkSeriesIterator(chks [][]chunks.Chunk, isTerm bool, logr LogReader, cr chunks.ChunkReader, mint, maxt int64, lastSegNum uint64) *flashbackChunkSeriesIterator {
	var cur []chunks.Postings
	for _, cs := range chks {
		var p []chunks.Postings
		for _, c := range cs {
			chunk := c.ChunkEnc(isTerm, cr)
			p = append(p, chunk.Iterator(mint, maxt))
		}
		cur = append(cur, chunks.Merge(p...))
	}
	h := &logChunk{}
	heap.Init(h)
	posting := chunks.Intersect(cur...)
	for posting.Next() {
		t, v, pos := posting.At()
		heap.Push(h, logResult{t, v, pos})
	}
	return &flashbackChunkSeriesIterator{
		h:          h,
		logr:       logr,
		lastSegNum: lastSegNum,
	}

}

// func (it *chunkSeriesIterator) Seek() (t int64) {
// 	return it.t, it.v, it.logr.ReadLog(it.v)
// }

func (it *flashbackChunkSeriesIterator) At() (int64, uint64, []int, []byte) {
	return it.t, it.v + it.lastSegNum, it.pos, it.logr.ReadLog(it.v)
}

func (it *flashbackChunkSeriesIterator) Next() bool {
	if it.h.Len() > 0 {
		r := heap.Pop(it.h).(logResult)
		it.t, it.v, it.pos = r.t, r.v, r.pos
		return true
	}
	return false
}

func (it *flashbackChunkSeriesIterator) Err() error {
	return it.err
}

type chunkSeriesIterator struct {
	cur        chunks.Postings
	logr       LogReader
	t          int64
	v          uint64
	pos        []int
	err        error
	lastSegNum uint64
}

func newChunkSeriesIterator(chks [][]chunks.Chunk, isTerm bool, logr LogReader, cr chunks.ChunkReader, mint, maxt int64, lastSegNum uint64) *chunkSeriesIterator {
	var cur []chunks.Postings
	for _, cs := range chks {
		var p []chunks.Postings
		for _, c := range cs {
			chunk := c.ChunkEnc(isTerm, cr)
			p = append(p, chunk.Iterator(mint, maxt))
		}
		cur = append(cur, chunks.Merge(p...))
	}

	return &chunkSeriesIterator{
		cur:        chunks.Merge(cur...),
		logr:       logr,
		lastSegNum: lastSegNum,
	}
}

// func (it *chunkSeriesIterator) Seek() (t int64) {
// 	return it.t, it.v, it.logr.ReadLog(it.v)
// }

func (it *chunkSeriesIterator) At() (int64, uint64, []int, []byte) {
	return it.t, it.v + it.lastSegNum, it.pos, it.logr.ReadLog(it.v)
}

func (it *chunkSeriesIterator) Next() bool {
	if it.cur.Next() {
		it.t, it.v, it.pos = it.cur.At()
		return true
	}
	return false
}

func (it *chunkSeriesIterator) Err() error {
	return it.err
}

type populatedChunkSeries struct {
	set ChunkSeriesSet
	//chunks     ChunkReader
	mint, maxt int64

	err  error
	chks [][]chunks.Chunk
	lset labels.Labels
}

func (s *populatedChunkSeries) At() (labels.Labels, [][]chunks.Chunk) {
	return s.lset, s.chks
}

func (s *populatedChunkSeries) Next() bool {
	for s.set.Next() {
		lset, chkss := s.set.At()
		for _, chks := range chkss {
			for len(chks) > 0 {
				if chks[0].MaxTime() >= s.mint {
					break
				}
				chks = chks[1:]
			}
			for i, rlen := 0, len(chks); i < rlen; i++ {
				j := i - (rlen - len(chks))
				c := chks[j]
				// Break out at the first chunk that has no overlap with mint, maxt.
				if c.MinTime() > s.maxt {
					chks = chks[:j]
					break
				}
			}
		}
		s.lset = lset
		s.chks = chkss
		return true
	}
	if err := s.set.Err(); err != nil {
		s.err = err
	}
	return false
}

func (s *populatedChunkSeries) Err() error {
	return s.err
}

type baseChunkSeries struct {
	p      posting.Postings
	series []series.Series

	lset labels.Labels
	chks [][]chunks.Chunk
	err  error
}

func (c *baseChunkSeries) Next() bool {
	chs := make([][]chunks.Chunk, len(c.series))
	for c.p.Next() {
		for i, s := range c.series { //term
			lset, chunk, err := s.GetByID(c.p.At())
			if err != nil {
				return false
			}
			chs[i] = chunk
			if i == 0 {
				c.lset = lset
			}
		}
		c.chks = chs
		return true
	}
	if err := c.p.Err(); err != nil {
		c.err = err
	}
	return false
}

func (c *baseChunkSeries) At() (labels.Labels, [][]chunks.Chunk) {
	return c.lset, c.chks
}

func (c *baseChunkSeries) Err() error {
	return c.err
}

type blockSearcher struct {
	indexr        IndexReader
	logr          LogReader
	baseTimeStamp int64
	lastSegNum    uint64
}

func (s *blockSearcher) Search(lset []*prompb.LabelMatcher, expr temql.Expr, mint, maxt int64) SeriesSet {
	posting, series := s.indexr.Search(lset, expr)
	isTerm := expr != nil
	return &blockSeriesSet{
		set: &populatedChunkSeries{
			set: &baseChunkSeries{
				p:      posting,
				series: series,
			},
			mint: mint,
			maxt: maxt,
		},
		isTerm:     isTerm,
		chunkr:     s.indexr.ChunkReader(),
		logr:       s.logr,
		mint:       mint,
		maxt:       maxt,
		lastSegNum: s.lastSegNum,
	}
}

func (s *blockSearcher) Close() error {

	var merr MultiError

	merr.Add(s.indexr.Close())
	merr.Add(s.logr.Close())

	return merr.Err()

}

type logSet interface {
	Next() bool
	At() Serices
}

type Serices interface {
	//Iterator() SeriesIterator
}
