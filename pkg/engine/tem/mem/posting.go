package mem

import (
	"errors"
	"sort"
	"sync"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/chunks"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/index"
	"github.com/sophon-lab/temsearch/pkg/lib/prometheus/labels"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/disk"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/global"
)

type RawPosting struct {
	lset               labels.Labels
	lastLogID          uint64 //上一次文档号
	lastLogDelta       uint64 //
	lastPos            int    //上一次位置
	freq               int    //词频
	logNum             int    //日志数
	lastTimeStamp      int64  //上一次时间
	lastTimeStampDelta int64
	IsCommit           bool
	//meta
	minT, maxT int64
	//	byteStart  uint64
	//	chunk      *temChunk

	byteStart      uint64 //byte开始地方
	logFreqIndex   uint64
	logFreqLen     uint64
	skipStartIndex [global.FreqSkipListLevel]uint64
	skipLen        [global.FreqSkipListLevel]uint64
	posIndex       uint64
	posLen         uint64
}

func (t *RawPosting) MinTime() int64 {
	return t.minT
}

func (t *RawPosting) MaxTime() int64 {
	return t.maxT
}

func (t *RawPosting) ChunkEnc(isTerm bool, cr chunks.ChunkReader) chunks.ChunkEnc {
	ref := make([]uint64, 3+global.FreqSkipListLevel*2+2)
	ref[0] = t.byteStart
	ref[1] = t.logFreqIndex
	ref[2] = t.logFreqLen
	for i := 0; i < global.FreqSkipListLevel; i++ {
		ref[3+i] = t.skipStartIndex[i]
	}
	for i := 0; i < global.FreqSkipListLevel; i++ {
		ref[3+global.FreqSkipListLevel+i] = t.skipLen[i]
	}
	ref[3+global.FreqSkipListLevel*2] = t.posIndex
	ref[3+global.FreqSkipListLevel*2+1] = t.posLen
	return cr.ReadChunk(isTerm, ref...) //c.ReadChunks(t.byteStart, t.logFreqIndex, t.logFreqLen, t.skipStartIndex)
}

func (p *RawPosting) getLogFreqIndex() int {
	return int(p.logFreqIndex)
}

func (p *RawPosting) getSkipLen() uint64 {
	var n uint64
	for _, v := range p.skipLen {
		n += v
	}
	return n
}

//TermPosting term 倒排表
type TermPosting struct {
	series map[uint64]*RawPosting //
}

func (t *TermPosting) GetByID(ref uint64) (labels.Labels, []chunks.Chunk, error) {
	series, ok := t.series[ref]
	if !ok {
		return nil, nil, errors.New("ref error")
	}
	return series.lset, []chunks.Chunk{series}, nil
}

type UInt64Slice []uint64

func (p UInt64Slice) Len() int           { return len(p) }
func (p UInt64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p UInt64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (t *TermPosting) seriesID() []uint64 {
	id := make([]uint64, 0, len(t.series))
	for k := range t.series {
		id = append(id, k)
	}
	sort.Sort(UInt64Slice(id))
	return id
}

type RawPostings []*RawPosting

func (p RawPostings) Len() int           { return len(p) }
func (p RawPostings) Less(i, j int) bool { return labels.Compare(p[i].lset, p[j].lset) < 0 }
func (p RawPostings) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (t *TermPosting) toPosting() RawPostings {
	p := make(RawPostings, 0, len(t.series))
	for _, v := range t.series {
		p = append(p, v)
	}
	sort.Sort(p)
	return p
}

func newTermPosting() *TermPosting {
	p := &TermPosting{}
	p.series = make(map[uint64]*RawPosting)
	return p
}

type LabelPosting struct {
	seriesID []uint64
}

type MemSeriesList []*MemSeries

func (p MemSeriesList) Len() int           { return len(p) }
func (p MemSeriesList) Less(i, j int) bool { return labels.Compare(p[i].lset, p[j].lset) < 0 }
func (p MemSeriesList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (t *LabelPosting) toPosting(s seriesReader) MemSeriesList {
	p := make(MemSeriesList, 0, len(t.seriesID))
	for _, v := range t.seriesID {
		p = append(p, s.getByID(v))
	}
	sort.Sort(p)
	return p
}

func newRawPosting() *RawPosting {
	p := &RawPosting{}
	return p
}

// type postingIterator interface {
// 	Next() bool
// 	Key() []byte
// 	Value() unsafe.Pointer
// }

type tagIterator interface {
	Next() bool
	Key() string
	Value() index.Index
}

type seriesReader interface {
	getByID(id uint64) *MemSeries
}

//
type indexGroup interface {
	Get(string) (index.Index, bool)
	Set(string, index.Index)
	Iterator(chunks.ChunkReader, seriesReader) disk.IteratorLabel //tagIterator
	Release() error
}

type defalutTagIterator struct {
	keys    []string
	tags    *defalutTagGroup
	i       int
	chunkr  chunks.ChunkReader // posting.InvertListReader
	seriesr seriesReader
}

func (d *defalutTagIterator) First() bool {
	d.i++
	if d.i < len(d.keys) {
		return true
	}
	return false
}

func (d *defalutTagIterator) Next() bool {
	d.i++
	if d.i < len(d.keys) {
		return true
	}
	return false
}

func (d *defalutTagIterator) Key() []byte {
	return byteutil.Str2bytes(d.keys[d.i])
}

func (d *defalutTagIterator) Value() []byte {
	return nil
}

func (d *defalutTagIterator) Iter() disk.WriterIterator {
	pList := d.tags.group[d.keys[d.i]]
	return &memIterator{iter: pList.Iterator(), chunkr: d.chunkr, seriesr: d.seriesr}
	//return pList.Iterator(d.chunkr, d.seriesr)
}

// func (d *defalutTagIterator) Value() postingList {
// 	return d.tags.group[d.keys[d.i]]
// }

//
type defalutTagGroup struct {
	group map[string]index.Index
	mu    sync.RWMutex
}

func NewDefalutTagGroup() *defalutTagGroup {
	tags := &defalutTagGroup{}
	tags.group = make(map[string]index.Index)
	return tags
}

func (d *defalutTagGroup) Get(k string) (p index.Index, ok bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	p, ok = d.group[k]
	return
}

func (d *defalutTagGroup) Release() error {
	for k, v := range d.group {
		v.Free()
		delete(d.group, k)
	}
	return nil
}

// func (d *defalutTagGroup) Size() int {
// 	var size int
// 	for _, v := range d.group {
// 		size += v.Size()
// 	}
// 	return size
// }

func (d *defalutTagGroup) Set(k string, p index.Index) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.group[k] = p
}

func (d *defalutTagGroup) Iterator(chunkr chunks.ChunkReader, seriesr seriesReader) disk.IteratorLabel {
	iter := &defalutTagIterator{}
	iter.i = -1
	iter.keys = make([]string, 0, len(d.group))
	for k := range d.group {
		iter.keys = append(iter.keys, k)
	}
	iter.tags = d
	iter.chunkr = chunkr
	iter.seriesr = seriesr
	sort.Strings(iter.keys)
	return iter
}
