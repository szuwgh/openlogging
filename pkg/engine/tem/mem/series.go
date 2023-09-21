package mem

//like prometheus
import (
	"sync"

	"github.com/szuwgh/hawkobserve/pkg/engine/tem/chunks"
	"github.com/szuwgh/hawkobserve/pkg/lib/prometheus/labels"
)

type MemSeries struct {
	ref                    uint64
	lset                   labels.Labels
	byteStart              uint64 //byte开始地方
	seriesIndex, seriesLen uint64
	logNum                 int
	minT, maxT             int64

	lastLogID     uint64 //上一次文档号
	lastTimeStamp int64  //上一次时间
}

func (m *MemSeries) Lset() labels.Labels {
	return m.lset
}

func (m *MemSeries) MinTime() int64 {
	return m.minT
}

func (m *MemSeries) MaxTime() int64 {
	return m.maxT
}

func (m *MemSeries) SegmentNum() uint64 {
	return 0
}

func (m *MemSeries) ChunkEnc(isTerm bool, cr chunks.ChunkReader) chunks.ChunkEnc {
	ref := make([]uint64, 3)
	ref[0] = m.byteStart
	ref[1] = m.seriesIndex
	ref[2] = m.seriesLen
	return cr.ReadChunk(isTerm, ref...)
}

//func (s *MemSeries)newS

func newMemSeries(lset labels.Labels, id uint64) *MemSeries {
	s := &MemSeries{
		lset: lset,
		ref:  id,
		minT: -1,
	}
	return s
}

type seriesHashmap map[uint64][]*MemSeries

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

type stripeSeries struct {
	series [stripeSize]map[uint64]*MemSeries
	hashes [stripeSize]seriesHashmap
	locks  [stripeSize]stripeLock
}

func (s *stripeSeries) gc() {
	for i := range s.series {
		for k := range s.series[i] {
			delete(s.series[i], k)
		}
	}
	for i := range s.hashes {
		for k := range s.hashes[i] {
			delete(s.hashes[i], k)
		}
	}
}

func newStripeSeries() *stripeSeries {
	s := &stripeSeries{}

	for i := range s.series {
		s.series[i] = map[uint64]*MemSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	return s
}

func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *MemSeries {
	i := hash & stripeMask

	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getOrSet(hash uint64, series *MemSeries) (*MemSeries, bool) {
	i := hash & stripeMask

	s.locks[i].Lock()

	if prev := s.hashes[i].get(hash, series.lset); prev != nil {
		s.locks[i].Unlock()
		return prev, false
	}
	s.hashes[i].set(hash, series)
	s.locks[i].Unlock()

	i = series.ref & stripeMask

	s.locks[i].Lock()
	s.series[i][series.ref] = series
	s.locks[i].Unlock()

	return series, true
}

func (s *stripeSeries) GetByID(id uint64) (labels.Labels, []chunks.Chunk, error) {
	series := s.getByID(id)
	return series.lset, []chunks.Chunk{series}, nil
}

func (s *stripeSeries) getByID(id uint64) *MemSeries {
	i := id & stripeMask

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (m seriesHashmap) get(hash uint64, lset labels.Labels) *MemSeries {
	for _, s := range m[hash] {
		if s.lset.Equals(lset) {
			return s
		}
	}
	return nil
}

func (m seriesHashmap) set(hash uint64, s *MemSeries) {
	l := m[hash]
	for i, prev := range l {
		if prev.lset.Equals(s.lset) {
			l[i] = s
			return
		}
	}
	m[hash] = append(l, s)
}
