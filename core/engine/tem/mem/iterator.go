package mem

import (
	"github.com/sophon-lab/temsearch/core/engine/tem/index"

	"github.com/sophon-lab/temsearch/core/engine/tem/chunks"
	"github.com/sophon-lab/temsearch/core/engine/tem/disk"
)

type memIterator struct {
	iter    index.Iterator
	chunkr  chunks.ChunkReader // posting.InvertListReader
	seriesr seriesReader
	//isTag   bool
}

func (i *memIterator) First() bool {
	return i.iter.Next()
}

func (i *memIterator) Next() bool {
	return i.iter.Next() //i.nextIndex != 0
}

func (i *memIterator) Value() []byte {
	return nil
}

func (i *memIterator) value() interface{} {
	return i.iter.Value()
}

func (i *memIterator) Key() []byte {
	return i.iter.Key()
}

func (m *memIterator) writeMsg(w disk.IndexWriter, segmentNum uint64, baseTime int64) ([]disk.TimeChunk, []uint64, error) {
	termP := m.value().(*TermPosting)
	postings := termP.toPosting()
	seriesPosting := make([]uint64, 0, len(postings))
	timeChunk := make([]disk.TimeChunk, 0, len(postings))
	for _, p := range postings {
		chunkEnc := p.ChunkEnc(true, m.chunkr)
		chunkRef, err := w.WriteChunks(chunkEnc.Bytes())
		if err != nil {
			return nil, nil, err
		}
		chk := disk.TimeChunk{Lset: p.lset}
		chk.Meta = append(chk.Meta, disk.ChunkMeta{Ref: chunkRef, MinT: p.minT, MaxT: p.maxT})
		timeChunk = append(timeChunk, chk)

		seriesRef, _ := w.GetSeries(p.lset)
		seriesPosting = append(seriesPosting, seriesRef)
	}
	return timeChunk, seriesPosting, nil
}

func (s *memIterator) writeSeries(w disk.IndexWriter, segmentNum uint64, baseTime int64) ([]disk.TimeChunk, []uint64, error) {
	p := s.value().(*LabelPosting)
	var seriesPosting []uint64
	timeChunk := make([]disk.TimeChunk, 0, len(p.seriesID))
	seriesList := p.toPosting(s.seriesr)
	for _, series := range seriesList {
		seriesRef, exist := w.GetSeries(series.lset)
		if !exist {
			chunkEnc := series.ChunkEnc(false, s.chunkr)
			chunkRef, err := w.WriteChunks(chunkEnc.Bytes())
			if err != nil {
				return nil, nil, err
			}
			chk := disk.TimeChunk{Lset: series.lset}
			chk.Meta = append(chk.Meta, disk.ChunkMeta{Ref: chunkRef, MinT: series.minT, MaxT: series.maxT})
			timeChunk = append(timeChunk, chk)
		} else {
			seriesPosting = append(seriesPosting, seriesRef)
		}
	}
	return timeChunk, seriesPosting, nil
}

func (i *memIterator) Write(w disk.IndexWriter, segmentNum uint64, baseTime int64) ([]disk.TimeChunk, []uint64, error) {
	if i.iter.IsTag() {
		return i.writeSeries(w, segmentNum, baseTime)
	}
	return i.writeMsg(w, segmentNum, baseTime)

}
