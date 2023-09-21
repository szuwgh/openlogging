package mem

import (
	"github.com/szuwgh/hawkobserve/pkg/engine/tem/index"

	"github.com/szuwgh/hawkobserve/pkg/engine/tem/chunks"
	"github.com/szuwgh/hawkobserve/pkg/engine/tem/disk"
)

type memIterator struct {
	iter    index.Iterator
	chunkr  chunks.ChunkReader
	seriesr seriesReader
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

func (m *memIterator) msg(w disk.IndexWriter, segmentNum uint64) ([]disk.TimeChunk, []uint64, error) {
	termP := m.value().(*TermPosting)
	postings := termP.toPosting()
	seriesPosting := make([]uint64, 0, len(postings))
	timeChunk := make([]disk.TimeChunk, 0, len(postings))
	for _, p := range postings {
		// chunkEnc := p.ChunkEnc(true, m.chunkr)
		// chunkRef, err := w.WriteChunks(chunkEnc.Bytes())

		// if err != nil {
		// 	return nil, nil, err
		// }
		chk := disk.TimeChunk{Lset: p.lset}
		chk.Meta = append(chk.Meta, disk.ChunkMetaIndex{Chunk: p, IterIndex: 0})
		timeChunk = append(timeChunk, chk)
		seriesRef, _ := w.GetSeries(p.lset)
		seriesPosting = append(seriesPosting, seriesRef)
	}
	return timeChunk, seriesPosting, nil
}

func (s *memIterator) series(w disk.IndexWriter, segmentNum uint64) ([]disk.TimeChunk, []uint64, error) {
	p := s.value().(*LabelPosting)
	var seriesPosting []uint64
	timeChunk := make([]disk.TimeChunk, 0, len(p.seriesID))
	seriesList := p.toPosting(s.seriesr)
	for _, series := range seriesList {
		seriesRef, exist := w.GetSeries(series.lset)
		if !exist {
			// chunkEnc := series.ChunkEnc(false, s.chunkr)
			// chunkRef, err := w.WriteChunks(chunkEnc.Bytes())
			// if err != nil {
			// 	return nil, nil, err
			// }
			chk := disk.TimeChunk{Lset: series.lset}
			chk.Meta = append(chk.Meta, disk.ChunkMetaIndex{Chunk: series, IterIndex: 0})
			timeChunk = append(timeChunk, chk)
		} else {
			seriesPosting = append(seriesPosting, seriesRef)
		}
	}
	return timeChunk, seriesPosting, nil
}

func (i *memIterator) ChunksPosting(w disk.IndexWriter, segmentNum uint64, iterIndex int) ([]disk.TimeChunk, []uint64, error) {
	if i.iter.IsTag() {
		return i.series(w, segmentNum)
	}
	return i.msg(w, segmentNum)
}

func (i *memIterator) ChunkByte(isTerm bool, c chunks.Chunk) chunks.ChunkEnc {
	return c.ChunkEnc(isTerm, i.chunkr)
}

// func (i *memIterator) Write(w disk.IndexWriter, segmentNum uint64, baseTime int64) ([]disk.TimeChunk, []uint64, error) {
// 	if i.iter.IsTag() {
// 		return i.writeSeries(w, segmentNum, baseTime)
// 	}
// 	return i.writeMsg(w, segmentNum, baseTime)

// }
