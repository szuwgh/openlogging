package chunks

import (
	"github.com/sophon-lab/temsearch/core/indexer/lsm/global"
)

//快照块
type SnapBlock interface {
	ReadByte() (byte, error)
	Seek(uint64)
	//	Bytes() [][]byte
	ReadVLong() int64
	ReadVInt() int
	ReadVUInt64() uint64
	ReadVInt64() int64
}

type LogPosting interface {
	Next() bool
	// Seek advances the iterator to value v or greater and returns
	// true if a value was found.
	Seek(v uint64) bool

	// At returns the value at the current iterator position.
	At() uint64
}

type LogID struct {
	LogBytes SnapBlock
}

type TermSnapReader interface {
	//LogFreqReader() snapBlock
	//PosReader() snapBlock
	//SkipReader() [global.FreqSkipListLevel]snapBlock
	Encode() (SnapBlock, SnapBlock, [global.FreqSkipListLevel]SnapBlock, uint64)
	Bytes() [][]byte
}

type TermSnapShot struct {
	//LogFreqBytes snapBlock
	//PosBytes     snapBlock
	//SkipBytes    [global.FreqSkipListLevel]snapBlock
	snapReader    TermSnapReader
	baseTimeStamp int64
	segmentNum    uint64
	//buf           []byte
}

func NewTermSnapShot() *TermSnapShot {
	termSna := &TermSnapShot{}
	//termSna.buf = buf
	return termSna
}

func (s *TermSnapShot) Iterator(minT, maxT int64) Postings {
	t := &TermPosting{}
	logFreqr, posr, skipsr, segmentNum := s.snapReader.Encode()
	t.skipReader = make([]SnapBlock, global.FreqSkipListLevel)
	for i := 0; i < global.FreqSkipListLevel; i++ {
		t.skipReader[i] = skipsr[i] //s.snapReader.SkipReader()[i] //snapReader{data: s.SkipBytes[i]}
	}
	t.logFreqReader = logFreqr //s.snapReader.LogFreqReader() //snapReader{data: s.LogFreqBytes}
	t.posReader = posr         //s.snapReader.PosReader() //snapReader{data: s.PosBytes}
	t.minTimeStamp = minT
	t.maxTimeStamp = maxT
	t.baseTimeStamp = s.baseTimeStamp
	t.segmentNum = segmentNum
	return t
}

func (s *TermSnapShot) Bytes() [][]byte {
	return s.snapReader.Bytes()
}

func (s *TermSnapShot) SetTimeStamp(timestamp int64) {
	s.baseTimeStamp = timestamp
}

func (s *TermSnapShot) SetSegmentNum(segmentNum uint64) {
	s.segmentNum = segmentNum
}

func (s *TermSnapShot) SetSnapReader(snapReader TermSnapReader) {
	s.snapReader = snapReader
}

type SeriesSnapReader interface {
	//LogFreqReader() snapBlock
	//PosReader() snapBlock
	//SkipReader() [global.FreqSkipListLevel]snapBlock
	Encode() (SnapBlock, uint64)
	Bytes() [][]byte
}

type SeriesSnapShot struct {
	//LogBytes      SnapBlock
	snapReader    SeriesSnapReader
	baseTimeStamp int64
	segmentNum    uint64
	//buf           []byte
}

func NewSeriesSnapShot() *SeriesSnapShot {
	seriesSna := &SeriesSnapShot{}
	//seriesSna.buf = buf
	return seriesSna
}

func (s *SeriesSnapShot) Iterator(minT, maxT int64) Postings {
	l := &labelPosting{}
	logr, segmentNum := s.snapReader.Encode()
	l.logReader = logr //snapReader{data: s.LogBytes}
	l.minTimeStamp = minT
	l.maxTimeStamp = maxT
	l.segmentNum = segmentNum
	return l
}

func (s *SeriesSnapShot) Bytes() [][]byte {
	return s.snapReader.Bytes()
}

func (s *SeriesSnapShot) SetSegmentNum(segmentNum uint64) {
	s.segmentNum = segmentNum
}

func (s *SeriesSnapShot) SetSnapReader(snapReader SeriesSnapReader) {
	s.snapReader = snapReader
}
