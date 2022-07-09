package chunks

//快照块
type SnapBlock interface {
	ReadByte() (byte, error)
	Seek(uint64)
	ReadVInt() int
	ReadVUInt64() uint64
	ReadVInt64() int64
}

type LogPosting interface {
	Next() bool
	Seek(v uint64) bool
	At() uint64
}

type LogID struct {
	LogBytes SnapBlock
}

type TermSnapReader interface {
	Encode() (SnapBlock, SnapBlock, []SnapBlock)
	Bytes() [][]byte
}

type TermSnapShot struct {
	snapReader    TermSnapReader
	baseTimeStamp int64
	segmentNum    uint64
}

func NewTermSnapShot() *TermSnapShot {
	termSna := &TermSnapShot{}
	return termSna
}

func (s *TermSnapShot) Iterator(minT, maxT int64, segmentNum uint64) Postings {
	t := &TermPosting{}
	logFreqr, posr, skipsr := s.snapReader.Encode()
	t.skipReader = make([]SnapBlock, len(skipsr))
	for i := 0; i < len(skipsr); i++ {
		t.skipReader[i] = skipsr[i]
	}
	t.skipLastID = make([]uint64, len(skipsr))
	t.notfirst = make([]bool, len(skipsr))
	t.logFreqReader = logFreqr
	t.posReader = posr
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
	Encode() SnapBlock
	Bytes() [][]byte
}

type SeriesSnapShot struct {
	snapReader    SeriesSnapReader
	baseTimeStamp int64
	segmentNum    uint64
}

func NewSeriesSnapShot() *SeriesSnapShot {
	seriesSna := &SeriesSnapShot{}
	return seriesSna
}

func (s *SeriesSnapShot) Iterator(minT, maxT int64, segmentNum uint64) Postings {
	l := &labelPosting{}
	logr := s.snapReader.Encode()
	l.logReader = logr
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
