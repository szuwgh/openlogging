package byteutil

import (
	"encoding/binary"
	"errors"

	bin "github.com/sophon-lab/temsearch/core/indexer/lsm/binary"
	"github.com/sophon-lab/temsearch/core/indexer/lsm/chunks"
	"github.com/sophon-lab/temsearch/core/indexer/lsm/global"
)

//对byte块进行写入
type InvertedBytePoolReader struct {
	bytePool         Inverted
	limit            uint64 //限制能读多少内容
	offset, endIndex uint64
	level            int //块层数
	baseTime         int64
	curBytes         []byte
	//buf              [binary.MaxVarintLen32]byte
	encbuf *EncBuf
	//这个块最后能读的索引
}

func NewInvertedBytePoolReader(bytePool Inverted, baseTime int64) *InvertedBytePoolReader {
	r := &InvertedBytePoolReader{}
	r.bytePool = bytePool
	r.baseTime = baseTime
	r.encbuf = &EncBuf{}
	return r
}

func (r *InvertedBytePoolReader) SetBaseTime(baseTime int64) {
	r.baseTime = baseTime
}

func (r *InvertedBytePoolReader) ReadChunk(isTerm bool, ref ...uint64) chunks.ChunkEnc { //Postings
	if isTerm {
		return r.readTermChunk(ref...)
	}
	return r.readLabelChunk(ref...)
}

func (r *InvertedBytePoolReader) readLabelChunk(ref ...uint64) chunks.ChunkEnc {
	snap := chunks.NewSeriesSnapShot()
	//snap.SetTimeStamp(r.baseTime)
	seriesSnap := &memSeriesSnapReader{}
	seriesSnap.ref = ref
	seriesSnap.r = r.Clone()
	snap.SetSnapReader(seriesSnap)
	return snap
}

type memSeriesSnapReader struct {
	r   *InvertedBytePoolReader
	ref []uint64
}

func (m *memSeriesSnapReader) Encode() (chunks.SnapBlock, uint64) {
	byteStart := m.ref[0]
	seriesIndex := m.ref[1]
	seriesLen := m.ref[2]
	logFreqSnap := NewSnapBlock(byteStart,
		seriesIndex,
		seriesLen, m.r.Clone())
	return logFreqSnap, 0
}

func (m *memSeriesSnapReader) Bytes() [][]byte {
	byteStart := m.ref[0]
	seriesIndex := m.ref[1]
	seriesLen := m.ref[2]
	var b [][]byte
	m.r.encbuf.Reset()
	m.r.encbuf.PutUvarint(0)
	m.r.encbuf.PutUvarint64(seriesLen)
	b = append(b, m.r.encbuf.Get())
	m.r.initReader(byteStart, seriesIndex)
	for m.r.Next() {
		b = append(b, m.r.Block())
	}
	return b
}

type memTermSnapReader struct {
	r   *InvertedBytePoolReader
	ref []uint64
}

func (m *memTermSnapReader) Encode() (chunks.SnapBlock, chunks.SnapBlock, [global.FreqSkipListLevel]chunks.SnapBlock, uint64) {
	byteStart := m.ref[0]
	logFreqIndex := m.ref[1]
	logFreqLen := m.ref[2]
	skipIndex := m.ref[3 : 3+global.FreqSkipListLevel]
	skipLen := m.ref[3+global.FreqSkipListLevel : 3+global.FreqSkipListLevel+global.FreqSkipListLevel]
	posIndex := m.ref[3+global.FreqSkipListLevel+global.FreqSkipListLevel]
	posLen := m.ref[3+global.FreqSkipListLevel+global.FreqSkipListLevel+1]
	logFreqSnap := NewSnapBlock(byteStart,
		logFreqIndex,
		logFreqLen,
		m.r.Clone())
	var skipSnap [global.FreqSkipListLevel]chunks.SnapBlock
	for i := 0; i < global.FreqSkipListLevel; i++ {
		skipSnap[i] = NewSnapBlock(byteStart+SizeClass[0]*uint64(1+i),
			skipIndex[i],
			skipLen[i],
			m.r.Clone())
	}
	posSnap := NewSnapBlock(byteStart+SizeClass[0]*(global.FreqSkipListLevel+1),
		posIndex,
		posLen,
		m.r.Clone())
	return logFreqSnap, posSnap, skipSnap, 0
}

func (m *memTermSnapReader) Bytes() [][]byte {
	byteStart := m.ref[0]
	logFreqIndex := m.ref[1]
	logFreqLen := m.ref[2]
	skipIndex := m.ref[3 : 3+global.FreqSkipListLevel]
	skipLen := m.ref[3+global.FreqSkipListLevel : 3+global.FreqSkipListLevel+global.FreqSkipListLevel]
	posIndex := m.ref[3+global.FreqSkipListLevel+global.FreqSkipListLevel]
	posLen := m.ref[3+global.FreqSkipListLevel+global.FreqSkipListLevel+1]
	var b [][]byte
	m.r.encbuf.Reset()
	m.r.encbuf.PutUvarint(0)
	m.r.encbuf.PutUvarint64(logFreqLen)

	for i := 0; i < global.FreqSkipListLevel; i++ {
		m.r.encbuf.PutUvarint64(skipLen[i])
	}
	m.r.encbuf.PutUvarint64(posLen)
	b = append(b, m.r.encbuf.Get())
	m.r.initReader(byteStart, logFreqIndex)
	for m.r.Next() {
		b = append(b, m.r.Block())
	}
	for i := 0; i < global.FreqSkipListLevel; i++ {
		m.r.initReader(byteStart+SizeClass[0]*uint64(1+i), skipIndex[i])
		for m.r.Next() {
			b = append(b, m.r.Block())
		}
	}
	m.r.initReader(byteStart+SizeClass[0]*(global.FreqSkipListLevel+1), posIndex)
	for m.r.Next() {
		b = append(b, m.r.Block())
	}
	return b
}

func (r *InvertedBytePoolReader) readTermChunk(ref ...uint64) chunks.ChunkEnc {
	searchSnap := chunks.NewTermSnapShot() //&chunks.TermSnapShot{}
	searchSnap.SetTimeStamp(r.baseTime)
	termSnap := &memTermSnapReader{}
	termSnap.ref = ref
	termSnap.r = r.Clone()
	searchSnap.SetSnapReader(termSnap)
	return searchSnap
}

func (r *InvertedBytePoolReader) Clone() *InvertedBytePoolReader {
	replica := &InvertedBytePoolReader{}
	replica.bytePool = r.bytePool
	replica.encbuf = r.encbuf
	replica.baseTime = r.baseTime
	return replica
}

func (r *InvertedBytePoolReader) Init(startIndex, endIndex, length uint64) {
	r.initReader(startIndex, endIndex)
}

func (r *InvertedBytePoolReader) initReader(startIndex, endIndex uint64) *InvertedBytePoolReader {
	r.offset = startIndex
	r.endIndex = endIndex
	r.level = 0
	//r.read = 0
	if startIndex+uint64(SizeClass[r.level])-PointerLen >= endIndex {
		//只有一个块能读
		r.limit = endIndex //startIndex + length
	} else {
		//有多个块 先读第一个块
		r.limit = startIndex + uint64(SizeClass[r.level]) - 4
	}
	return r
}

func (r *InvertedBytePoolReader) scanByte(i uint64) byte {
	m, n := turn(i)
	//r.read++
	return r.bytePool.Byte(m, n)
}

func (r *InvertedBytePoolReader) scanByteBlock(i, length uint64) []byte {
	m, n := turn(i)
	//r.read += length
	return r.bytePool.Bytes(m, n, length) //r.bytePoolr.buffers[m][n : n+length]
}

//剩余未读
func (r *InvertedBytePoolReader) getByteLeft() uint64 {
	return r.limit - r.offset
}

func (r *InvertedBytePoolReader) nextBlock() {
	if r.level < 9 {
		r.level++
	}
	newIndex := ((uint64(r.scanByte(r.limit)) & 0xff) << 24) +
		((uint64(r.scanByte(1+r.limit)) & 0xff) << 16) +
		((uint64(r.scanByte(2+r.limit)) & 0xff) << 8) +
		(uint64(r.scanByte(3+r.limit)) & 0xff)
	r.offset = newIndex
	if newIndex+uint64(SizeClass[r.level])-PointerLen >= r.endIndex {
		r.limit = r.endIndex
	} else {
		r.limit = newIndex + uint64(SizeClass[r.level]) - 4
	}
}

// func (r *InvertedBytePoolReader) readInt32(i int) int {
// 	m, n := turn(i)
// 	return int(binary.LittleEndian.Uint32(r.bytePool.buffers[m][n:]))
// }

func (r *InvertedBytePoolReader) ReadByte() (byte, error) {
	return r.readByte(), nil
}

func (r *InvertedBytePoolReader) readByte() byte {
	if r.offset >= r.endIndex {
		return 0
	}
	if r.offset == r.limit {
		r.nextBlock()
	}
	b := r.scanByte(r.offset)
	r.offset++
	//r.read++
	return b
}

//都可变长Int
func (r *InvertedBytePoolReader) readVInt() int {
	return int(bin.Varint64(r))
}

func (r *InvertedBytePoolReader) readVInt64() int64 {
	return bin.Varint64(r)
}

func (r *InvertedBytePoolReader) readVUInt64() uint64 {
	return bin.Uvarint64(r)
}

func (r *InvertedBytePoolReader) readString() string {
	return string(r.readTerm())
}

func (r *InvertedBytePoolReader) readTerm() []byte {
	length := r.readVInt()
	return r.readBytes(uint64(length))
}

//读数据
func (r *InvertedBytePoolReader) readBytes(len uint64) []byte {
	b := make([]byte, len)
	var offset uint64
	for len > 0 {
		sizeLeft := r.getByteLeft()
		if len > sizeLeft {
			copy(b[offset:offset+sizeLeft], r.scanByteBlock(r.offset, sizeLeft)) //r.getBuffer()[r.offset:r.limit]
			r.nextBlock()
			len -= sizeLeft
			offset += sizeLeft
		} else {
			copy(b[offset:], r.scanByteBlock(r.offset, len))
			r.offset += len
			break
		}
	}
	return b
}

//快照
type snapBlock struct {
	//startIndex int
	endIndex uint64
	limit    uint64 //读取限制
	offset   uint64
	r        *InvertedBytePoolReader
	cur      *fragBlock
	n        uint64
	len      uint64
}

func NewSnapBlock(startIndex, endIndex, limit uint64, r *InvertedBytePoolReader) *snapBlock {
	snapBlock := &snapBlock{}
	snapBlock.r = r
	snapBlock.endIndex = endIndex
	snapBlock.limit = limit
	snapBlock.r.initReader(startIndex, endIndex)
	return snapBlock
}

func (s *snapBlock) Bytes() [][]byte {
	var tmp [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(tmp[0:], s.limit)
	var b [][]byte
	b = append(b, tmp[:n])
	for s.r.Next() {
		b = append(b, s.r.Block())
	}
	return b
}

func (s *snapBlock) Seek(offset uint64) {
	if s.endIndex == 0 {
		return
	}
	if s.cur == nil {
		s.cur = &fragBlock{}
		s.cur = s.r.readFrag(&s.n, &s.len)
	}
	for offset > s.cur.absolute {
		x := s.cur
		if s.cur.next == nil {
			s.cur.next = s.r.readFrag(&s.n, &s.len)
		}
		s.cur = s.cur.next
		s.cur.prev = x
	}
	for offset < (s.cur.absolute - s.cur.limit) {
		s.cur = s.cur.prev
	}
	s.offset = offset
}

func (s *snapBlock) ReadByte() (byte, error) {
	if s.offset >= s.limit {
		return 0, errors.New("EOF")
	}
	if s.cur == nil {
		s.cur = &fragBlock{}
		s.cur = s.r.readFrag(&s.n, &s.len)
	}
	if s.offset >= s.cur.absolute {
		x := s.cur
		if s.cur.next == nil {
			s.cur.next = s.r.readFrag(&s.n, &s.len)
		}
		s.cur = s.cur.next
		s.cur.prev = x
	}
	b := s.cur.data[s.cur.limit-(s.cur.absolute-s.offset)]
	s.offset++
	return b, nil
}

func (s *snapBlock) ReadVLong() int64 {
	return bin.Varint64(s)
}

func (s *snapBlock) ReadVInt() int {
	return int(bin.Varint64(s))
}

func (s *snapBlock) ReadVUInt64() uint64 {
	return bin.Uvarint64(s)
}

func (s *snapBlock) ReadVInt64() int64 {
	return bin.Varint64(s)
}

// type byteBlock struct {
// 	cur    *fragBlock
// 	offset int
// 	limit  int
// }

type fragBlock struct {
	data     []byte
	limit    uint64 //读取限制
	absolute uint64 //绝对长度
	prev     *fragBlock
	next     *fragBlock
}

func (r *InvertedBytePoolReader) readFrag(n, len *uint64) *fragBlock {
	frag := &fragBlock{}
	*len = r.limit - r.offset
	frag.data = r.scanByteBlock(r.offset, *len)
	frag.limit = *len
	*n += *len
	frag.absolute = *n
	if r.limit < r.endIndex {
		r.nextBlock()
	}
	return frag
}

func (r *InvertedBytePoolReader) Block() []byte {
	return r.curBytes
}

func (r *InvertedBytePoolReader) Next() bool {
	if r.offset >= r.endIndex {
		return false
	}
	r.curBytes = r.scanByteBlock(r.offset, r.limit-r.offset)
	if r.limit >= r.endIndex {
		r.offset = r.limit
	} else {
		r.nextBlock()
	}
	return true
}

type ForwardBytePoolReader struct {
	bytePool Forward
	buffer   []byte
	limit    uint64 //限制能读多少内容
	offset   uint64 //偏移
	n        uint64

	len      uint64
	curBytes []byte
}

func NewForwardBytePoolReader(bytePool Forward) *ForwardBytePoolReader {
	r := &ForwardBytePoolReader{}
	r.bytePool = bytePool
	return r
}

func (r *ForwardBytePoolReader) Init(offset uint64) *ForwardBytePoolReader {
	r.offset = offset
	var m uint64
	m, r.n = turn(r.offset)
	r.buffer = r.bytePool.Buffer(m) //r.bytePool.buffers[m]
	return r
}

func (r *ForwardBytePoolReader) InitReader(offset uint64) uint64 {
	r.offset = offset
	var m uint64
	m, r.n = turn(r.offset)
	r.buffer = r.bytePool.Buffer(m) //r.bytePool.buffers[m]
	r.len = r.readVUInt64()
	return r.len
}

func (r *ForwardBytePoolReader) Block() []byte {
	return r.curBytes
}

func (r *ForwardBytePoolReader) Next() bool {
	if r.len <= 0 {
		return false
	}
	m, n := turn(r.offset)
	if n+r.len >= BYTE_BLOCK_SIZE {
		l := BYTE_BLOCK_SIZE - n
		r.offset += l
		r.len -= l
		r.curBytes = r.getBuffer(m, n, l)
	} else {
		len := r.len
		r.len = 0
		r.curBytes = r.getBuffer(m, n, len)
	}
	return true
}

func (r *ForwardBytePoolReader) ReadByte() (byte, error) {
	return r.readByte(), nil
}

func (r *ForwardBytePoolReader) readByte() byte {
	b := r.buffer[r.n]
	r.n++
	r.offset++
	return b
}

func (r *ForwardBytePoolReader) getBuffer(m, n, l uint64) []byte {
	return r.bytePool.Bytes(m, n, l)
}

//都可变长Int
func (r *ForwardBytePoolReader) readVInt() int {
	return int(bin.Varint64(r))
}

func (r *ForwardBytePoolReader) readBytes(len uint64) []byte {
	b := make([]byte, len)
	r.copyBytes(b, len)
	return b
}

func (r *ForwardBytePoolReader) copyBytes(dst []byte, len uint64) {
	m, n := turn(r.offset)
	if n+len >= BYTE_BLOCK_SIZE {
		l := BYTE_BLOCK_SIZE - n
		copy(dst, r.getBuffer(m, n, l))
		r.offset += l
		r.copyBytes(dst[l:], len-l)
	} else {
		copy(dst, r.getBuffer(m, n, len))
		r.offset += len
	}
}

func (r *ForwardBytePoolReader) readVInt64() int64 {
	return bin.Varint64(r)
}

func (r *ForwardBytePoolReader) readVUInt64() uint64 {
	return bin.Uvarint64(r)
}

func (r *ForwardBytePoolReader) ReadString() []byte {
	length := r.readVUInt64()
	return r.readBytes(length)
}

func (r *ForwardBytePoolReader) GetNextBlock() []byte {
	return nil
}
