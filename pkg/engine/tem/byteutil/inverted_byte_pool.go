package byteutil

import (
	"sync"

	bin "github.com/szuwgh/temsearch/pkg/engine/tem/mybinary"
)

//内存分配
type InvertedBytePool struct {
	// buffers  [][]byte //数据
	// buffer   []byte   //current buffer
	// bufIndex int
	bytePool
	//usedBufSize  uint64
	MaxblockSize int

	baseTimeStamp int64

	mu sync.Mutex
}

func (b *InvertedBytePool) GetBuffers() [][]byte {
	return b.buffers
}

//可用内存
func (b *InvertedBytePool) free() uint64 {
	return BYTE_BLOCK_SIZE - b.byteSize
}

func NewInvertedBytePool(alloc Allocator) *InvertedBytePool {
	bp := &InvertedBytePool{}
	bp.alloc = alloc
	MaxblockSize := 10 //10个内存块
	bp.buffers = make([][]byte, 0, MaxblockSize)
	bp.bufOffset = 0
	bp.bufIndex = -1
	return bp
}

//initBytes 申请新的bytes
// 9 9 9 9 9
func (ib *InvertedBytePool) InitBytes(skiplistLevel int) uint64 {
	var size uint64
	var num uint64 = 2 + uint64(skiplistLevel)
	//num++
	size = num * SizeClass[0]
	offset := ib.newBytes(size)
	for i := uint64(0); i < num; i++ {
		ib.buffer[ib.bufOffset-i*SizeClass[0]-PointerLen] = classEOP[0]
	}
	return offset
}

//newBytes 申请内存
func (ib *InvertedBytePool) newBytes(size uint64) uint64 {
	if ib.bufIndex == -1 {
		ib.newBuffer()
	}

	if ib.bufOffset >= uint64(BYTE_BLOCK_SIZE-size) {
		ib.newBuffer()
	}
	offset := ib.bufOffset
	ib.bufOffset += size
	return offset + uint64(ib.usedBufSize)
}

func (ib *InvertedBytePool) setByte(i uint64, b byte) {
	m, n := turn(i)
	ib.buffers[m][n] = b
}

func (ib *InvertedBytePool) Byte(m, n uint64) byte {
	return ib.buffers[m][n]
}

func (ib *InvertedBytePool) Bytes(m, n, l uint64) []byte {
	return ib.buffers[m][n : n+l]
}

func (ib *InvertedBytePool) Alloc(slice []byte, size byte) []byte {
	ib.mu.Lock()
	defer ib.mu.Unlock()

	newLevel := size
	if newLevel >= 10 {
		newLevel = 9
	}
	newSize := SizeClass[newLevel]
	offset := ib.newBytes(newSize)
	ib.buffer[ib.bufOffset-PointerLen] = classEOP[newLevel]
	length := len(slice)
	for i := 0; i < length; i++ {
		slice[i] = byte(offset >> uint(8*(3-i)))
	}
	return ib.buffer[offset : offset+newSize]
}

//allocBytes 扩容bytes
func (ib *InvertedBytePool) allocBytes(slice []byte, size byte) uint64 {

	newLevel := size
	if newLevel >= 10 {
		newLevel = 9
	}
	newSize := SizeClass[newLevel]
	offset := ib.newBytes(newSize)
	ib.buffer[ib.bufOffset-PointerLen] = classEOP[newLevel]
	length := len(slice)
	for i := 0; i < length; i++ {
		slice[i] = byte(offset >> uint(8*(3-i)))
	}
	return offset
}

//writeByte 写入byte
func (ib *InvertedBytePool) writeByte(i uint64, b byte) uint64 {
	m, n := turn(i)
	buffer := ib.buffers[m]
	if buffer[n] == 0 {
		buffer[n] = b
	} else {
		//申请新的内存
		i = ib.allocBytes(buffer[n:n+4], buffer[n])
		m, n = turn(i)
		ib.buffers[m][n] = b
	}
	i = i + 1
	ib.byteSize++
	return i
}

func (ib *InvertedBytePool) PutByte(i uint64, b byte) uint64 {
	return ib.writeByte(i, b)
}

func (ib *InvertedBytePool) WriteBytes(i uint64, b []byte) (uint64, int) {
	return ib.writeBytes(i, b)
}

func (ib *InvertedBytePool) writeBytes(i uint64, b []byte) (uint64, int) {
	end := len(b)
	for offset := 0; offset < end; offset++ {
		i = ib.writeByte(i, b[offset])
	}
	return i, end
}

func (ib *InvertedBytePool) writeVInt(i uint64, b int) (uint64, int) {
	var size int
	i, size = bin.Putvarint(ib, i, b)
	return i, size
}

func (ib *InvertedBytePool) WriteVInt(i uint64, b int) (uint64, int) {
	return ib.writeVInt(i, b)
}

func (ib *InvertedBytePool) WriteVInt64(i uint64, b int64) (uint64, int) {
	var size int
	i, size = bin.Putvarint64(ib, i, b)
	return i, size
}

func (ib *InvertedBytePool) WriteVUint64(i uint64, b uint64) (uint64, int) {
	var size int
	i, size = bin.PutUvarint64(ib, i, b)
	return i, size
}

func (ib *InvertedBytePool) WriteString(i uint64, s string) (uint64, int) {
	var size int
	length := 0
	i, length = ib.writeVInt(i, len(s))
	size += length
	i, length = ib.writeBytes(i, []byte(s))
	size += length
	return i, size
}

func (ib *InvertedBytePool) BaseTime() int64 {
	return ib.baseTimeStamp
}

func turn(i uint64) (uint64, uint64) {
	m := i / BYTE_BLOCK_SIZE
	n := i & (BYTE_BLOCK_SIZE - 1)
	return m, n
}
