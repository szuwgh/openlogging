package byteutil

const BYTE_BLOCK_SIZE = 1 << 15 //64 //1 << 15 //64 //32 //1 << 15
const blockSize = 4 * 1024
const (
	PointerLen = 4
)

var (
	//SizeClass = []uint64{9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	SizeClass    = []uint64{9, 18, 24, 34, 44, 64, 84, 104, 148, 204}
	classEOP     = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA}
	SizeClassLen = len(SizeClass)
)

type Inverted interface {
	InitBytes(skiplistLevel int) uint64
	Byte(uint64, uint64) byte
	Bytes(uint64, uint64, uint64) []byte
	WriteString(uint64, string) (uint64, int)
	WriteBytes(uint64, []byte) (uint64, int)
	WriteVInt(uint64, int) (uint64, int)
	WriteVInt64(uint64, int64) (uint64, int)
	WriteVUint64(uint64, uint64) (uint64, int)
	BaseTime() int64
	Release(recycle, alloced *int)
	//Size() int
}

type Forward interface {
	WriteMsg([]byte) int
	Iterator() ForwardIterator
	UseByteOffset() uint64
	Buffer(i uint64) []byte
	Bytes(m, n, l uint64) []byte
	Release(recycle, alloced *int)
	//BlockSize() uint64
}

type ForwardIterator interface {
	NextBuffer() bool
	NextBytes() bool
	Bytes() []byte
}

type bytePool struct {
	buffers     [][]byte //数据
	buffer      []byte   //current buffer
	bufIndex    int
	usedBufSize int
	bufOffset   uint64
	alloc       Allocator
	byteSize    uint64 //已使用内存大小
}

func (bp *bytePool) newBuffer() {
	newBuf := bp.alloc.Allocate()
	bp.buffers = append(bp.buffers, newBuf)
	bp.buffer = newBuf
	bp.bufOffset = 0
	bp.bufIndex++
	if bp.bufIndex > 0 {
		bp.usedBufSize += BYTE_BLOCK_SIZE
	}
}

func (bp *bytePool) Release(recycle, alloced *int) {
	for i := range bp.buffers {
		fill(bp.buffers[i], 0)
	}

	*alloced += len(bp.buffers)
	if *recycle == 0 {
		bp.alloc.Recycle(bp.buffers)
	} else if *alloced >= *recycle {
		bp.alloc.Recycle(bp.buffers[:*alloced-*recycle])
	}
	*recycle = *alloced
	bp.buffers = bp.buffers[:0]
	bp.bufOffset = 0
	bp.bufIndex = -1
	bp.usedBufSize = 0
	bp.byteSize = 0
	bp.buffer = nil
}

// func (bp *bytePool) Size() int {
// 	return bp.byteSize
// }
