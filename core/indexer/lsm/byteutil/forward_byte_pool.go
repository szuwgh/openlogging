package byteutil

import (
	bin "github.com/sophon-lab/temsearch/core/indexer/lsm/binary"
)

type ForwardBytePool struct {
	bytePool
	endIndex      []uint64
	useByteOffset uint64
}

func NewForwardBytePool(alloc Allocator) *ForwardBytePool {
	fb := &ForwardBytePool{}
	fb.alloc = alloc
	MaxblockSize := 10 //10个内存块
	fb.buffers = make([][]byte, 0, MaxblockSize)
	fb.endIndex = make([]uint64, 0, MaxblockSize)
	fb.bufOffset = 0
	fb.bufIndex = -1
	//fb.newBuffer()
	return fb
}

func (fb *ForwardBytePool) newBuffer() {
	fb.bytePool.newBuffer()
	fb.endIndex = append(fb.endIndex, BYTE_BLOCK_SIZE)
	//fb.endIndex[fb.bufIndex] = fb.blockSize // uint64(fb.alloc.BlockSize()) //BYTE_BLOCK_SIZE
}

func (fb *ForwardBytePool) Buffer(i uint64) []byte {
	return fb.buffers[i]
}

func (fb *ForwardBytePool) Bytes(m, n, l uint64) []byte {
	return fb.buffers[m][n : n+l]
}

func (fb *ForwardBytePool) WriteByte(i uint64, b byte) uint64 {
	return fb.writeByteAt(i, b)
}

//writeByte 写入byte
func (fb *ForwardBytePool) writeByte(b byte) uint64 {
	fb.buffer[fb.bufOffset] = b
	fb.bufOffset++
	return fb.bufOffset
}

func (fb *ForwardBytePool) BlockSize() uint64 {
	return fb.BlockSize()
}

func (fb *ForwardBytePool) writeByteAt(i uint64, b byte) uint64 {
	if fb.bufIndex == -1 {
		fb.newBuffer()
	}
	fb.buffer[i] = b
	i++
	return i
}

func (fb *ForwardBytePool) writeBytes(l int, b []byte) error {
	// end := len(b)
	// for offset := 0; offset < end; offset++ {
	// 	fb.writeByte(b[offset])
	// }
	m := uint64(l)
	if fb.bufOffset+m >= BYTE_BLOCK_SIZE {
		n := BYTE_BLOCK_SIZE - fb.bufOffset
		copy(fb.buffer[fb.bufOffset:], b[:n])
		fb.bufOffset += n
		fb.newBuffer()
		fb.writeBytes(len(b[n:]), b[n:])
	} else {
		copy(fb.buffer[fb.bufOffset:], b)
		fb.bufOffset += m
	}
	return nil
}

func (fb *ForwardBytePool) writeVInt(b int) (size int) {
	var offset uint64
	offset, size = bin.Putvarint(fb, uint64(fb.bufOffset), b)
	fb.bufOffset = offset
	return
}

func (fb *ForwardBytePool) writeUVInt(b uint64) (size int) {
	var offset uint64
	offset, size = bin.PutUvarint64(fb, fb.bufOffset, b)
	fb.bufOffset = offset
	return
}

func (fb *ForwardBytePool) writeString(s string) (size int) {
	return fb.writeMsg(str2bytes(s))
}

func (fb *ForwardBytePool) WriteMsg(s []byte) (size int) {
	return fb.writeMsg(s)
}

func (fb *ForwardBytePool) UseByteOffset() uint64 {
	return fb.useByteOffset
}

func (fb *ForwardBytePool) writeMsg(s []byte) (size int) {
	l := len(s)
	size = fb.writeUVInt(uint64(l))
	fb.writeBytes(l, s)
	n := size + l
	fb.useByteOffset += uint64(n)
	fb.endIndex[fb.bufIndex] = fb.bufOffset
	m := BYTE_BLOCK_SIZE - fb.bufOffset
	if m < 4 {
		fb.useByteOffset += m
		fb.newBuffer()
	}
	return n
}

func (bp *ForwardBytePool) Release(recycle, alloced *int) {
	bp.bytePool.Release(recycle, alloced)
	bp.endIndex = bp.endIndex[:0]
	bp.useByteOffset = 0
}

func (fb *ForwardBytePool) Iterator() ForwardIterator {
	iter := &ForwardDefaultIterator{}
	iter.buffers = fb.buffers
	iter.endIndex = fb.endIndex
	iter.bufIndex = fb.bufIndex
	iter.i = -1
	return iter
}

type ForwardDefaultIterator struct {
	buffers    [][]byte //数据
	endIndex   []uint64
	bufIndex   int
	offset     int
	n, m, j, i int

	b []byte
}

func (iter *ForwardDefaultIterator) NextBuffer() bool {
	iter.i++
	if iter.i > iter.bufIndex {
		return false
	}
	endIndex := iter.endIndex[iter.i]
	iter.m = int(endIndex / blockSize)
	iter.n = int(endIndex % blockSize)
	iter.j = 0
	iter.offset = 0
	return true

}

func (iter *ForwardDefaultIterator) NextBytes() bool {

	if iter.j > iter.m {
		return false
	}
	if iter.j == iter.m {
		if iter.n == 0 {
			return false
		}
		iter.b = iter.buffers[iter.i][iter.offset : iter.offset+iter.n]
		iter.j++
		return true
	}
	iter.b = iter.buffers[iter.i][iter.offset : iter.offset+blockSize]
	iter.offset += blockSize
	iter.j++
	return true

}

func (iter *ForwardDefaultIterator) Bytes() []byte {
	return iter.b
}
