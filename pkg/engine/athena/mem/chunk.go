package mem

import "github.com/szuwgh/athena/pkg/engine/athena/mybinary"

type chunkAlloc interface {
	Alloc([]byte, byte) []byte
}

type chunk struct {
	data []byte
	a    chunkAlloc
	//i    uint64
}

func (c *chunk) PutByte(i uint64, b byte) uint64 {
	if c.data[i] == 0 {
		c.data[i] = b
	} else {
		//申请新的内存
		c.data = c.a.Alloc(c.data[i:i+4], c.data[i])
		i = 0
		c.data[i] = b
	}
	i++
	return i
}

func (c *chunk) putVInt(b int) int {
	var size int
	//i, size = mybinary.Putvarint(c, i, b)

	return size
}

func (c *chunk) putVInt64(b int64) int {
	var size int
	var i uint64
	i, size = mybinary.Putvarint64(c, i, b)
	c.data = c.data[i:]
	return size
}

type labelChunk struct {
	log chunk
}

type temChunk struct {
	logFreq chunk
	skip    []chunk
	pos     chunk
}
