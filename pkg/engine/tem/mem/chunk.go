package mem

import "github.com/sophon-lab/temsearch/pkg/engine/tem/mybinary"

type chunkAlloc interface {
	Alloc([]byte, byte) []byte
}

type chunk struct {
	data []byte
	a    chunkAlloc
	i    uint64
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
	return 0, i
}

func (c *chunk) putVInt(b int) int {
	var size int
	c.i, size = mybinary.Putvarint(c, c.i, b)
	return size
}

func (c *chunk) putVInt64(b int64) int {
	var size int
	c.i, size = mybinary.Putvarint(c, c.i, b)
	return 0
}

type labelChunk struct {
	log chunk
}

type temChunk struct {
	logFreq chunk
	skip    chunk
	pos     chunk
}
