package disk

import (
	"encoding/binary"

	"github.com/szuwgh/hawkobserve/pkg/engine/tem/chunks"
	"github.com/szuwgh/hawkobserve/pkg/lib/prometheus/labels"
)

type blockHandle struct {
	offset, length uint64
}

type valueIndex struct {
	count  uint64
	chunks []TimeChunk
}

//TimeChunk 时间段
type TimeChunk struct {
	Lset labels.Labels
	Meta []ChunkMetaIndex
}

type ChunkMetaIndex struct {
	chunks.Chunk
	IterIndex int
}

type ChunkMeta struct {
	Ref        uint64 //Length
	MinT, MaxT int64
	LastLogNum uint64
}

// func isEqualChunk(ch1, ch2 ChunkMeta) bool {
// 	if ch1.Ref != ch2.Ref {
// 		return false
// 	}
// 	if ch1.MinT != ch2.MinT {
// 		return false
// 	}
// 	if ch1.MaxT != ch2.MaxT {
// 		return false
// 	}
// 	return true
// }

func (c ChunkMeta) ChunkEnc(isTerm bool, cr chunks.ChunkReader) chunks.ChunkEnc {
	return cr.ReadChunk(isTerm, c.Ref)
}

func (c ChunkMeta) MinTime() int64 {
	return c.MinT
}

func (c ChunkMeta) MaxTime() int64 {
	return c.MaxT
}

func (c ChunkMeta) SegmentNum() uint64 {
	return c.LastLogNum
}

func (c ChunkMeta) Meta(ref uint64) ChunkMeta {
	c.Ref = ref
	return c
}

func decodeBlockHandle(src []byte) (blockHandle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return blockHandle{}, 0
	}
	return blockHandle{offset, length}, n + m
}

func encodeBlockHandle(dst []byte, b blockHandle) int {
	n := binary.PutUvarint(dst, b.offset)
	m := binary.PutUvarint(dst[n:], b.length)
	return n + m
}

func encodeBlockOffset(dst []byte, offset uint64) int {
	n := binary.PutUvarint(dst, offset)
	return n
}

func encodeBlockLength(dst []byte, length uint64) {
	dst[0] = 0
	dst[1] = 0
	dst[2] = 0
	binary.PutUvarint(dst, length)
}

func decodeValueIndex(dst []byte, lastT int64) (valueIndex, int) {
	return valueIndex{}, 0
}

func decodeUvarint(dst []byte, n int) (uint64, int) {
	i, k := binary.Uvarint(dst[n:])
	return i, n + k
}

func decodeVarint(dst []byte, n int) (int64, int) {
	i, k := binary.Varint(dst[n:])
	return i, n + k
}

func encodeInt(dst []byte, v int) int {
	n := binary.PutUvarint(dst, uint64(v))
	return n
}

func decodeInt(dst []byte) (int, int) {
	v, n := binary.Uvarint(dst)
	return int(v), n
}

func encodeUInt64(dst []byte, v uint64) int {
	n := binary.PutUvarint(dst, v)
	return n
}

func decodeUInt64(dst []byte) (uint64, int) {
	v, n := binary.Uvarint(dst)
	return v, n
}
