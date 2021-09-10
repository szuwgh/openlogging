package disk

import (
	"encoding/binary"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/chunks"
	"github.com/sophon-lab/temsearch/pkg/lib/prometheus/labels"
)

type blockHandle struct {
	offset, length uint64
}

type valueIndex struct {
	//	index uint64
	count  uint64
	chunks []TimeChunk
}

//TimeChunk 时间段
type TimeChunk struct {
	Lset labels.Labels
	Meta []ChunkMeta
}

type ChunkMeta struct {
	Ref        uint64 // Length
	MinT, MaxT int64
}

func isEqualChunk(ch1, ch2 ChunkMeta) bool {
	if ch1.Ref != ch2.Ref {
		return false
	}
	if ch1.MinT != ch2.MinT {
		return false
	}
	if ch1.MaxT != ch2.MaxT {
		return false
	}
	return true
}

func (c ChunkMeta) ChunkEnc(isTerm bool, cr chunks.ChunkReader) chunks.ChunkEnc {
	return cr.ReadChunk(isTerm, c.Ref)
}

func (c ChunkMeta) MinTime() int64 {
	return c.MinT
}

func (c ChunkMeta) MaxTime() int64 {
	return c.MaxT
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

// func encodeValueIndex(e *encBuf, lastT int64, b valueIndex) {
// 	e.putUvarint64(b.count)
// 	var offset uint64
// 	//var lastMinT int64
// 	for i := range b.chunks {
// 		e.putUvarint64(b.chunks[i].Offset - offset)
// 		//e.putUvarint64(b.chunks[i].Length)
// 		e.putVarint64(b.chunks[i].MinT - lastT)
// 		e.putVarint64(b.chunks[i].MaxT - b.chunks[i].MinT)
// 		offset = b.chunks[i].Offset
// 		lastT = b.chunks[i].MaxT
// 	}
// }

func decodeValueIndex(dst []byte, lastT int64) (valueIndex, int) {
	return valueIndex{}, 0
	// count, n := binary.Uvarint(dst)
	// chunks := make([]ChunkMeta, count)
	// var lastOffset, offset uint64
	// var t int64
	// for i := uint64(0); i < count; i++ {
	// 	ch := ChunkMeta{}
	// 	offset, n = decodeUvarint(dst, n)
	// 	ch.Ref = lastOffset + offset
	// 	//	ch.Length, n = decodeUvarint(dst, n)
	// 	t, n = decodeVarint(dst, n)
	// 	ch.MinT = lastT + t
	// 	t, n = decodeVarint(dst, n)
	// 	ch.MaxT = ch.MinT + t
	// 	lastT = ch.MaxT
	// 	lastOffset = ch.Ref
	// 	chunks[i] = ch
	// }
	// return valueIndex{count: count, chunks: chunks}, n
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
