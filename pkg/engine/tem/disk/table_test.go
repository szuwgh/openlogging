package disk

import (
	"testing"
)

func Test_EncodeDecodeValueIndex(t *testing.T) {
	//var dst [48]byte
	// value := valueIndex{}
	// value.count = 3
	// buf := encBuf{}
	// var baseT int64
	// value.chunks = make([]TimeChunk, 3)
	// var minT int64
	// for i := 0; i < 3; i++ {
	// 	maxT := minT + int64(i+1)
	// 	value.chunks[i] = TimeChunk{Offset: 1 + uint64(i), Length: 10, MinT: minT + int64(i), MaxT: maxT}
	// 	minT = maxT
	// }
	// t.Log(value)
	// encodeValueIndex(&buf, baseT, value)
	// t.Log(buf.bytes.Bytes())
	// value1, _ := decodeValueIndex(buf.bytes.Bytes(), baseT)
	// t.Log(value1)
	// for i := 0; i < len(value.chunks); i++ {
	// 	if value.chunks[i] != value1.chunks[i] {
	// 		t.Fatal("chunk not equal", i, value.chunks[i], value1.chunks[i])
	// 	}
	// }

	// t.Log("ok")

}
