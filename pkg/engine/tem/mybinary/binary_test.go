package mybinary

import (
	"testing"
)

type testBuf struct {
	data   []byte
	offset int
}

func (t *testBuf) ReadByte() (byte, error) {
	b := t.data[t.offset]
	t.offset++
	return b, nil
}

func (t *testBuf) PutByte(i uint64, b byte) uint64 {
	t.data[i] = b
	i++
	return i
}

//
func Test_PutUvarint(t *testing.T) {
	tBuf := &testBuf{}
	tBuf.data = make([]byte, 40)
	i := uint64(0)
	i, _ = PutUvarint64(tBuf, i, 10)
	i, _ = PutUvarint64(tBuf, i, 11)
	i, _ = PutUvarint64(tBuf, i, 3780)
	t.Log(tBuf.data)
	t.Log(Uvarint64(tBuf))
	t.Log(Uvarint64(tBuf))
	t.Log(Uvarint64(tBuf))
}

func Test_Putvarint(t *testing.T) {
	tBuf := &testBuf{}
	tBuf.data = make([]byte, 40)
	i := uint64(0)
	i, _ = Putvarint64(tBuf, i, 10)
	i, _ = Putvarint64(tBuf, i, 11)
	i, _ = Putvarint64(tBuf, i, 3780)
	t.Log(tBuf.data)
	t.Log(Varint64(tBuf))
	t.Log(Varint64(tBuf))
	t.Log(Varint64(tBuf))

	// x, n := binary.Varint(tBuf.data)
	// x1, m := binary.Varint(tBuf.data[n:])
	// x2, _ := binary.Varint(tBuf.data[n+m:])
	// t.Log(x)
	// t.Log(x1)
	// t.Log(x2)
}

func Test_Binary(t *testing.T) {
	a := 90
	b := 200

	var a1 [4]byte
	a1[0] = byte(a)
	a1[1] = byte(a >> 8)
	a1[2] = byte(a >> 16)
	a1[3] = byte(a >> 24)

	a1[0] = (a1[0] + byte(b))
	t.Log(uint32(a1[0] + byte(b)))

}
