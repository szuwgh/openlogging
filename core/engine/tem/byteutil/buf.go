package byteutil

import (
	mybin "github.com/sophon-lab/temsearch/core/engine/tem/binary"
)

type EncBuf struct {
	bytes Buffer
	//shareBuf [binary.MaxVarintLen64]byte
}

func (e *EncBuf) Reset()             { e.bytes.Reset() }
func (e *EncBuf) Len() int           { return e.bytes.Len() }
func (e *EncBuf) Get() []byte        { return e.bytes.Bytes() }
func (e *EncBuf) Alloc(n int) []byte { return e.bytes.Alloc(n) }

func (e *EncBuf) WriteByte(i uint64, b byte) uint64 {
	e.bytes.WriteByte(b)
	return 0
}

func (e *EncBuf) PutUvarint64(x uint64) int {
	_, i := mybin.PutUvarint64(e, 0, x)
	return i
}

func (e *EncBuf) PutVarint64(x int64) int {
	_, i := mybin.Putvarint64(e, 0, x)
	return i
}

func (e *EncBuf) PutUvarint(x int) int { return e.PutUvarint64(uint64(x)) }

func (e *EncBuf) PutUvarintStr(s string) int {
	b := Str2bytes(s)
	n := e.PutUvarint(len(b))
	e.write(b)
	return n + len(b)
}

func (e *EncBuf) write(bufs ...[]byte) (int, error) {
	var n int
	for _, b := range bufs {
		i, err := e.bytes.Write(b)
		n += i
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (e *EncBuf) Write(bufs ...[]byte) (int, error) {
	return e.write(bufs...)
}
