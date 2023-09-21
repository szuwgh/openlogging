package byteutil

import (
	"encoding/binary"
	"fmt"

	mybin "github.com/szuwgh/hawkobserve/pkg/engine/tem/mybinary"
)

var errInvalidSize = fmt.Errorf("invalid size")

type EncBuf struct {
	bytes Buffer
}

func (e *EncBuf) Reset()             { e.bytes.Reset() }
func (e *EncBuf) Len() int           { return e.bytes.Len() }
func (e *EncBuf) Get() []byte        { return e.bytes.Bytes() }
func (e *EncBuf) Alloc(n int) []byte { return e.bytes.Alloc(n) }

func (e *EncBuf) PutByte(i uint64, b byte) uint64 {
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

func (e *EncBuf) PutUint32(x uint32) {
	tmp := e.bytes.Alloc(4)
	binary.LittleEndian.PutUint32(tmp, x)
}

func (e *EncBuf) PutVarint(x int) int { return e.PutVarint64(int64(x)) }

func (e *EncBuf) PutUvarintStr(s string) int {
	b := Str2bytes(s)
	n := e.PutVarint(len(b))
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

type Decbuf struct {
	b []byte
	e error
}

func NewDecBuf(b []byte) Decbuf {
	return Decbuf{b: b}
}

func WithErrDecBuf() Decbuf {
	return Decbuf{e: errInvalidSize}
}

func (d *Decbuf) Reset(b []byte) { d.b = b }
func (d *Decbuf) Err() error     { return d.e }
func (d *Decbuf) Len() int       { return len(d.b) }
func (d *Decbuf) Get() []byte    { return d.b }

func (d *Decbuf) Varint() int       { return int(d.Varint64()) }
func (d *Decbuf) Uvarint32() uint32 { return uint32(d.Uvarint64()) }

func (d *Decbuf) Uint32() uint32 {
	b4 := d.Bytes(4)
	return binary.LittleEndian.Uint32(b4)
}

// func (d *decbuf) be32int() int      { return int(d.be32()) }
// func (d *decbuf) be64int64() int64  { return int64(d.be64()) }

func (d *Decbuf) UvarintStr() string {
	l := d.Varint()
	if d.e != nil {
		return ""
	}
	if len(d.b) < int(l) {
		d.e = errInvalidSize
		return ""
	}
	s := string(d.b[:l])
	d.b = d.b[l:]
	return s
}

func (d *Decbuf) ReadByte() {}

func (d *Decbuf) Varint64() int64 {
	if d.e != nil {
		return 0
	}
	x, n := binary.Varint(d.b)
	if n < 1 {
		d.e = errInvalidSize
		return 0
	}
	d.b = d.b[n:]
	return x
}

func (d *Decbuf) Uvarint64() uint64 {
	if d.e != nil {
		return 0
	}
	x, n := binary.Uvarint(d.b)
	if n < 1 {
		d.e = errInvalidSize
		return 0
	}
	d.b = d.b[n:]
	return x
}

func (d *Decbuf) Bytes(l int) []byte {
	if l == 0 {
		return nil
	}
	if l > d.Len() {
		return nil
	}
	b := d.b[:l]
	d.b = d.b[l:]
	return b
}
