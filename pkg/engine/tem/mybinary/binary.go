package mybinary

type bufRead interface {
	ReadByte() (byte, error)
}

type bufWrite interface {
	PutByte(uint64, byte) uint64
}

// //‭0101 1010‬
// //‭1100 1000‬
// //1 0010 0010
// //‭000100100010‬
// func PlusUint32(b []byte, v uint32) {
// 	_ = b[3] // early bounds check to guarantee safety of writes below
// 	x := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
// 	v = x + v
// 	b[0] = byte(v)
// 	b[1] = byte(v >> 8)
// 	b[2] = byte(v >> 16)
// 	b[3] = byte(v >> 24)
// }

//PutUvarint
func PutUvarint64(buf bufWrite, n uint64, x uint64) (uint64, int) {
	i := 0
	for x >= 0x80 {
		n = buf.PutByte(n, byte(x)|0x80)
		x >>= 7
		i++
	}
	n = buf.PutByte(n, byte(x))
	return n, i + 1
}

func Putvarint(buf bufWrite, n uint64, x int) (uint64, int) {
	return Putvarint64(buf, n, int64(x))
}

func Putvarint64(buf bufWrite, n uint64, x int64) (uint64, int) {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return PutUvarint64(buf, n, ux)

}

//Uvarint
func Uvarint64(buf bufRead) uint64 {
	var x uint64
	var s uint
	var i int
	for {
		b, err := buf.ReadByte()
		if err != nil {
			return 0
		}
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return 0 // overflow
			}
			return x | uint64(b)<<s
		}
		x |= uint64(b&0x7f) << s
		s += 7
		i++
	}
}

func Varint64(buf bufRead) int64 {
	ux := Uvarint64(buf) // ok to continue in presence of error
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x
}
