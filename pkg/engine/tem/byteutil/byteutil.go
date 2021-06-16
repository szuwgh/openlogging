package byteutil

import "unsafe"

// Releaser is the interface that wraps the basic Release method.
type Releaser interface {
	// Release releases associated resources. Release should always success
	// and can be called multiple times without causing error.
	Release()
}

func Str2bytes(s string) []byte {
	return str2bytes(s)
}

func str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func Byte2Str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func fill(b []byte, value byte) {
	for i := range b {
		b[i] = value
	}
}
