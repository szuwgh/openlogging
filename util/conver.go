package util

import "unsafe"

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

func Str2Int64(s string) int64 {

	return 0
}
