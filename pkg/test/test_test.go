package test

import (
	"fmt"
	"testing"
	"unsafe"
)

type ii interface {
	debug()
}

type Str string

func (s Str) debug() {
	fmt.Println(s)
}

func Byte2Str(b string) Str {
	return *(*Str)(unsafe.Pointer(&b))
}

func Test_a(t *testing.T) {
	a := "aa"
	b := Byte2Str(a)
	var i ii = b
	i.debug()
}
