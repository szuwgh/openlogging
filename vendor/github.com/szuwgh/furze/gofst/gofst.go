package gofst

import (
	"reflect"
	"unsafe"
)

/*
#cgo CFLAGS: -I.
#cgo LDFLAGS: -L../lib -lfurze
#include "ffi.h"
*/
import "C"

type Builder struct {
	fstBuilder unsafe.Pointer
}

func (b *Builder) Add(k []byte, v uint64) error {
	res := C.add_key(b.fstBuilder, (*C.uchar)(unsafe.Pointer(&k[0])), C.uint(len(k)), C.ulong(v))
	if res == -1 {
		return nil
	}
	return nil
}

func (b *Builder) Finish() error {
	res := C.finish(b.fstBuilder)
	if res == -1 {
		return nil
	}
	return nil
}

func (b *Builder) Bytes() []byte {
	var length, capacity uint32
	bytes := C.bytes(b.fstBuilder, (*C.uint)(unsafe.Pointer(&length)), (*C.uint)(unsafe.Pointer(&capacity)))
	var data []byte
	h := (*reflect.SliceHeader)((unsafe.Pointer(&data)))
	h.Data = uintptr(unsafe.Pointer(bytes))
	h.Len = int(length)
	h.Cap = int(capacity)
	return data
}

func NewBuilder() *Builder {
	return &Builder{
		fstBuilder: C.new_fst_builder(),
	}
}

type FST struct {
	rsFST unsafe.Pointer
}

func (f *FST) Get(k []byte) (uint64, error) {
	res := C.get(f.rsFST, (*C.uchar)(unsafe.Pointer(&k[0])), C.uint(len(k)))
	if res == -1 {
		return 0, nil
	}
	return uint64(res), nil
}

func (f *FST) Get_First_Key(k []byte) (uint64, error) {
	res := C.get_first_key(f.rsFST, (*C.uchar)(unsafe.Pointer(&k[0])), C.uint(len(k)))
	if res == -1 {
		return 0, nil
	}
	return uint64(res), nil
}

func Load(b []byte) *FST {
	rsFST := C.load((*C.uchar)(unsafe.Pointer(&b[0])), C.uint(len(b)), C.uint(cap(b)))
	return &FST{
		rsFST: rsFST,
	}
}
