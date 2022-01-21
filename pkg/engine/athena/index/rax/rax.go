package rax

/*
#cgo CFLAGS: -I./ -O3 -Wall -Wno-pointer-to-int-cast -Wno-int-to-pointer-cast
#include <stdlib.h>
#include "easy_rax.h"
*/
import "C"
import (
	"sync"
	"unsafe"

	"github.com/szuwgh/athena/pkg/engine/athena/index"
)

var (
	NULL = unsafe.Pointer(nil)
)

type Rax struct {
	tree  unsafe.Pointer
	mu    sync.RWMutex
	isTag bool
}

type RaxIterator struct {
	treeIt unsafe.Pointer
	r      *Rax
}

func New(isTag bool) *Rax {
	tree := C.radix_tree_new()
	rax := &Rax{
		tree:  tree,
		isTag: isTag,
	}
	return rax
}

func (r *Rax) Iterator() index.Iterator {
	iter := &RaxIterator{}
	iter.treeIt = C.radix_tree_new_it(r.tree)
	iter.r = r
	return iter
}

func (r *RaxIterator) Next() bool {
	return C.radix_tree_next(r.treeIt) == 1
}

func (r *RaxIterator) Key() []byte {
	return C.GoBytes(unsafe.Pointer(C.radix_tree_key(r.treeIt)), C.int(C.radix_tree_key_len(r.treeIt)))
}

func (r *RaxIterator) Value() interface{} {
	return (interface{})(unsafe.Pointer(C.radix_tree_value(r.treeIt)))
}

func (r *RaxIterator) IsTag() bool {
	return r.IsTag()
}

func (r *Rax) Find(k []byte) (interface{}, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	data := C.radix_tree_find(r.tree, (*C.uchar)(unsafe.Pointer(&k[0])), C.size_t(len(k)))
	if data == C.raxNotFound {
		return nil, false
	}
	return (interface{})(unsafe.Pointer(data)), true
}

func (r *Rax) IsTag() bool {
	return r.isTag
}

func (r *Rax) Insert(k []byte, v interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	C.radix_tree_insert(r.tree, (*C.uchar)(unsafe.Pointer(&k[0])), C.size_t(len(k)), unsafe.Pointer(&v))
}

func (r *Rax) Free() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	C.radix_tree_destroy(r.tree)
	return nil
}
