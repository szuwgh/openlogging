package gorax

import (
	"sync"

	iradix "github.com/hashicorp/go-immutable-radix"
	"github.com/szuwgh/athena/pkg/engine/athena/index"
)

type GoRax struct {
	tree  *iradix.Tree
	mu    sync.RWMutex
	isTag bool
}

type GoRaxIterator struct {
	r    *GoRax
	iter *iradix.Iterator
	k    []byte
	v    interface{}
}

func New(isTag bool) *GoRax {
	rax := &GoRax{
		tree:  iradix.New(),
		isTag: isTag,
	}
	return rax
}

func (r *GoRax) Iterator() index.Iterator {
	i := &GoRaxIterator{}
	i.iter = r.tree.Root().Iterator()
	i.iter.SeekLowerBound([]byte("a"))
	i.r = r
	return i
}

func (r *GoRaxIterator) Next() bool {
	var ok bool
	r.k, r.v, ok = r.iter.Next()
	return ok
}

func (r *GoRaxIterator) Key() []byte {
	return r.k
}

func (r *GoRaxIterator) Value() interface{} {
	return r.v
}

func (r *GoRaxIterator) IsTag() bool {
	return r.r.IsTag()
}

func (r *GoRax) Find(k []byte) (interface{}, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tree.Get(k)
}

func (r *GoRax) IsTag() bool {
	return r.isTag
}

func (r *GoRax) Insert(k []byte, v interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tree, _, _ = r.tree.Insert(k, v)
}

func (r *GoRax) Free() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tree = nil
	return nil
}
