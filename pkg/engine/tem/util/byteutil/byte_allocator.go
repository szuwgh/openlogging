package byteutil

import (
	"sync"
)

type Allocator interface {
	Recycle(blocks [][]byte)
	Allocate() []byte
	Len() int
	BlockSize() int
	GC()
}

type ByteGC interface {
	alloc() []byte
	recycle(blocks []byte)
	len() int
	gc()
}

type ByteBlockGoPoolAllocator struct {
}

type ByteBlockStackAllocator struct {
	blockSize      int
	freeByteBlocks ByteGC
}

func NewByteBlockStackAllocator() Allocator {
	alloc := &ByteBlockStackAllocator{}
	alloc.freeByteBlocks = newStack()
	return alloc
}

func (alloc *ByteBlockStackAllocator) Allocate() []byte {
	var b []byte
	if alloc.freeByteBlocks.len() == 0 {
		b = make([]byte, BYTE_BLOCK_SIZE)
	} else {
		b = alloc.freeByteBlocks.alloc()
	}
	return b
}

func (alloc *ByteBlockStackAllocator) BlockSize() int {
	return alloc.blockSize
}

func (alloc *ByteBlockStackAllocator) Recycle(blocks [][]byte) {
	for i := range blocks {
		alloc.freeByteBlocks.recycle(blocks[i])
	}
}

func (alloc *ByteBlockStackAllocator) Len() int {
	return alloc.freeByteBlocks.len()
}

func (alloc *ByteBlockStackAllocator) GC() {
	alloc.freeByteBlocks.gc()
}

type (
	Stack struct {
		top    *node
		length int
		lock   *sync.RWMutex
	}
	node struct {
		value []byte
		prev  *node
	}
)

// Create a new stack
func newStack() *Stack {
	return &Stack{nil, 0, &sync.RWMutex{}}
}

func (stack *Stack) gc() {
	// stack.lock.Lock()
	// defer stack.lock.Unlock()
	// for stack.len() > 0 {
	// 	stack.alloc()
	// }
}

// Return the number of items in the stack
func (stack *Stack) len() int {
	stack.lock.RLock()
	defer stack.lock.RUnlock()
	return stack.length
}

// View the top item on the stack
func (stack *Stack) peek() []byte {
	if stack.length == 0 {
		return nil
	}
	return stack.top.value
}

// Pop the top item of the stack and return it
func (stack *Stack) alloc() []byte {
	stack.lock.Lock()
	defer stack.lock.Unlock()
	if stack.length == 0 {
		return nil
	}
	n := stack.top
	stack.top = n.prev
	stack.length--
	return n.value
}

// Push a value onto the top of the stack
func (stack *Stack) recycle(value []byte) {
	stack.lock.Lock()
	defer stack.lock.Unlock()
	n := &node{value, stack.top}
	stack.top = n
	stack.length++
}
