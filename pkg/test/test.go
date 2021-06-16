package test

import (
	"log"
	"runtime"
	"sync"
)

var s *Stack

func init1() {
	s = NewStack()
	for i := 0; i < 8192; i++ {
		s.Push(make([]byte, 8192))
	}
}
func main() {
	//	fmt.Println(a)
	init1()
	printMemStats()
	//a = a[0:1]
	//fmt.Println(a)
	for i := 0; i < 8000; i++ {
		s.Pop()
	}
	printMemStats()
	runtime.GC()
	printMemStats()
	//fmt.Println(filepath.Glob("tst_!(abc-a)-*.log"))
}

// HeapSys：程序向应用程序申请的内存
// HeapAlloc：堆上目前分配的内存
// HeapIdle：堆上目前没有使用的内存
// HeapReleased：回收到操作系统的内存
func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Alloc = %v TotalAlloc = %v Sys = %v NumGC = %v\n", m.Alloc/1024, m.TotalAlloc/1024, m.Sys/1024, m.NumGC)
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
func NewStack() *Stack {
	return &Stack{nil, 0, &sync.RWMutex{}}
}

// Return the number of items in the stack
func (stack *Stack) Len() int {
	return stack.length
}

// View the top item on the stack
func (stack *Stack) Peek() []byte {
	if stack.length == 0 {
		return nil
	}
	return stack.top.value
}

// Pop the top item of the stack and return it
func (stack *Stack) Pop() []byte {
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
func (stack *Stack) Push(value []byte) {
	stack.lock.Lock()
	defer stack.lock.Unlock()
	n := &node{value, stack.top}
	stack.top = n
	stack.length++
}
