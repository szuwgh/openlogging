package byteutil

import (
	"fmt"
	"testing"
)

func Test_alloc(t *testing.T) {
	alloc := NewByteBlockAllocator()
	b := alloc.Allocate()
	b[0] = 2
	fmt.Println(b)
	alloc.Recycle([][]byte{b})
	b = alloc.Allocate()
	fmt.Println(b)
}
