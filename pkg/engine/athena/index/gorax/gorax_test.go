package gorax

import (
	"fmt"
	"testing"
)

func Test_gorax(t *testing.T) {
	rax := New(true)
	b := "a"
	//c := "c"
	rax.Insert([]byte("a"), b)
	rax.Insert([]byte("c"), b)
	rax.Insert([]byte("b"), b)
	rax.Insert([]byte("f"), b)
	rax.Insert([]byte("faaa"), b)
	rax.Insert([]byte("eeee"), b)
	rax.Insert([]byte("aaaa"), b)
	rax.Insert([]byte("eeee"), b)
	//rax.Insert([]byte("b"), unsafe.Pointer(b))
	//rax.show()
	//iter := rax.Iterator()

	v, ok := rax.Find([]byte("a"))
	fmt.Println("ok", ok)
	if ok {
		fmt.Println("ok", v)
	}

	iter := rax.Iterator()
	for iter.Next() {
		fmt.Println(string(iter.Key()), iter.Value())
	}

}
