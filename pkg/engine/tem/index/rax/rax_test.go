package rax

import (
	"fmt"
	"testing"
)

type a struct {
	b map[string]int
}

func Test_rax(t *testing.T) {
	rax := New(true)
	b := &a{
		b: make(map[string]int),
	}
	//c := "c"
	rax.Insert([]byte("a"), b)
	//rax.Insert([]byte("b"), unsafe.Pointer(b))
	//rax.show()
	//iter := rax.Iterator()

	_, ok := rax.Find([]byte("ab"))
	fmt.Println("ok", ok)
	if ok {
		//b1 := (*a)(pointer)
		//b1.b = append(b1.b, "a")
		//fmt.Println(b1.b)
	}

}
