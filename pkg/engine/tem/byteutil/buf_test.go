package byteutil

import (
	"fmt"
	"testing"
)

func Test_EncBuf(t *testing.T) {
	e := EncBuf{}
	e.PutVarint(1234)

	d := NewDecBuf(e.Get())
	s := d.Varint()
	fmt.Println(s)
}
