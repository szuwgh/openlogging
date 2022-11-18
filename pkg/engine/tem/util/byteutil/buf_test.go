package byteutil

import (
	"fmt"
	"testing"
)

func Test_EncBuf(t *testing.T) {
	e := EncBuf{}
	e.PutVarint(12345544)

	d := NewDecBuf(e.Get())
	s := d.Varint()
	fmt.Println(s)
}
