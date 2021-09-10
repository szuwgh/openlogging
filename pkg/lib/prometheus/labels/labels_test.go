package labels

import (
	"fmt"
	"testing"
)

type Tag interface {
	Tag() string
}

func Test_ToByte(t *testing.T) {
	l := Label{}
	l.Name = "pod"
	l.Value = "sss"
	var a Tag = &l
	fmt.Println(l.ToByte())
	fmt.Println(a.Tag())
	fmt.Println([]byte(fmt.Sprint(l)))
}
