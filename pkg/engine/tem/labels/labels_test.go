package labels

import (
	"fmt"
	"testing"
)

func Test_ToByte(t *testing.T) {
	l := Label{}
	l.Name = "pod"
	l.Value = "sss"
	fmt.Println(l.ToByte())
	fmt.Println([]byte(fmt.Sprint(l)))
}
