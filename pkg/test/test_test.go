package test

import (
	"fmt"
	"testing"
)

type Str string

func Test_str(t *testing.T) {
	a := "aa"
	b := Str(a)
	fmt.Println(b)
}
