package parse

import (
	"fmt"
	"testing"
)

func Test_Lex(t *testing.T) {
	l := newLex("abcd!@#$%^&*(){}[]")
	for i := l.next(); i != eof; i = l.next() {
		fmt.Printf("%c\n", i)
	}
}
