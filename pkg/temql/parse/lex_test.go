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

func Test_LexItem(t *testing.T) {
	l := newLex(`xxx{job="zhangsan",name="lisi"}`)
	var item Item
	for {
		l.nextItem(&item)
		if item.Typ == EOF {
			break
		}
		fmt.Println(item)
	}
}
