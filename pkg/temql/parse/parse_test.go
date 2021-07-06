package parse

import (
	"fmt"
	"testing"
)

func Test_Parse(t *testing.T) {
	p := newParser(`(aa and bb){job="zhangsan",name="lisi"}`)
	expr := p.parseGenerated()
	v := expr.(*VectorSelector)
	fmt.Println(v.Name)
	for _, vv := range v.LabelMatchers {
		fmt.Println(*vv)
	}
}
