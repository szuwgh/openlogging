package temql

import (
	"fmt"
	"testing"
)

func Test_Parse(t *testing.T) {
	expr := ParseExpr(`{job="zhangsan",name="lisi"}`)
	v := expr.(*VectorSelector)
	fmt.Println(v.Name)
	switch v.Expr.(type) {
	case *TermBinaryExpr:
		//fmt.Println("termBinaryExpr")
		printExpr(v.Expr)
	case *TermExpr:
		fmt.Println("termExpr")
	}
	for _, vv := range v.LabelMatchers {
		fmt.Println(*vv)
	}
}
