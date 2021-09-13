package temql

import (
	"fmt"
	"testing"
)

func Test_Parse(t *testing.T) {
	expr := ParseExpr(`(a or b)`)
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

func Test_ParseLabel(t *testing.T) {
	lset, err := ParseLabels(`{job="zhangsan",name="lisi"}`)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(lset)

}
