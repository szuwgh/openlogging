package temql

import (
	"fmt"
	"testing"
)

func Test_Parse(t *testing.T) {
	p := newParser(`( aa AND (BB OR cc) )`)
	expr := p.parseGenerated()
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

func printExpr(e Expr) {
	switch e.(type) {
	case *TermBinaryExpr:
		//fmt.Println("termBinaryExpr")
		e := e.(*TermBinaryExpr)
		printExpr(e.LHS)
		fmt.Print(keyType[e.Op], " ")
		printExpr(e.RHS)
	case *TermExpr:
		e := e.(*TermExpr)
		fmt.Print(e.name, " ")
	}
}
