package parse

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
	case *termBinaryExpr:
		//fmt.Println("termBinaryExpr")
		printExpr(v.Expr)
	case *termExpr:
		fmt.Println("termExpr")
	}
	for _, vv := range v.LabelMatchers {
		fmt.Println(*vv)
	}
}

func printExpr(e Expr) {
	switch e.(type) {
	case *termBinaryExpr:
		//fmt.Println("termBinaryExpr")
		e := e.(*termBinaryExpr)
		printExpr(e.LHS)
		fmt.Print(keyType[e.Op], " ")
		printExpr(e.RHS)
	case *termExpr:
		e := e.(*termExpr)
		fmt.Print(e.name, " ")
	}
}
