package temql

import (
	"fmt"
	"reflect"

	"github.com/szuwgh/temsearch/pkg/lib/prompb"
)

type Expr interface {
	Node
}

type Node interface {
	// String representation of the node that returns the given node when parsed
	// as part of a valid query.
	String() string
}

type VectorSelector struct {
	Name string

	Expr Expr

	LabelMatchers []*prompb.LabelMatcher
}

func (*VectorSelector) String() string {
	return "VectorSelector"
}

type TermBinaryExpr struct {
	Op       ItemType // The operation of the expression.
	LHS, RHS Expr     // The operands on the respective sides of the operator.
}

func (t *TermBinaryExpr) Print() {
	printExpr(t)
}

func printExpr(e Expr) {
	switch e.(type) {
	case *TermBinaryExpr:
		e := e.(*TermBinaryExpr)
		fmt.Println("e.LHS", reflect.ValueOf(e.LHS))
		fmt.Println("e.RHS", reflect.ValueOf(e.RHS))
		printExpr(e.LHS)
		fmt.Print(keyType[e.Op], " ")
		printExpr(e.RHS)
	case *TermExpr:
		e := e.(*TermExpr)
		fmt.Print(e.Name, " ")
	}
}

func (t *TermBinaryExpr) String() string {
	return "termBinaryExpr"
}

type TermExpr struct {
	Name string
}

func (t *TermExpr) String() string {
	return "term"
}

// type termExpr struct {
// 	name1, name2 string
// 	Op           ItemType // The operation of the expression.
// }

// func (t *termExpr) String() string {
// 	return "termExpr"
// }
