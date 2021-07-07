package temql

import "github.com/sophon-lab/temsearch/pkg/temql/labels"

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

	Expr Node

	LabelMatchers []*labels.Matcher
}

func (*VectorSelector) String() string {
	return "VectorSelector"
}

type TermBinaryExpr struct {
	Op       ItemType // The operation of the expression.
	LHS, RHS Expr     // The operands on the respective sides of the operator.
}

func (t *TermBinaryExpr) String() string {
	return "termBinaryExpr"
}

type TermExpr struct {
	name string
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
