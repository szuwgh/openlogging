package parse

import (
	"github.com/sophon-lab/temsearch/pkg/temql/labels"
)

type parser struct {
	lexer *lexer

	yyParser yyParserImpl

	generatedParserResult interface{}
}

func newParser(input string) *parser {
	return &parser{
		lexer: newLex(input),
	}
}

func (p *parser) newLabelMatcher(label Item, operator Item, value Item) *labels.Matcher {
	op := operator.Typ

	// Map the Item to the respective match type.
	var matchType labels.MatchType
	switch op {
	case EQL:
		matchType = labels.MatchEqual
	default:
		// This should never happen, since the error should have been caught
		// by the generated parser.
		panic("invalid operator")
	}
	return labels.NewMatcher(matchType, label.Val, value.Val)
}

func (p *parser) newBinaryExpr(lhs Node, op Item, rhs Node) Node {
	ret := &termBinaryExpr{}
	ret.LHS = lhs.(Expr)
	ret.RHS = rhs.(Expr)
	ret.Op = op.Typ
	return ret
}

// func (p *parser) newTermExpr(name1 Item, operator Item, name2 Item) *termExpr {
// 	op := operator.Typ

// 	var matchType term.MatchType
// 	switch op {
// 	case LAND:
// 		matchType = term.MatchAnd
// 	case LOR:
// 		matchType = term.MatchOr
// 	default:
// 		panic("invalid operator")
// 	}
// 	return &termExpr{name1.Val, name2.Val, matchType}
// }

func (p *parser) newTermExpr(name Item) Node {
	return &termExpr{name.Val}
}

func (p *parser) parseGenerated() interface{} {
	p.yyParser.Parse(p)
	return p.generatedParserResult
}

func (p *parser) Lex(lval *yySymType) int {

	p.lexer.nextItem(&lval.item)
	typ := lval.item.Typ
	switch typ {
	case ERROR:
		return 0
	case EOF:
	}
	return int(typ)
}

func (p *parser) Error(s string) {

}

func (p *parser) unexpected(context string, expected string) {

}
