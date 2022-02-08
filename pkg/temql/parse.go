package temql

import (
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"
	"github.com/szuwgh/temsearch/pkg/lib/prometheus/strutil"
	"github.com/szuwgh/temsearch/pkg/lib/prompb"
)

type parser struct {
	lexer *lexer

	inject    ItemType
	injecting bool

	yyParser yyParserImpl

	generatedParserResult interface{}

	parseErrors ParseErrors
}

type ParseErrors []ParseErr

// ParseErr wraps a parsing error with line and position context.
type ParseErr struct {
	PositionRange PositionRange
	Err           error
	Query         string

	// LineOffset is an additional line offset to be added. Only used inside unit tests.
	LineOffset int
}

// PositionRange describes a position in the input string of the parser.
type PositionRange struct {
	Start Pos
	End   Pos
}

func newParser(input string) *parser {
	return &parser{
		lexer: newLex(input),
	}
}

func ParseLabels(input string) (labels.Labels, error) {
	ls, err := ParseMetric(input)
	if err != nil {
		return nil, err
	}
	sort.Sort(ls)
	return ls, nil
}

func ParseMetric(input string) (m labels.Labels, err error) {
	p := newParser(input)

	parseResult := p.parseGenerated(START_METRIC)
	if parseResult != nil {
		m = parseResult.(labels.Labels)
	}
	return m, err
}

func ParseExpr(input string) interface{} {
	p := newParser(input)
	return p.parseGenerated(START_EXPRESSION)
}

func (p *parser) newLabelMatcher(label Item, operator Item, value Item) *prompb.LabelMatcher {
	op := operator.Typ

	var matchType prompb.LabelMatcher_Type
	switch op {
	case EQL:
		matchType = prompb.LabelMatcher_EQ
	default:

		panic("invalid operator")
	}

	return &prompb.LabelMatcher{Type: matchType, Name: label.Val, Value: strings.Trim(value.Val, `"`)} //labels.NewMatcher(matchType, label.Val, value.Val)
}

func (p *parser) newBinaryExpr(lhs Node, op Item, rhs Node) Node {
	ret := &TermBinaryExpr{}
	ret.LHS = lhs.(Expr)
	ret.RHS = rhs.(Expr)
	ret.Op = op.Typ
	return ret
}

func (p *parser) newTermExpr(name Item) Node {
	return &TermExpr{name.Val}
}

func (p *parser) parseGenerated(startSymbol ItemType) interface{} {
	p.InjectItem(startSymbol)
	p.yyParser.Parse(p)
	return p.generatedParserResult
}

func (p *parser) InjectItem(typ ItemType) {
	if p.injecting {
		panic("cannot inject multiple Items into the token stream")
	}

	// if typ != 0 && (typ <= startSymbolsStart || typ >= startSymbolsEnd) {
	// 	panic("cannot inject symbol that isn't start symbol")
	// }
	p.inject = typ
	p.injecting = true
}

func (p *parser) Lex(lval *yySymType) int {
	if p.injecting {
		p.injecting = false
		return int(p.inject)
	}
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

func (p *parser) unquoteString(s string) string {
	unquoted, err := strutil.Unquote(s)
	if err != nil {
		p.addParseErrf(p.yyParser.lval.item.PositionRange(), "error unquoting string %q: %s", s, err)
	}
	return unquoted
}

func (p *parser) addParseErrf(positionRange PositionRange, format string, args ...interface{}) {
	p.addParseErr(positionRange, errors.Errorf(format, args...))
}

// addParseErr appends the provided error to the list of parsing errors.
func (p *parser) addParseErr(positionRange PositionRange, err error) {
	perr := ParseErr{
		PositionRange: positionRange,
		Err:           err,
		Query:         p.lexer.input,
	}

	p.parseErrors = append(p.parseErrors, perr)
}
