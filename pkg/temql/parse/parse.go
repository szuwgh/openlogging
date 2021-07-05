package parse

import "github.com/sophon-lab/temsearch/pkg/temql/labels"

type parser struct {
	lexer *lex

	//yyParser yyParserImpl

	generatedParserResult interface{}
}

func (p *parser) newLabelMatcher(label Item, operator Item, value Item) *labels.Matcher {
	return nil
}

func (p *parser) Lex(lval *yySymType) int {
	return 0
}

func (p *parser) Error(s string) {

}

func (p *parser) unexpected(context string, expected string) {

}
