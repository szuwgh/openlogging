package parse

type parser struct {
	lexer *lex

	//yyParser yyParserImpl

	generatedParserResult interface{}
}

func (p *parser) Lex(lval *yySymType) int {
	return 0
}

func (p *parser) Error(s string) {

}
