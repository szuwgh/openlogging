package temql

type Parse struct {
	lexer *lex

	//yyParser yyParserImpl

	generatedParserResult interface{}
}
