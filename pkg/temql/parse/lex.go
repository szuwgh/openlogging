package parse

import (
	"strings"
	"unicode/utf8"
)

const eof = -1

type Pos int

type ItemType int

type stateFn func(*lexer) stateFn

// Item represents a token or text string returned from the scanner.
type Item struct {
	Typ ItemType // The type of this Item.
	Pos int      // The starting position, in bytes, of this Item in the input string.
	Val string   // The value of this Item.
}

////////////// 词法定义 //////////////
// 关键词定义
var key = map[string]ItemType{
	// Operators.
	"and": LAND,
	"or":  LOR,
}

// 符号定义
var ItemTypeStr = map[ItemType]string{
	LEFT_PAREN:  "(",
	RIGHT_PAREN: ")",
	LEFT_BRACE:  "{",
	RIGHT_BRACE: "}",
	ASSIGN:      "=",
	COMMA:       ",",
}

type lexer struct {
	input string
	pos   int
	len   int
	width int
	start int // Start position of this Item.
	itemp *Item
}

func newLex(input string) *lexer {
	return &lexer{
		input: input,
	}
}

func (l *lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	l.pos += l.width
	return r
}

//获取下一个item
func (l *lexer) nextItem(itemp *Item) {
	l.itemp = itemp
	//l.lexStatements()
}

func (l *lexer) backup() {
	l.pos -= l.width
}

//生成一个item
func (l *lexer) eject(t ItemType) {
	*l.itemp = Item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
}

func lexSpace(l *lexer) stateFn {
	for isSpace(l.peek()) {
		l.next()
	}
	l.ignore()
	return lexStatements
}

func lexInsideBraces(l *lexer) stateFn {
	switch r := l.next(); {
	case isSpace(r):
		skipSpaces(l)
	case isAlpha(r):
	}

}

func lexKeywordOrIdentifier(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case isAlphaNumeric(r):
		default:
			l.backup()
			word := l.input[l.start:l.pos]
			if kw, ok := key[strings.ToLower(word)]; ok {
				l.eject(kw)
			} else {
				l.eject(METRIC_IDENTIFIER)
			}
		}
	}
}

//lex
func lexStatements(l *lexer) stateFn {
	switch r := l.next(); {
	case r == eof:
	case r == ',':
	case r == '{':
		return lexInsideBraces(l)
	case r == '}':
	case r == '=':
	case isAlpha(r):
		return lexKeywordOrIdentifier(l)
	}
	return nil
}

// skipSpaces skips the spaces until a non-space is encountered.
// func skipSpaces(l *lexer) {
// 	for isSpace(l.peek()) {
// 		l.next()
// 	}
// 	//l.ignore()
// }

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

func isAlpha(r rune) bool {
	return r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z')
}

func isAlphaNumeric(r rune) bool {
	return isAlpha(r) || isDigit(r)
}

func isDigit(r rune) bool {
	return '0' <= r && r <= '9'
}
