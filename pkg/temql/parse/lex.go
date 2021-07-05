package parse

import "unicode/utf8"

const eof = -1

type Pos int

type ItemType int

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
}

type lex struct {
	input string
	pos   int
	len   int
	width int
	start int // Start position of this Item.
	itemp *Item
}

func newLex(input string) *lex {
	return &lex{
		input: input,
	}
}

func (l *lex) next() rune {
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
func (l *lex) nextItem(itemp *Item) {
	l.itemp = itemp
	l.lexStatements()

}

//生成一个item
func (l *lex) eject(t ItemType) {
	*l.itemp = Item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
}

//lex
func (l *lex) lexStatements() {
	switch r := l.next(); {
	case r == eof:
	case r == ',':
	case r == ':':
	case r == '{':
	case isAlpha(r):

	}

	return 0
}

func lexKeywordOrIdentifier() {

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
