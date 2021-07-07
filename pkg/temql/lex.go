package temql

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

var keyType = map[ItemType]string{
	// Operators.
	LAND: "and",
	LOR:  "or",
}

// 符号定义
var ItemTypeStr = map[ItemType]string{
	LEFT_PAREN:  "(",
	RIGHT_PAREN: ")",
	LEFT_BRACE:  "{",
	RIGHT_BRACE: "}",
	EQL:         "=",
	COMMA:       ",",
}

type lexer struct {
	input       string
	state       stateFn // The next lexing function to enter.
	pos         int
	len         int
	width       int
	start       int // Start position of this Item.
	itemp       *Item
	braceOpen   bool // Whether a { is opened.
	parenDepth  int  // Whether a ( is opened.
	stringOpen  rune // Quote rune of the string currently being read.
	scannedItem bool // Set to true every time an item is scanned.
}

func newLex(input string) *lexer {
	return &lexer{
		input: input,
		state: lexStatements,
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
	l.scannedItem = false
	if l.state != nil {
		for !l.scannedItem {
			l.state = l.state(l)
		}
	} else {
		l.eject(EOF)
	}
}

func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *lexer) ignore() {
	l.start = l.pos
}

func (l *lexer) backup() {
	l.pos -= l.width
}

//生成一个item
func (l *lexer) eject(t ItemType) {
	*l.itemp = Item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
	l.scannedItem = true
}

func lexSpace(l *lexer) stateFn {
	for isSpace(l.peek()) {
		l.next()
	}
	l.ignore()
	return lexStatements
}

func lexIdentifier(l *lexer) stateFn {
	for isAlphaNumeric(l.next()) {
		// absorb
	}
	l.backup()
	l.eject(IDENTIFIER)
	return lexStatements
}

func lexString(l *lexer) stateFn {
Loop:
	for {
		switch l.next() {
		case utf8.RuneError:
			return lexString
		case l.stringOpen:
			break Loop
		}
	}
	l.eject(STRING)
	return lexStatements
}

func lexInsideBraces(l *lexer) stateFn {
	switch r := l.next(); {
	case isSpace(r):
		return lexSpace(l)
	case isAlpha(r):
		return lexIdentifier
	case r == '=':
		l.eject(EQL)
	case r == '"' || r == '\'':
		l.stringOpen = r
		return lexString
	case r == ',':
		l.eject(COMMA)
	case r == '}':
		l.eject(RIGHT_BRACE)
		l.braceOpen = false
		return lexStatements
	default:
		return nil
	}
	return lexInsideBraces
}

func lexKeywordOrIdentifier(l *lexer) stateFn {
Loop:
	for {
		switch r := l.next(); {
		case isAlphaNumeric(r):
		default:
			l.backup()
			word := l.input[l.start:l.pos]
			if kw, ok := key[strings.ToLower(word)]; ok {
				l.eject(kw)
			} else {
				l.eject(IDENTIFIER)
			}
			break Loop
		}
	}
	return lexStatements
}

func lexInsideParen(l *lexer) stateFn {
	switch r := l.next(); {
	case isSpace(r):
		return lexSpace(l)
	case r == '(':
		l.eject(LEFT_PAREN)
		l.parenDepth++
		return lexInsideParen
	case isAlpha(r):
		return lexKeywordOrIdentifier
	case r == ')':
		l.eject(RIGHT_PAREN)
		l.parenDepth--
		//l.parenOpen = false
		return lexStatements
	default:
		return nil
	}
}

//lex
func lexStatements(l *lexer) stateFn {
	if l.braceOpen {
		return lexInsideBraces
	}
	if l.parenDepth > 0 {
		return lexInsideParen
	}
	switch r := l.next(); {
	case r == eof:
		l.eject(EOF)
		return nil
	case r == '(':
		l.eject(LEFT_PAREN)
		l.parenDepth++
		return lexInsideParen
	case r == ')':
		l.eject(RIGHT_PAREN)
		l.parenDepth--
		//l.parenOpen = false
	case r == ',':
	case r == '{':
		l.eject(LEFT_BRACE)
		l.braceOpen = true
		return lexInsideBraces
	case r == '}':
		l.eject(RIGHT_BRACE)
		l.braceOpen = false
	case r == '=':
	case isAlpha(r):
		return lexKeywordOrIdentifier
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
