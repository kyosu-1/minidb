// Package sql implements a simple SQL parser and executor.
package sql

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType represents the type of a lexical token.
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenError
	
	// Keywords
	TokenSelect
	TokenInsert
	TokenUpdate
	TokenDelete
	TokenFrom
	TokenWhere
	TokenInto
	TokenValues
	TokenSet
	TokenAnd
	TokenOr
	TokenNot
	TokenNull
	TokenBegin
	TokenCommit
	TokenRollback
	TokenCreate
	TokenTable
	TokenInt
	TokenText
	TokenBool
	
	// Literals
	TokenIdent
	TokenNumber
	TokenString
	TokenTrue
	TokenFalse
	
	// Operators
	TokenEq        // =
	TokenNe        // != or <>
	TokenLt        // <
	TokenLe        // <=
	TokenGt        // >
	TokenGe        // >=
	
	// Punctuation
	TokenComma     // ,
	TokenLParen    // (
	TokenRParen    // )
	TokenStar      // *
	TokenSemicolon // ;
)

var tokenNames = map[TokenType]string{
	TokenEOF:       "EOF",
	TokenError:     "ERROR",
	TokenSelect:    "SELECT",
	TokenInsert:    "INSERT",
	TokenUpdate:    "UPDATE",
	TokenDelete:    "DELETE",
	TokenFrom:      "FROM",
	TokenWhere:     "WHERE",
	TokenInto:      "INTO",
	TokenValues:    "VALUES",
	TokenSet:       "SET",
	TokenAnd:       "AND",
	TokenOr:        "OR",
	TokenNot:       "NOT",
	TokenNull:      "NULL",
	TokenBegin:     "BEGIN",
	TokenCommit:    "COMMIT",
	TokenRollback:  "ROLLBACK",
	TokenCreate:    "CREATE",
	TokenTable:     "TABLE",
	TokenInt:       "INT",
	TokenText:      "TEXT",
	TokenBool:      "BOOL",
	TokenIdent:     "IDENT",
	TokenNumber:    "NUMBER",
	TokenString:    "STRING",
	TokenTrue:      "TRUE",
	TokenFalse:     "FALSE",
	TokenEq:        "=",
	TokenNe:        "!=",
	TokenLt:        "<",
	TokenLe:        "<=",
	TokenGt:        ">",
	TokenGe:        ">=",
	TokenComma:     ",",
	TokenLParen:    "(",
	TokenRParen:    ")",
	TokenStar:      "*",
	TokenSemicolon: ";",
}

func (t TokenType) String() string {
	if name, ok := tokenNames[t]; ok {
		return name
	}
	return fmt.Sprintf("Token(%d)", t)
}

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}

func (t Token) String() string {
	return fmt.Sprintf("{%s %q}", t.Type, t.Literal)
}

// Keywords maps keyword strings to token types.
var keywords = map[string]TokenType{
	"SELECT":   TokenSelect,
	"INSERT":   TokenInsert,
	"UPDATE":   TokenUpdate,
	"DELETE":   TokenDelete,
	"FROM":     TokenFrom,
	"WHERE":    TokenWhere,
	"INTO":     TokenInto,
	"VALUES":   TokenValues,
	"SET":      TokenSet,
	"AND":      TokenAnd,
	"OR":       TokenOr,
	"NOT":      TokenNot,
	"NULL":     TokenNull,
	"BEGIN":    TokenBegin,
	"COMMIT":   TokenCommit,
	"ROLLBACK": TokenRollback,
	"CREATE":   TokenCreate,
	"TABLE":    TokenTable,
	"INT":      TokenInt,
	"TEXT":     TokenText,
	"BOOL":     TokenBool,
	"TRUE":     TokenTrue,
	"FALSE":    TokenFalse,
}

// Lexer tokenizes SQL input.
type Lexer struct {
	input string
	pos   int
	ch    byte
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.advance()
	return l
}

func (l *Lexer) advance() {
	if l.pos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.pos]
	}
	l.pos++
}

func (l *Lexer) peek() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *Lexer) skipWhitespace() {
	for l.ch != 0 && unicode.IsSpace(rune(l.ch)) {
		l.advance()
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	
	startPos := l.pos - 1
	
	if l.ch == 0 {
		return Token{Type: TokenEOF, Pos: startPos}
	}
	
	// Single character tokens
	switch l.ch {
	case ',':
		l.advance()
		return Token{Type: TokenComma, Literal: ",", Pos: startPos}
	case '(':
		l.advance()
		return Token{Type: TokenLParen, Literal: "(", Pos: startPos}
	case ')':
		l.advance()
		return Token{Type: TokenRParen, Literal: ")", Pos: startPos}
	case '*':
		l.advance()
		return Token{Type: TokenStar, Literal: "*", Pos: startPos}
	case ';':
		l.advance()
		return Token{Type: TokenSemicolon, Literal: ";", Pos: startPos}
	case '=':
		l.advance()
		return Token{Type: TokenEq, Literal: "=", Pos: startPos}
	case '<':
		l.advance()
		if l.ch == '=' {
			l.advance()
			return Token{Type: TokenLe, Literal: "<=", Pos: startPos}
		}
		if l.ch == '>' {
			l.advance()
			return Token{Type: TokenNe, Literal: "<>", Pos: startPos}
		}
		return Token{Type: TokenLt, Literal: "<", Pos: startPos}
	case '>':
		l.advance()
		if l.ch == '=' {
			l.advance()
			return Token{Type: TokenGe, Literal: ">=", Pos: startPos}
		}
		return Token{Type: TokenGt, Literal: ">", Pos: startPos}
	case '!':
		l.advance()
		if l.ch == '=' {
			l.advance()
			return Token{Type: TokenNe, Literal: "!=", Pos: startPos}
		}
		return Token{Type: TokenError, Literal: "!", Pos: startPos}
	case '\'':
		return l.readString()
	}
	
	// Numbers
	if unicode.IsDigit(rune(l.ch)) || (l.ch == '-' && unicode.IsDigit(rune(l.peek()))) {
		return l.readNumber()
	}
	
	// Identifiers and keywords
	if unicode.IsLetter(rune(l.ch)) || l.ch == '_' {
		return l.readIdentifier()
	}
	
	ch := l.ch
	l.advance()
	return Token{Type: TokenError, Literal: string(ch), Pos: startPos}
}

func (l *Lexer) readString() Token {
	startPos := l.pos - 1
	l.advance() // skip opening quote
	
	start := l.pos - 1
	for l.ch != 0 && l.ch != '\'' {
		l.advance()
	}
	
	literal := l.input[start : l.pos-1]
	
	if l.ch == '\'' {
		l.advance() // skip closing quote
	}
	
	return Token{Type: TokenString, Literal: literal, Pos: startPos}
}

func (l *Lexer) readNumber() Token {
	startPos := l.pos - 1
	start := l.pos - 1
	
	if l.ch == '-' {
		l.advance()
	}
	
	for unicode.IsDigit(rune(l.ch)) {
		l.advance()
	}
	
	return Token{Type: TokenNumber, Literal: l.input[start : l.pos-1], Pos: startPos}
}

func (l *Lexer) readIdentifier() Token {
	startPos := l.pos - 1
	start := l.pos - 1
	
	for unicode.IsLetter(rune(l.ch)) || unicode.IsDigit(rune(l.ch)) || l.ch == '_' {
		l.advance()
	}
	
	literal := l.input[start : l.pos-1]
	upper := strings.ToUpper(literal)
	
	if tokenType, ok := keywords[upper]; ok {
		return Token{Type: tokenType, Literal: upper, Pos: startPos}
	}
	
	return Token{Type: TokenIdent, Literal: literal, Pos: startPos}
}

// Tokenize returns all tokens from the input.
func Tokenize(input string) []Token {
	lexer := NewLexer(input)
	var tokens []Token
	
	for {
		token := lexer.NextToken()
		tokens = append(tokens, token)
		if token.Type == TokenEOF || token.Type == TokenError {
			break
		}
	}
	
	return tokens
}
