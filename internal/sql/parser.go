package sql

import (
	"fmt"
	"minidb/pkg/types"
	"strconv"
)

// Statement represents a parsed SQL statement.
type Statement interface {
	statementNode()
}

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Columns   []string // Column names or "*"
	TableName string
	Where     Expr
}

func (s *SelectStmt) statementNode() {}

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	TableName string
	Columns   []string
	Values    []Expr
}

func (s *InsertStmt) statementNode() {}

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	TableName string
	Set       map[string]Expr
	Where     Expr
}

func (s *UpdateStmt) statementNode() {}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	TableName string
	Where     Expr
}

func (s *DeleteStmt) statementNode() {}

// BeginStmt represents a BEGIN statement.
type BeginStmt struct{}

func (s *BeginStmt) statementNode() {}

// CommitStmt represents a COMMIT statement.
type CommitStmt struct{}

func (s *CommitStmt) statementNode() {}

// RollbackStmt represents a ROLLBACK statement.
type RollbackStmt struct{}

func (s *RollbackStmt) statementNode() {}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (s *CreateTableStmt) statementNode() {}

// ColumnDef represents a column definition.
type ColumnDef struct {
	Name     string
	Type     types.ValueType
	Nullable bool
}

// Expr represents an expression.
type Expr interface {
	exprNode()
}

// LiteralExpr represents a literal value.
type LiteralExpr struct {
	Value types.Value
}

func (e *LiteralExpr) exprNode() {}

// ColumnExpr represents a column reference.
type ColumnExpr struct {
	Name string
}

func (e *ColumnExpr) exprNode() {}

// BinaryExpr represents a binary expression (e.g., a = 1).
type BinaryExpr struct {
	Left  Expr
	Op    TokenType
	Right Expr
}

func (e *BinaryExpr) exprNode() {}

// Parser parses SQL statements.
type Parser struct {
	lexer   *Lexer
	current Token
	peek    Token
	errors  []string
}

// NewParser creates a new parser.
func NewParser(input string) *Parser {
	p := &Parser{
		lexer: NewLexer(input),
	}
	// Load first two tokens
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.current = p.peek
	p.peek = p.lexer.NextToken()
}

func (p *Parser) expect(t TokenType) bool {
	if p.current.Type == t {
		p.nextToken()
		return true
	}
	p.errors = append(p.errors, fmt.Sprintf("expected %s, got %s", t, p.current.Type))
	return false
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peek.Type == t {
		p.nextToken()
		return true
	}
	p.errors = append(p.errors, fmt.Sprintf("expected %s, got %s", t, p.peek.Type))
	return false
}

// Parse parses the input and returns a statement.
func (p *Parser) Parse() (Statement, error) {
	var stmt Statement
	
	switch p.current.Type {
	case TokenSelect:
		stmt = p.parseSelect()
	case TokenInsert:
		stmt = p.parseInsert()
	case TokenUpdate:
		stmt = p.parseUpdate()
	case TokenDelete:
		stmt = p.parseDelete()
	case TokenBegin:
		stmt = &BeginStmt{}
		p.nextToken()
	case TokenCommit:
		stmt = &CommitStmt{}
		p.nextToken()
	case TokenRollback:
		stmt = &RollbackStmt{}
		p.nextToken()
	case TokenCreate:
		stmt = p.parseCreateTable()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.current.Type)
	}
	
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %v", p.errors)
	}
	
	return stmt, nil
}

func (p *Parser) parseSelect() *SelectStmt {
	stmt := &SelectStmt{}
	p.nextToken() // skip SELECT
	
	// Parse columns
	stmt.Columns = p.parseColumnList()
	
	// Expect FROM
	if !p.expect(TokenFrom) {
		return nil
	}
	
	// Parse table name
	if p.current.Type != TokenIdent {
		p.errors = append(p.errors, "expected table name")
		return nil
	}
	stmt.TableName = p.current.Literal
	p.nextToken()
	
	// Optional WHERE
	if p.current.Type == TokenWhere {
		p.nextToken()
		stmt.Where = p.parseExpr()
	}
	
	return stmt
}

func (p *Parser) parseInsert() *InsertStmt {
	stmt := &InsertStmt{}
	p.nextToken() // skip INSERT
	
	// Expect INTO
	if !p.expect(TokenInto) {
		return nil
	}
	
	// Parse table name
	if p.current.Type != TokenIdent {
		p.errors = append(p.errors, "expected table name")
		return nil
	}
	stmt.TableName = p.current.Literal
	p.nextToken()
	
	// Optional column list
	if p.current.Type == TokenLParen {
		p.nextToken()
		for p.current.Type == TokenIdent {
			stmt.Columns = append(stmt.Columns, p.current.Literal)
			p.nextToken()
			if p.current.Type == TokenComma {
				p.nextToken()
			}
		}
		if !p.expect(TokenRParen) {
			return nil
		}
	}
	
	// Expect VALUES
	if !p.expect(TokenValues) {
		return nil
	}
	
	// Parse values
	if !p.expect(TokenLParen) {
		return nil
	}
	
	for p.current.Type != TokenRParen && p.current.Type != TokenEOF {
		expr := p.parseExpr()
		if expr != nil {
			stmt.Values = append(stmt.Values, expr)
		}
		if p.current.Type == TokenComma {
			p.nextToken()
		}
	}
	
	p.expect(TokenRParen)
	
	return stmt
}

func (p *Parser) parseUpdate() *UpdateStmt {
	stmt := &UpdateStmt{
		Set: make(map[string]Expr),
	}
	p.nextToken() // skip UPDATE
	
	// Parse table name
	if p.current.Type != TokenIdent {
		p.errors = append(p.errors, "expected table name")
		return nil
	}
	stmt.TableName = p.current.Literal
	p.nextToken()
	
	// Expect SET
	if !p.expect(TokenSet) {
		return nil
	}
	
	// Parse assignments
	for {
		if p.current.Type != TokenIdent {
			break
		}
		column := p.current.Literal
		p.nextToken()
		
		if !p.expect(TokenEq) {
			return nil
		}
		
		value := p.parseExpr()
		stmt.Set[column] = value
		
		if p.current.Type != TokenComma {
			break
		}
		p.nextToken()
	}
	
	// Optional WHERE
	if p.current.Type == TokenWhere {
		p.nextToken()
		stmt.Where = p.parseExpr()
	}
	
	return stmt
}

func (p *Parser) parseDelete() *DeleteStmt {
	stmt := &DeleteStmt{}
	p.nextToken() // skip DELETE
	
	// Expect FROM
	if !p.expect(TokenFrom) {
		return nil
	}
	
	// Parse table name
	if p.current.Type != TokenIdent {
		p.errors = append(p.errors, "expected table name")
		return nil
	}
	stmt.TableName = p.current.Literal
	p.nextToken()
	
	// Optional WHERE
	if p.current.Type == TokenWhere {
		p.nextToken()
		stmt.Where = p.parseExpr()
	}
	
	return stmt
}

func (p *Parser) parseCreateTable() *CreateTableStmt {
	stmt := &CreateTableStmt{}
	p.nextToken() // skip CREATE
	
	// Expect TABLE
	if !p.expect(TokenTable) {
		return nil
	}
	
	// Parse table name
	if p.current.Type != TokenIdent {
		p.errors = append(p.errors, "expected table name")
		return nil
	}
	stmt.TableName = p.current.Literal
	p.nextToken()
	
	// Expect (
	if !p.expect(TokenLParen) {
		return nil
	}
	
	// Parse column definitions
	for p.current.Type != TokenRParen && p.current.Type != TokenEOF {
		colDef := p.parseColumnDef()
		if colDef != nil {
			stmt.Columns = append(stmt.Columns, *colDef)
		}
		
		if p.current.Type == TokenComma {
			p.nextToken()
		}
	}
	
	p.expect(TokenRParen)
	
	return stmt
}

func (p *Parser) parseColumnDef() *ColumnDef {
	if p.current.Type != TokenIdent {
		p.errors = append(p.errors, "expected column name")
		return nil
	}
	
	col := &ColumnDef{
		Name:     p.current.Literal,
		Nullable: true,
	}
	p.nextToken()
	
	// Parse type
	switch p.current.Type {
	case TokenInt:
		col.Type = types.ValueTypeInt
	case TokenText:
		col.Type = types.ValueTypeString
	case TokenBool:
		col.Type = types.ValueTypeBool
	default:
		p.errors = append(p.errors, fmt.Sprintf("expected type, got %s", p.current.Type))
		return nil
	}
	p.nextToken()
	
	// Optional NOT NULL
	if p.current.Type == TokenNot {
		p.nextToken()
		if p.current.Type == TokenNull {
			col.Nullable = false
			p.nextToken()
		}
	}
	
	return col
}

func (p *Parser) parseColumnList() []string {
	var columns []string
	
	if p.current.Type == TokenStar {
		columns = append(columns, "*")
		p.nextToken()
		return columns
	}
	
	for p.current.Type == TokenIdent {
		columns = append(columns, p.current.Literal)
		p.nextToken()
		
		if p.current.Type == TokenComma {
			p.nextToken()
		} else {
			break
		}
	}
	
	return columns
}

func (p *Parser) parseExpr() Expr {
	return p.parseOrExpr()
}

func (p *Parser) parseOrExpr() Expr {
	left := p.parseAndExpr()
	
	for p.current.Type == TokenOr {
		op := p.current.Type
		p.nextToken()
		right := p.parseAndExpr()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
	
	return left
}

func (p *Parser) parseAndExpr() Expr {
	left := p.parseCompareExpr()
	
	for p.current.Type == TokenAnd {
		op := p.current.Type
		p.nextToken()
		right := p.parseCompareExpr()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
	
	return left
}

func (p *Parser) parseCompareExpr() Expr {
	left := p.parsePrimaryExpr()
	
	switch p.current.Type {
	case TokenEq, TokenNe, TokenLt, TokenLe, TokenGt, TokenGe:
		op := p.current.Type
		p.nextToken()
		right := p.parsePrimaryExpr()
		return &BinaryExpr{Left: left, Op: op, Right: right}
	}
	
	return left
}

func (p *Parser) parsePrimaryExpr() Expr {
	switch p.current.Type {
	case TokenIdent:
		expr := &ColumnExpr{Name: p.current.Literal}
		p.nextToken()
		return expr
		
	case TokenNumber:
		val, _ := strconv.ParseInt(p.current.Literal, 10, 64)
		expr := &LiteralExpr{Value: types.Value{Type: types.ValueTypeInt, IntVal: val}}
		p.nextToken()
		return expr
		
	case TokenString:
		expr := &LiteralExpr{Value: types.Value{Type: types.ValueTypeString, StrVal: p.current.Literal}}
		p.nextToken()
		return expr
		
	case TokenTrue:
		expr := &LiteralExpr{Value: types.Value{Type: types.ValueTypeBool, BoolVal: true}}
		p.nextToken()
		return expr
		
	case TokenFalse:
		expr := &LiteralExpr{Value: types.Value{Type: types.ValueTypeBool, BoolVal: false}}
		p.nextToken()
		return expr
		
	case TokenNull:
		expr := &LiteralExpr{Value: types.Value{Type: types.ValueTypeNull, IsNull: true}}
		p.nextToken()
		return expr
		
	case TokenLParen:
		p.nextToken()
		expr := p.parseExpr()
		p.expect(TokenRParen)
		return expr
	}
	
	p.errors = append(p.errors, fmt.Sprintf("unexpected token in expression: %s", p.current.Type))
	return nil
}

// Errors returns parse errors.
func (p *Parser) Errors() []string {
	return p.errors
}
