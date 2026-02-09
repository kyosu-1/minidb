package sql

import (
	"minidb/pkg/types"
	"testing"
)

// --- Lexer tests ---

func TestLexerKeywords(t *testing.T) {
	input := "SELECT INSERT UPDATE DELETE FROM WHERE INTO VALUES SET AND OR NOT NULL BEGIN COMMIT ROLLBACK CREATE TABLE INT TEXT BOOL TRUE FALSE"
	tokens := Tokenize(input)

	expected := []TokenType{
		TokenSelect, TokenInsert, TokenUpdate, TokenDelete,
		TokenFrom, TokenWhere, TokenInto, TokenValues,
		TokenSet, TokenAnd, TokenOr, TokenNot, TokenNull,
		TokenBegin, TokenCommit, TokenRollback,
		TokenCreate, TokenTable, TokenInt, TokenText, TokenBool,
		TokenTrue, TokenFalse, TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("token count = %d, want %d", len(tokens), len(expected))
	}
	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token[%d].Type = %s, want %s", i, tok.Type, expected[i])
		}
	}
}

func TestLexerCaseInsensitive(t *testing.T) {
	tokens := Tokenize("select FROM where")
	if tokens[0].Type != TokenSelect {
		t.Errorf("'select' should be TokenSelect, got %s", tokens[0].Type)
	}
	if tokens[1].Type != TokenFrom {
		t.Errorf("'FROM' should be TokenFrom, got %s", tokens[1].Type)
	}
	if tokens[2].Type != TokenWhere {
		t.Errorf("'where' should be TokenWhere, got %s", tokens[2].Type)
	}
}

func TestLexerIdentifiers(t *testing.T) {
	tokens := Tokenize("my_table column1")
	if tokens[0].Type != TokenIdent || tokens[0].Literal != "my_table" {
		t.Errorf("token[0] = %v, want Ident 'my_table'", tokens[0])
	}
	if tokens[1].Type != TokenIdent || tokens[1].Literal != "column1" {
		t.Errorf("token[1] = %v, want Ident 'column1'", tokens[1])
	}
}

func TestLexerNumbers(t *testing.T) {
	tokens := Tokenize("42 -7 0")
	if tokens[0].Type != TokenNumber || tokens[0].Literal != "42" {
		t.Errorf("token[0] = %v, want Number '42'", tokens[0])
	}
	if tokens[1].Type != TokenNumber || tokens[1].Literal != "-7" {
		t.Errorf("token[1] = %v, want Number '-7'", tokens[1])
	}
	if tokens[2].Type != TokenNumber || tokens[2].Literal != "0" {
		t.Errorf("token[2] = %v, want Number '0'", tokens[2])
	}
}

func TestLexerStrings(t *testing.T) {
	tokens := Tokenize("'hello' 'world'")
	if tokens[0].Type != TokenString || tokens[0].Literal != "hello" {
		t.Errorf("token[0] = %v, want String 'hello'", tokens[0])
	}
	if tokens[1].Type != TokenString || tokens[1].Literal != "world" {
		t.Errorf("token[1] = %v, want String 'world'", tokens[1])
	}
}

func TestLexerOperators(t *testing.T) {
	tests := []struct {
		input string
		want  TokenType
	}{
		{"=", TokenEq},
		{"!=", TokenNe},
		{"<>", TokenNe},
		{"<", TokenLt},
		{"<=", TokenLe},
		{">", TokenGt},
		{">=", TokenGe},
	}
	for _, tt := range tests {
		tokens := Tokenize(tt.input)
		if tokens[0].Type != tt.want {
			t.Errorf("Tokenize(%q)[0].Type = %s, want %s", tt.input, tokens[0].Type, tt.want)
		}
	}
}

func TestLexerPunctuation(t *testing.T) {
	tokens := Tokenize(", ( ) * ;")
	expected := []TokenType{TokenComma, TokenLParen, TokenRParen, TokenStar, TokenSemicolon, TokenEOF}
	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token[%d].Type = %s, want %s", i, tok.Type, expected[i])
		}
	}
}

func TestLexerBangError(t *testing.T) {
	tokens := Tokenize("!")
	if tokens[0].Type != TokenError {
		t.Errorf("'!' should be TokenError, got %s", tokens[0].Type)
	}
}

// --- Parser tests ---

func TestParseSelectStar(t *testing.T) {
	p := NewParser("SELECT * FROM users")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if len(sel.Columns) != 1 || sel.Columns[0] != "*" {
		t.Errorf("Columns = %v, want [*]", sel.Columns)
	}
	if sel.TableName != "users" {
		t.Errorf("TableName = %q, want %q", sel.TableName, "users")
	}
	if sel.Where != nil {
		t.Error("Where should be nil")
	}
}

func TestParseSelectColumns(t *testing.T) {
	p := NewParser("SELECT id, name FROM users")
	stmt, _ := p.Parse()

	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 2 {
		t.Errorf("Columns = %v, want 2 columns", sel.Columns)
	}
	if sel.Columns[0] != "id" || sel.Columns[1] != "name" {
		t.Errorf("Columns = %v, want [id, name]", sel.Columns)
	}
}

func TestParseSelectWhere(t *testing.T) {
	p := NewParser("SELECT * FROM users WHERE id = 1")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	sel := stmt.(*SelectStmt)
	if sel.Where == nil {
		t.Fatal("Where should not be nil")
	}

	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Where should be *BinaryExpr, got %T", sel.Where)
	}
	if bin.Op != TokenEq {
		t.Errorf("Op = %s, want =", bin.Op)
	}
}

func TestParseSelectWhereAnd(t *testing.T) {
	p := NewParser("SELECT * FROM users WHERE id = 1 AND name = 'alice'")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	sel := stmt.(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Where should be *BinaryExpr, got %T", sel.Where)
	}
	if bin.Op != TokenAnd {
		t.Errorf("Op = %s, want AND", bin.Op)
	}
}

func TestParseSelectWhereOr(t *testing.T) {
	p := NewParser("SELECT * FROM users WHERE id = 1 OR id = 2")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	sel := stmt.(*SelectStmt)
	bin, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Where should be *BinaryExpr, got %T", sel.Where)
	}
	if bin.Op != TokenOr {
		t.Errorf("Op = %s, want OR", bin.Op)
	}
}

func TestParseInsertWithColumns(t *testing.T) {
	p := NewParser("INSERT INTO users (id, name) VALUES (1, 'alice')")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}
	if ins.TableName != "users" {
		t.Errorf("TableName = %q, want %q", ins.TableName, "users")
	}
	if len(ins.Columns) != 2 {
		t.Errorf("Columns count = %d, want 2", len(ins.Columns))
	}
	if len(ins.Values) != 2 {
		t.Errorf("Values count = %d, want 2", len(ins.Values))
	}
}

func TestParseInsertWithoutColumns(t *testing.T) {
	p := NewParser("INSERT INTO users VALUES (1, 'alice')")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	ins := stmt.(*InsertStmt)
	if len(ins.Columns) != 0 {
		t.Errorf("Columns = %v, want empty", ins.Columns)
	}
	if len(ins.Values) != 2 {
		t.Errorf("Values count = %d, want 2", len(ins.Values))
	}
}

func TestParseUpdate(t *testing.T) {
	p := NewParser("UPDATE users SET name = 'bob' WHERE id = 1")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	upd, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}
	if upd.TableName != "users" {
		t.Errorf("TableName = %q, want %q", upd.TableName, "users")
	}
	if len(upd.Set) != 1 {
		t.Errorf("Set count = %d, want 1", len(upd.Set))
	}
	if _, ok := upd.Set["name"]; !ok {
		t.Error("Set should contain 'name'")
	}
	if upd.Where == nil {
		t.Error("Where should not be nil")
	}
}

func TestParseDelete(t *testing.T) {
	p := NewParser("DELETE FROM users WHERE id = 1")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected *DeleteStmt, got %T", stmt)
	}
	if del.TableName != "users" {
		t.Errorf("TableName = %q, want %q", del.TableName, "users")
	}
	if del.Where == nil {
		t.Error("Where should not be nil")
	}
}

func TestParseDeleteWithoutWhere(t *testing.T) {
	p := NewParser("DELETE FROM users")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	del := stmt.(*DeleteStmt)
	if del.Where != nil {
		t.Error("Where should be nil")
	}
}

func TestParseBeginCommitRollback(t *testing.T) {
	tests := []struct {
		sql  string
		want interface{}
	}{
		{"BEGIN", &BeginStmt{}},
		{"COMMIT", &CommitStmt{}},
		{"ROLLBACK", &RollbackStmt{}},
	}

	for _, tt := range tests {
		p := NewParser(tt.sql)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse(%q) error = %v", tt.sql, err)
		}
		switch tt.want.(type) {
		case *BeginStmt:
			if _, ok := stmt.(*BeginStmt); !ok {
				t.Errorf("Parse(%q) = %T, want *BeginStmt", tt.sql, stmt)
			}
		case *CommitStmt:
			if _, ok := stmt.(*CommitStmt); !ok {
				t.Errorf("Parse(%q) = %T, want *CommitStmt", tt.sql, stmt)
			}
		case *RollbackStmt:
			if _, ok := stmt.(*RollbackStmt); !ok {
				t.Errorf("Parse(%q) = %T, want *RollbackStmt", tt.sql, stmt)
			}
		}
	}
}

func TestParseCreateTable(t *testing.T) {
	p := NewParser("CREATE TABLE users (id INT NOT NULL, name TEXT, active BOOL)")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}
	if ct.TableName != "users" {
		t.Errorf("TableName = %q, want %q", ct.TableName, "users")
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("Columns count = %d, want 3", len(ct.Columns))
	}

	// Column 0: id INT NOT NULL
	if ct.Columns[0].Name != "id" || ct.Columns[0].Type != types.ValueTypeInt || ct.Columns[0].Nullable {
		t.Errorf("Column[0] = %+v", ct.Columns[0])
	}
	// Column 1: name TEXT (nullable by default)
	if ct.Columns[1].Name != "name" || ct.Columns[1].Type != types.ValueTypeString || !ct.Columns[1].Nullable {
		t.Errorf("Column[1] = %+v", ct.Columns[1])
	}
	// Column 2: active BOOL
	if ct.Columns[2].Name != "active" || ct.Columns[2].Type != types.ValueTypeBool {
		t.Errorf("Column[2] = %+v", ct.Columns[2])
	}
}

func TestParseComparisonOperators(t *testing.T) {
	ops := []struct {
		sql string
		op  TokenType
	}{
		{"SELECT * FROM t WHERE x = 1", TokenEq},
		{"SELECT * FROM t WHERE x != 1", TokenNe},
		{"SELECT * FROM t WHERE x < 1", TokenLt},
		{"SELECT * FROM t WHERE x <= 1", TokenLe},
		{"SELECT * FROM t WHERE x > 1", TokenGt},
		{"SELECT * FROM t WHERE x >= 1", TokenGe},
	}

	for _, tt := range ops {
		p := NewParser(tt.sql)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse(%q) error = %v", tt.sql, err)
		}
		sel := stmt.(*SelectStmt)
		bin := sel.Where.(*BinaryExpr)
		if bin.Op != tt.op {
			t.Errorf("Parse(%q) Op = %s, want %s", tt.sql, bin.Op, tt.op)
		}
	}
}

func TestParseInvalidSQL(t *testing.T) {
	tests := []string{
		"INVALID STATEMENT",
		"",
	}

	for _, sql := range tests {
		p := NewParser(sql)
		_, err := p.Parse()
		if err == nil {
			t.Errorf("Parse(%q) should error", sql)
		}
	}
}

func TestParseInsertValueTypes(t *testing.T) {
	p := NewParser("INSERT INTO t VALUES (42, 'hello', TRUE, FALSE, NULL)")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	ins := stmt.(*InsertStmt)
	if len(ins.Values) != 5 {
		t.Fatalf("Values count = %d, want 5", len(ins.Values))
	}

	// Check integer
	lit0 := ins.Values[0].(*LiteralExpr)
	if lit0.Value.Type != types.ValueTypeInt || lit0.Value.IntVal != 42 {
		t.Errorf("Values[0] = %v, want Int 42", lit0.Value)
	}

	// Check string
	lit1 := ins.Values[1].(*LiteralExpr)
	if lit1.Value.Type != types.ValueTypeString || lit1.Value.StrVal != "hello" {
		t.Errorf("Values[1] = %v, want String 'hello'", lit1.Value)
	}

	// Check true
	lit2 := ins.Values[2].(*LiteralExpr)
	if lit2.Value.Type != types.ValueTypeBool || !lit2.Value.BoolVal {
		t.Errorf("Values[2] = %v, want Bool true", lit2.Value)
	}

	// Check false
	lit3 := ins.Values[3].(*LiteralExpr)
	if lit3.Value.Type != types.ValueTypeBool || lit3.Value.BoolVal {
		t.Errorf("Values[3] = %v, want Bool false", lit3.Value)
	}

	// Check null
	lit4 := ins.Values[4].(*LiteralExpr)
	if !lit4.Value.IsNull {
		t.Errorf("Values[4] = %v, want NULL", lit4.Value)
	}
}

func TestParseUpdateMultipleSet(t *testing.T) {
	p := NewParser("UPDATE users SET name = 'bob', age = 30")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	upd := stmt.(*UpdateStmt)
	if len(upd.Set) != 2 {
		t.Errorf("Set count = %d, want 2", len(upd.Set))
	}
}

func TestTokenTypeString(t *testing.T) {
	s := TokenSelect.String()
	if s != "SELECT" {
		t.Errorf("TokenSelect.String() = %q, want %q", s, "SELECT")
	}

	s = TokenType(999).String()
	if s == "" {
		t.Error("unknown TokenType.String() should not be empty")
	}
}

func TestTokenString(t *testing.T) {
	tok := Token{Type: TokenSelect, Literal: "SELECT"}
	s := tok.String()
	if s == "" {
		t.Error("Token.String() should not be empty")
	}
}

func TestParserErrors(t *testing.T) {
	p := NewParser("SELECT")
	_, err := p.Parse()
	if err == nil {
		t.Fatal("incomplete SELECT should error")
	}
}
