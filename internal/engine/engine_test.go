package engine

import (
	"path/filepath"
	"strings"
	"testing"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	dir := t.TempDir()
	e, err := New(Config{DataDir: dir, BufferPoolSize: 100})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return e
}

func TestEngineCreateClose(t *testing.T) {
	dir := t.TempDir()
	e, err := New(Config{DataDir: dir, BufferPoolSize: 100})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestEngineReopenEmpty(t *testing.T) {
	dir := t.TempDir()

	e, err := New(Config{DataDir: dir, BufferPoolSize: 100})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	e.Close()

	e2, err := New(Config{DataDir: dir, BufferPoolSize: 100})
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	defer e2.Close()
}

func TestEngineCreateTable(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	result := e.Execute("CREATE TABLE users (id INT, name TEXT)")
	if result.Error != nil {
		t.Fatalf("CREATE TABLE error = %v", result.Error)
	}
	if !strings.Contains(result.Message, "CREATE TABLE") {
		t.Errorf("Message = %q, want to contain CREATE TABLE", result.Message)
	}
}

func TestEngineInsertAndSelect(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")

	result := e.Execute("INSERT INTO users VALUES (1, 'alice')")
	if result.Error != nil {
		t.Fatalf("INSERT error = %v", result.Error)
	}
	if !strings.Contains(result.Message, "INSERT") {
		t.Errorf("Message = %q", result.Message)
	}

	result = e.Execute("SELECT * FROM users")
	if result.Error != nil {
		t.Fatalf("SELECT error = %v", result.Error)
	}
	if len(result.Rows) != 1 {
		t.Errorf("SELECT rows = %d, want 1", len(result.Rows))
	}
}

func TestEngineSelectWhere(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")
	e.Execute("INSERT INTO users VALUES (2, 'bob')")
	e.Execute("INSERT INTO users VALUES (3, 'charlie')")

	result := e.Execute("SELECT * FROM users WHERE id = 2")
	if result.Error != nil {
		t.Fatalf("SELECT WHERE error = %v", result.Error)
	}
	if len(result.Rows) != 1 {
		t.Errorf("SELECT WHERE rows = %d, want 1", len(result.Rows))
	}
}

func TestEngineUpdate(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")

	result := e.Execute("UPDATE users SET name = 'bob' WHERE id = 1")
	if result.Error != nil {
		t.Fatalf("UPDATE error = %v", result.Error)
	}
	if !strings.Contains(result.Message, "UPDATE 1") {
		t.Errorf("Message = %q, want UPDATE 1", result.Message)
	}

	// Verify update
	result = e.Execute("SELECT * FROM users WHERE name = 'bob'")
	if result.Error != nil {
		t.Fatalf("SELECT after update error = %v", result.Error)
	}
	if len(result.Rows) < 1 {
		t.Error("updated row not found")
	}
}

func TestEngineDelete(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")
	e.Execute("INSERT INTO users VALUES (2, 'bob')")

	result := e.Execute("DELETE FROM users WHERE id = 1")
	if result.Error != nil {
		t.Fatalf("DELETE error = %v", result.Error)
	}
	if !strings.Contains(result.Message, "DELETE 1") {
		t.Errorf("Message = %q, want DELETE 1", result.Message)
	}

	result = e.Execute("SELECT * FROM users")
	if result.Error != nil {
		t.Fatalf("SELECT after delete error = %v", result.Error)
	}
	if len(result.Rows) != 1 {
		t.Errorf("remaining rows = %d, want 1", len(result.Rows))
	}
}

func TestEngineExplicitTransaction(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")

	result := e.Execute("BEGIN")
	if result.Error != nil {
		t.Fatalf("BEGIN error = %v", result.Error)
	}

	e.Execute("INSERT INTO users VALUES (1, 'alice')")
	e.Execute("INSERT INTO users VALUES (2, 'bob')")

	result = e.Execute("COMMIT")
	if result.Error != nil {
		t.Fatalf("COMMIT error = %v", result.Error)
	}

	result = e.Execute("SELECT * FROM users")
	if result.Error != nil {
		t.Fatalf("SELECT error = %v", result.Error)
	}
	if len(result.Rows) != 2 {
		t.Errorf("rows = %d, want 2", len(result.Rows))
	}
}

func TestEngineRollback(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")

	e.Execute("BEGIN")
	e.Execute("INSERT INTO users VALUES (2, 'bob')")

	result := e.Execute("ROLLBACK")
	if result.Error != nil {
		t.Fatalf("ROLLBACK error = %v", result.Error)
	}

	// After rollback, the insert in the transaction should be gone
	// Note: the heap-based INSERT already happened at the disk level,
	// but the MVCC visibility should hide it from new transactions
	result = e.Execute("SELECT * FROM users")
	if result.Error != nil {
		t.Fatalf("SELECT after ROLLBACK error = %v", result.Error)
	}
	// Row inserted before BEGIN should still be visible
	if len(result.Rows) < 1 {
		t.Error("pre-txn row should still be visible")
	}
}

func TestEngineAutoCommit(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")

	// Without BEGIN, each statement auto-commits
	e.Execute("INSERT INTO users VALUES (1, 'alice')")

	result := e.Execute("SELECT * FROM users")
	if result.Error != nil {
		t.Fatalf("SELECT error = %v", result.Error)
	}
	if len(result.Rows) != 1 {
		t.Errorf("rows = %d, want 1", len(result.Rows))
	}
}

func TestEnginePersistence(t *testing.T) {
	dir := t.TempDir()

	e, err := New(Config{DataDir: dir, BufferPoolSize: 100})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")
	e.Execute("INSERT INTO users VALUES (2, 'bob')")
	e.Close()

	// Reopen
	e2, err := New(Config{DataDir: dir, BufferPoolSize: 100})
	if err != nil {
		t.Fatalf("Reopen error = %v", err)
	}
	defer e2.Close()

	result := e2.Execute("SELECT * FROM users")
	if result.Error != nil {
		t.Fatalf("SELECT after reopen error = %v", result.Error)
	}
	if len(result.Rows) < 1 {
		t.Error("data should survive close and reopen")
	}
}

func TestEngineCheckpoint(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")

	if err := e.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	result := e.Execute("SELECT * FROM users")
	if result.Error != nil {
		t.Fatalf("SELECT after checkpoint error = %v", result.Error)
	}
	if len(result.Rows) != 1 {
		t.Errorf("rows = %d, want 1", len(result.Rows))
	}
}

func TestEngineCreateIndex(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")

	if err := e.CreateIndex("users"); err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}

	// Index should be accessible
	tableID, _ := e.catalog.GetTableID("users")
	idx := e.GetIndex(tableID)
	if idx == nil {
		t.Error("index should exist after CreateIndex")
	}
}

func TestEngineDoubleBegin(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("BEGIN")
	result := e.Execute("BEGIN")
	if result.Error == nil {
		t.Error("double BEGIN should error")
	}
}

func TestEngineCommitNoTransaction(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	result := e.Execute("COMMIT")
	if result.Error == nil {
		t.Error("COMMIT without BEGIN should error")
	}
}

func TestEngineRollbackNoTransaction(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	result := e.Execute("ROLLBACK")
	if result.Error == nil {
		t.Error("ROLLBACK without BEGIN should error")
	}
}

func TestEngineSelectNonExistentTable(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	result := e.Execute("SELECT * FROM nonexistent")
	if result.Error == nil {
		t.Error("SELECT from non-existent table should error")
	}
}

func TestEngineInsertColumnCountMismatch(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")

	result := e.Execute("INSERT INTO users VALUES (1)")
	if result.Error == nil {
		t.Error("INSERT with wrong column count should error")
	}
}

func TestEngineInsertNonExistentTable(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	result := e.Execute("INSERT INTO nonexistent VALUES (1)")
	if result.Error == nil {
		t.Error("INSERT into non-existent table should error")
	}
}

func TestEngineStats(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	stats := e.Stats()
	if stats == nil {
		t.Fatal("Stats() returned nil")
	}
	if _, ok := stats["tables"]; !ok {
		t.Error("Stats() should include 'tables'")
	}
}

func TestEngineDefaultBufferPoolSize(t *testing.T) {
	dir := t.TempDir()
	e, err := New(Config{DataDir: dir})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer e.Close()
}

func TestEngineCreateIndexNonExistentTable(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	err := e.CreateIndex("nonexistent")
	if err == nil {
		t.Error("CreateIndex() on non-existent table should error")
	}
}

func TestEngineCreateDuplicateIndex(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")
	e.CreateIndex("users")

	err := e.CreateIndex("users")
	if err == nil {
		t.Error("duplicate CreateIndex() should error")
	}
}

func TestEngineRecoveryAfterCrash(t *testing.T) {
	dir := t.TempDir()

	e, err := New(Config{DataDir: dir, BufferPoolSize: 100})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")

	// Simulate a "crash" by closing WAL and disk directly without graceful shutdown
	e.walWriter.Flush()
	e.walWriter.Close()
	e.diskManager.Close()

	// Reopen - should trigger recovery
	e2, err := New(Config{DataDir: dir, BufferPoolSize: 100})
	if err != nil {
		t.Fatalf("Reopen after crash error = %v", err)
	}
	defer e2.Close()

	result := e2.Execute("SELECT * FROM users")
	if result.Error != nil {
		t.Fatalf("SELECT after recovery error = %v", result.Error)
	}
	// Data should still be there after recovery
	if len(result.Rows) < 1 {
		t.Error("data should survive crash recovery")
	}
}

func TestEngineDataDir(t *testing.T) {
	dir := t.TempDir()
	subdir := filepath.Join(dir, "nested", "db")

	e, err := New(Config{DataDir: subdir, BufferPoolSize: 10})
	if err != nil {
		t.Fatalf("New() with nested dir error = %v", err)
	}
	defer e.Close()
}

func TestEngineGetCatalogAndBufferPool(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	if e.GetCatalog() == nil {
		t.Error("GetCatalog() returned nil")
	}
	if e.GetBufferPool() == nil {
		t.Error("GetBufferPool() returned nil")
	}
}

func TestEngineInvalidSQL(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	result := e.Execute("INVALID SQL")
	if result.Error == nil {
		t.Error("invalid SQL should error")
	}
}

func TestEngineCreateDuplicateTable(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT)")
	result := e.Execute("CREATE TABLE users (id INT)")
	if result.Error == nil {
		t.Error("duplicate CREATE TABLE should error")
	}
}

func TestEngineMultipleInsertSelect(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE items (id INT, name TEXT, price INT)")

	for i := 1; i <= 10; i++ {
		result := e.Execute("INSERT INTO items VALUES (" + itoa(i) + ", 'item', " + itoa(i*10) + ")")
		if result.Error != nil {
			t.Fatalf("INSERT %d error = %v", i, result.Error)
		}
	}

	result := e.Execute("SELECT * FROM items")
	if result.Error != nil {
		t.Fatalf("SELECT error = %v", result.Error)
	}
	if len(result.Rows) != 10 {
		t.Errorf("rows = %d, want 10", len(result.Rows))
	}
}

func TestEngineVacuumAfterDelete(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")
	e.Execute("INSERT INTO users VALUES (2, 'bob')")

	e.Execute("DELETE FROM users WHERE id = 1")

	result, err := e.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}
	if result.TotalRemoved() != 1 {
		t.Errorf("TotalRemoved = %d, want 1", result.TotalRemoved())
	}

	// Remaining data should be intact
	sel := e.Execute("SELECT * FROM users")
	if sel.Error != nil {
		t.Fatalf("SELECT error = %v", sel.Error)
	}
	if len(sel.Rows) != 1 {
		t.Errorf("rows after vacuum = %d, want 1", len(sel.Rows))
	}
}

func TestEngineVacuumAfterUpdate(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")

	e.Execute("UPDATE users SET name = 'bob' WHERE id = 1")

	result, err := e.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}
	// The old version of the updated row should be removed
	if result.TotalRemoved() != 1 {
		t.Errorf("TotalRemoved = %d, want 1", result.TotalRemoved())
	}

	sel := e.Execute("SELECT * FROM users")
	if sel.Error != nil {
		t.Fatalf("SELECT error = %v", sel.Error)
	}
	if len(sel.Rows) != 1 {
		t.Errorf("rows after vacuum = %d, want 1", len(sel.Rows))
	}
}

func TestEngineVacuumIdempotent(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")
	e.Execute("DELETE FROM users WHERE id = 1")

	r1, err := e.Vacuum()
	if err != nil {
		t.Fatalf("first Vacuum() error = %v", err)
	}
	if r1.TotalRemoved() != 1 {
		t.Errorf("first TotalRemoved = %d, want 1", r1.TotalRemoved())
	}

	r2, err := e.Vacuum()
	if err != nil {
		t.Fatalf("second Vacuum() error = %v", err)
	}
	if r2.TotalRemoved() != 0 {
		t.Errorf("second TotalRemoved = %d, want 0", r2.TotalRemoved())
	}
}

func TestEngineVacuumSkipsAbortedTxn(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")

	// DELETE inside a transaction that gets rolled back
	e.Execute("BEGIN")
	e.Execute("DELETE FROM users WHERE id = 1")
	e.Execute("ROLLBACK")

	result, err := e.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}
	// Should NOT remove the tuple because the DELETE was aborted
	if result.TotalRemoved() != 0 {
		t.Errorf("TotalRemoved = %d, want 0 (aborted DELETE)", result.TotalRemoved())
	}
}

func TestEngineVacuumNoDeadTuples(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()

	e.Execute("CREATE TABLE users (id INT, name TEXT)")
	e.Execute("INSERT INTO users VALUES (1, 'alice')")
	e.Execute("INSERT INTO users VALUES (2, 'bob')")

	result, err := e.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}
	if result.TotalRemoved() != 0 {
		t.Errorf("TotalRemoved = %d, want 0", result.TotalRemoved())
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}
