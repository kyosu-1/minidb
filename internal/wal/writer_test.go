package wal

import (
	"minidb/pkg/types"
	"os"
	"path/filepath"
	"testing"
)

func newTestWriter(t *testing.T) (*Writer, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	return w, path
}

func TestNewWriterInitialState(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	if w.GetCurrentLSN() != 1 {
		t.Errorf("CurrentLSN = %d, want 1", w.GetCurrentLSN())
	}
	if w.GetFlushedLSN() != 0 {
		t.Errorf("FlushedLSN = %d, want 0", w.GetFlushedLSN())
	}
}

func TestAppendAssignsLSN(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	lsn1 := w.Append(&LogRecord{TxnID: 1, Type: types.LogRecordBegin})
	if lsn1 != 1 {
		t.Errorf("first LSN = %d, want 1", lsn1)
	}

	lsn2 := w.Append(&LogRecord{TxnID: 1, Type: types.LogRecordInsert})
	if lsn2 != 2 {
		t.Errorf("second LSN = %d, want 2", lsn2)
	}

	if w.GetCurrentLSN() != 3 {
		t.Errorf("CurrentLSN = %d, want 3", w.GetCurrentLSN())
	}
}

func TestAppendPrevLSNChain(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	r1 := &LogRecord{TxnID: 1, Type: types.LogRecordBegin}
	w.Append(r1)
	if r1.PrevLSN != types.InvalidLSN {
		t.Errorf("first PrevLSN = %d, want InvalidLSN", r1.PrevLSN)
	}

	r2 := &LogRecord{TxnID: 1, Type: types.LogRecordInsert}
	w.Append(r2)
	if r2.PrevLSN != 1 {
		t.Errorf("second PrevLSN = %d, want 1", r2.PrevLSN)
	}

	r3 := &LogRecord{TxnID: 1, Type: types.LogRecordUpdate}
	w.Append(r3)
	if r3.PrevLSN != 2 {
		t.Errorf("third PrevLSN = %d, want 2", r3.PrevLSN)
	}
}

func TestFlush(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	w.Append(&LogRecord{TxnID: 1, Type: types.LogRecordBegin})
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if w.GetFlushedLSN() != 1 {
		t.Errorf("FlushedLSN after flush = %d, want 1", w.GetFlushedLSN())
	}
}

func TestForce(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	lsn := w.Append(&LogRecord{TxnID: 1, Type: types.LogRecordBegin})
	if err := w.Force(lsn); err != nil {
		t.Fatalf("Force() error = %v", err)
	}
	if w.GetFlushedLSN() < lsn {
		t.Errorf("FlushedLSN = %d, want >= %d", w.GetFlushedLSN(), lsn)
	}

	// Force with already-flushed LSN should be a no-op
	if err := w.Force(lsn); err != nil {
		t.Fatalf("Force(already flushed) error = %v", err)
	}
}

func TestLogBegin(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	lsn := w.LogBegin(types.TxnID(1))
	if lsn == 0 {
		t.Error("LogBegin() returned 0")
	}
}

func TestLogCommitForcesToDisk(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	w.LogBegin(types.TxnID(1))
	lsn, err := w.LogCommit(types.TxnID(1))
	if err != nil {
		t.Fatalf("LogCommit() error = %v", err)
	}
	if w.GetFlushedLSN() < lsn {
		t.Errorf("commit not forced: FlushedLSN = %d, commitLSN = %d", w.GetFlushedLSN(), lsn)
	}
}

func TestLogAbort(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	w.LogBegin(types.TxnID(1))
	lsn := w.LogAbort(types.TxnID(1))
	if lsn == 0 {
		t.Error("LogAbort() returned 0")
	}
}

func TestLogInsert(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	lsn := w.LogInsert(types.TxnID(1), 1, 100, types.PageID(0), 0, []byte("data"))
	if lsn == 0 {
		t.Error("LogInsert() returned 0")
	}
}

func TestLogUpdate(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	lsn := w.LogUpdate(types.TxnID(1), 1, 100, types.PageID(0), 0, []byte("old"), []byte("new"))
	if lsn == 0 {
		t.Error("LogUpdate() returned 0")
	}
}

func TestLogDelete(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	lsn := w.LogDelete(types.TxnID(1), 1, 100, types.PageID(0), 0, []byte("data"))
	if lsn == 0 {
		t.Error("LogDelete() returned 0")
	}
}

func TestLogCheckpoint(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	activeTxns := []types.TxnID{1, 2}
	dirtyPages := map[types.PageID]types.LSN{0: 1}

	lsn, err := w.LogCheckpoint(activeTxns, dirtyPages)
	if err != nil {
		t.Fatalf("LogCheckpoint() error = %v", err)
	}
	if lsn == 0 {
		t.Error("LogCheckpoint() returned 0")
	}
	if w.GetFlushedLSN() < lsn {
		t.Errorf("checkpoint not forced: FlushedLSN = %d, lsn = %d", w.GetFlushedLSN(), lsn)
	}
}

func TestLogCLR(t *testing.T) {
	w, _ := newTestWriter(t)
	defer w.Close()

	lsn := w.LogCLR(types.TxnID(1), 1, 100, types.PageID(0), 0, types.LSN(5), []byte("undo"))
	if lsn == 0 {
		t.Error("LogCLR() returned 0")
	}
}

func TestCloseReopenContinuesLSN(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("data"))
	w.LogCommit(types.TxnID(1))
	lastLSN := w.GetCurrentLSN()
	w.Close()

	// Reopen
	w2, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Reopen NewWriter() error = %v", err)
	}
	defer w2.Close()

	if w2.GetCurrentLSN() != lastLSN {
		t.Errorf("CurrentLSN after reopen = %d, want %d", w2.GetCurrentLSN(), lastLSN)
	}
}

func TestCloseReopenReconstructsTxnLastLSN(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, _ := NewWriter(path)
	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("data"))
	// Don't commit - leave txn active
	w.Flush()
	w.Close()

	// Reopen
	w2, _ := NewWriter(path)
	defer w2.Close()

	lastLSN := w2.GetTxnLastLSN(types.TxnID(1))
	if lastLSN == 0 {
		t.Error("txnLastLSN not reconstructed for active txn")
	}
}

func TestGetMaxTxnID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, _ := NewWriter(path)
	w.LogBegin(types.TxnID(5))
	w.LogBegin(types.TxnID(10))
	w.LogCommit(types.TxnID(10))
	w.Close()

	w2, _ := NewWriter(path)
	defer w2.Close()

	maxID := w2.GetMaxTxnID()
	if maxID < types.TxnID(10) {
		t.Errorf("MaxTxnID = %d, want >= 10", maxID)
	}
}

func TestInvalidWALMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.wal")

	// Write invalid WAL file
	os.WriteFile(path, make([]byte, walFileHeader), 0644)

	_, err := NewWriter(path)
	if err == nil {
		t.Fatal("expected error for invalid WAL magic")
	}
}
