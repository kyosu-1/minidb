package wal

import (
	"minidb/pkg/types"
	"path/filepath"
	"testing"
)

func setupRecoveryTest(t *testing.T) (string, *Writer) {
	t.Helper()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	w, err := NewWriter(walPath)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	return walPath, w
}

func TestAnalysisBeginAndCommit(t *testing.T) {
	walPath, w := setupRecoveryTest(t)

	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("data"))
	w.LogCommit(types.TxnID(1))
	w.Close()

	// Reopen for recovery
	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)
	rm.SetCallbacks(func(r *LogRecord) error { return nil }, func(r *LogRecord) error { return nil })
	rm.SetPageLSNCallback(func(types.PageID) types.LSN { return types.InvalidLSN })

	rm.analysisPhase()

	att := rm.GetActiveTxnTable()
	if len(att) != 0 {
		t.Errorf("ATT size = %d, want 0 (committed txn should be removed)", len(att))
	}
}

func TestAnalysisBeginOnly(t *testing.T) {
	walPath, w := setupRecoveryTest(t)

	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("data"))
	// No commit
	w.Flush()
	w.Close()

	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)
	rm.analysisPhase()

	att := rm.GetActiveTxnTable()
	if len(att) != 1 {
		t.Errorf("ATT size = %d, want 1", len(att))
	}
	if _, ok := att[types.TxnID(1)]; !ok {
		t.Error("TxnID 1 should be in ATT")
	}
}

func TestAnalysisDirtyPageTable(t *testing.T) {
	walPath, w := setupRecoveryTest(t)

	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(5), 0, []byte("data"))
	w.LogUpdate(types.TxnID(1), 1, 1, types.PageID(7), 1, []byte("old"), []byte("new"))
	w.Flush()
	w.Close()

	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)
	rm.analysisPhase()

	dpt := rm.GetDirtyPageTable()
	if len(dpt) != 2 {
		t.Errorf("DPT size = %d, want 2", len(dpt))
	}
	if _, ok := dpt[types.PageID(5)]; !ok {
		t.Error("PageID 5 should be in DPT")
	}
	if _, ok := dpt[types.PageID(7)]; !ok {
		t.Error("PageID 7 should be in DPT")
	}
}

func TestAnalysisFromCheckpoint(t *testing.T) {
	walPath, w := setupRecoveryTest(t)

	// Pre-checkpoint activity
	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("data"))
	w.LogCommit(types.TxnID(1))

	// Checkpoint with txn 2 active
	w.LogBegin(types.TxnID(2))
	w.LogCheckpoint(
		[]types.TxnID{types.TxnID(2)},
		map[types.PageID]types.LSN{types.PageID(0): types.LSN(2)},
	)

	// Post-checkpoint activity
	w.LogInsert(types.TxnID(2), 1, 2, types.PageID(1), 0, []byte("data2"))
	w.Flush()
	w.Close()

	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)
	rm.analysisPhase()

	att := rm.GetActiveTxnTable()
	if _, ok := att[types.TxnID(2)]; !ok {
		t.Error("TxnID 2 should be in ATT from checkpoint")
	}

	dpt := rm.GetDirtyPageTable()
	if _, ok := dpt[types.PageID(0)]; !ok {
		t.Error("PageID 0 should be in DPT from checkpoint")
	}
	if _, ok := dpt[types.PageID(1)]; !ok {
		t.Error("PageID 1 should be in DPT from post-checkpoint insert")
	}
}

func TestRedoPhase(t *testing.T) {
	walPath, w := setupRecoveryTest(t)

	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("data"))
	w.LogCommit(types.TxnID(1))
	w.Close()

	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)

	var redoRecords []*LogRecord
	rm.SetCallbacks(
		func(r *LogRecord) error {
			redoRecords = append(redoRecords, r)
			return nil
		},
		func(r *LogRecord) error { return nil },
	)
	rm.SetPageLSNCallback(func(types.PageID) types.LSN { return types.InvalidLSN })

	rm.analysisPhase()
	rm.redoPhase()

	if len(redoRecords) == 0 {
		t.Error("redo callback was not called")
	}
}

func TestRedoSkipsAlreadyApplied(t *testing.T) {
	walPath, w := setupRecoveryTest(t)

	w.LogBegin(types.TxnID(1))
	insertLSN := w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("data"))
	w.LogCommit(types.TxnID(1))
	w.Close()

	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)

	redoCount := 0
	rm.SetCallbacks(
		func(r *LogRecord) error {
			redoCount++
			return nil
		},
		func(r *LogRecord) error { return nil },
	)
	// Page LSN >= record LSN means already applied
	rm.SetPageLSNCallback(func(types.PageID) types.LSN { return insertLSN })

	rm.analysisPhase()
	rm.redoPhase()

	if redoCount != 0 {
		t.Errorf("redo should skip already-applied records, got %d calls", redoCount)
	}
}

func TestUndoPhase(t *testing.T) {
	walPath, w := setupRecoveryTest(t)

	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("data"))
	// No commit - should be undone
	w.Flush()
	w.Close()

	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)

	var undoRecords []*LogRecord
	rm.SetCallbacks(
		func(r *LogRecord) error { return nil },
		func(r *LogRecord) error {
			undoRecords = append(undoRecords, r)
			return nil
		},
	)
	rm.SetPageLSNCallback(func(types.PageID) types.LSN { return types.InvalidLSN })

	rm.analysisPhase()
	rm.redoPhase()
	rm.undoPhase()

	if len(undoRecords) == 0 {
		t.Error("undo callback was not called for uncommitted txn")
	}
}

func TestFullRecoveryMixedTransactions(t *testing.T) {
	walPath, w := setupRecoveryTest(t)

	// Committed transaction
	w.LogBegin(types.TxnID(1))
	w.LogInsert(types.TxnID(1), 1, 1, types.PageID(0), 0, []byte("committed"))
	w.LogCommit(types.TxnID(1))

	// Uncommitted transaction
	w.LogBegin(types.TxnID(2))
	w.LogInsert(types.TxnID(2), 1, 2, types.PageID(1), 0, []byte("uncommitted"))
	w.Flush()
	w.Close()

	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)

	var redoRecords, undoRecords []*LogRecord
	rm.SetCallbacks(
		func(r *LogRecord) error {
			redoRecords = append(redoRecords, r)
			return nil
		},
		func(r *LogRecord) error {
			undoRecords = append(undoRecords, r)
			return nil
		},
	)
	rm.SetPageLSNCallback(func(types.PageID) types.LSN { return types.InvalidLSN })

	if err := rm.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}

	// Both inserts should be redone
	if len(redoRecords) < 1 {
		t.Error("expected redo records")
	}

	// Only uncommitted should be undone
	if len(undoRecords) != 1 {
		t.Errorf("undoRecords = %d, want 1", len(undoRecords))
	}
	if len(undoRecords) > 0 && undoRecords[0].TxnID != types.TxnID(2) {
		t.Errorf("undone TxnID = %d, want 2", undoRecords[0].TxnID)
	}
}

func TestRecoveryEmptyWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	w, _ := NewWriter(walPath)
	w.Close()

	w2, _ := NewWriter(walPath)
	defer w2.Close()

	rm := NewRecoveryManager(walPath, w2)
	rm.SetCallbacks(
		func(r *LogRecord) error { return nil },
		func(r *LogRecord) error { return nil },
	)
	rm.SetPageLSNCallback(func(types.PageID) types.LSN { return types.InvalidLSN })

	if err := rm.Recover(); err != nil {
		t.Fatalf("Recover() on empty WAL error = %v", err)
	}
}

func TestRecoveryNoWALFile(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "nonexistent.wal")

	rm := NewRecoveryManager(walPath, nil)

	// Analysis should handle non-existent WAL gracefully
	lsn, err := rm.analysisPhase()
	if err != nil {
		t.Fatalf("analysisPhase() error = %v", err)
	}
	if lsn != 0 {
		t.Errorf("checkpoint LSN = %d, want 0", lsn)
	}
}
