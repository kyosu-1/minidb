package txn

import (
	"minidb/internal/wal"
	"minidb/pkg/types"
	"path/filepath"
	"testing"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	w, err := wal.NewWriter(walPath)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return NewManager(w)
}

func TestBegin(t *testing.T) {
	m := newTestManager(t)

	txn := m.Begin()
	if txn == nil {
		t.Fatal("Begin() returned nil")
	}
	if txn.ID == types.InvalidTxnID {
		t.Error("txn ID should not be invalid")
	}
	if txn.Status != types.TxnStatusRunning {
		t.Errorf("Status = %v, want Running", txn.Status)
	}
	if txn.Snapshot == nil {
		t.Error("Snapshot should not be nil")
	}
}

func TestBeginMultiple(t *testing.T) {
	m := newTestManager(t)

	txn1 := m.Begin()
	txn2 := m.Begin()

	if txn1.ID == txn2.ID {
		t.Error("two transactions should have different IDs")
	}
	if txn2.ID <= txn1.ID {
		t.Error("second txn ID should be greater than first")
	}

	// txn2's snapshot should see txn1 as active
	if !txn2.Snapshot.ActiveTxns[txn1.ID] {
		t.Error("txn2's snapshot should contain txn1 as active")
	}
}

func TestCommit(t *testing.T) {
	m := newTestManager(t)

	txn := m.Begin()
	txnID := txn.ID

	if err := m.Commit(txn); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if txn.Status != types.TxnStatusCommitted {
		t.Errorf("Status = %v, want Committed", txn.Status)
	}

	// Should be removed from active txns
	activeTxns := m.GetActiveTxns()
	for _, id := range activeTxns {
		if id == txnID {
			t.Error("committed txn should not be in active list")
		}
	}
}

func TestCommitNonRunning(t *testing.T) {
	m := newTestManager(t)

	txn := m.Begin()
	m.Commit(txn)

	// Committing again should fail
	err := m.Commit(txn)
	if err == nil {
		t.Fatal("expected error committing non-running txn")
	}
}

func TestRollback(t *testing.T) {
	m := newTestManager(t)

	txn := m.Begin()
	txnID := txn.ID

	if err := m.Rollback(txn); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if txn.Status != types.TxnStatusAborted {
		t.Errorf("Status = %v, want Aborted", txn.Status)
	}

	// Should be removed from active txns
	activeTxns := m.GetActiveTxns()
	for _, id := range activeTxns {
		if id == txnID {
			t.Error("rolled back txn should not be in active list")
		}
	}
}

func TestRollbackNonRunning(t *testing.T) {
	m := newTestManager(t)

	txn := m.Begin()
	m.Rollback(txn)

	err := m.Rollback(txn)
	if err == nil {
		t.Fatal("expected error rolling back non-running txn")
	}
}

func TestGetActiveTxns(t *testing.T) {
	m := newTestManager(t)

	txn1 := m.Begin()
	txn2 := m.Begin()

	active := m.GetActiveTxns()
	if len(active) != 2 {
		t.Errorf("active txns = %d, want 2", len(active))
	}

	m.Commit(txn1)
	active = m.GetActiveTxns()
	if len(active) != 1 {
		t.Errorf("after commit, active txns = %d, want 1", len(active))
	}

	m.Rollback(txn2)
	active = m.GetActiveTxns()
	if len(active) != 0 {
		t.Errorf("after rollback, active txns = %d, want 0", len(active))
	}
}

func TestGetTransaction(t *testing.T) {
	m := newTestManager(t)

	txn := m.Begin()
	got := m.GetTransaction(txn.ID)
	if got == nil {
		t.Fatal("GetTransaction() returned nil")
	}
	if got.ID != txn.ID {
		t.Errorf("GetTransaction() ID = %d, want %d", got.ID, txn.ID)
	}

	// After commit, should not be found
	m.Commit(txn)
	got = m.GetTransaction(txn.ID)
	if got != nil {
		t.Error("GetTransaction() should return nil after commit")
	}
}

func TestGlobalXmin(t *testing.T) {
	m := newTestManager(t)

	// No active txns - globalXmin should be MaxTxnID
	if m.GetGlobalXmin() != types.MaxTxnID {
		t.Errorf("initial GlobalXmin = %d, want MaxTxnID", m.GetGlobalXmin())
	}

	txn1 := m.Begin()
	xmin1 := m.GetGlobalXmin()
	if xmin1 != txn1.ID {
		t.Errorf("GlobalXmin = %d, want %d", xmin1, txn1.ID)
	}

	txn2 := m.Begin()
	xmin2 := m.GetGlobalXmin()
	if xmin2 != txn1.ID {
		t.Errorf("GlobalXmin should still be %d (oldest), got %d", txn1.ID, xmin2)
	}

	m.Commit(txn1)
	xmin3 := m.GetGlobalXmin()
	if xmin3 != txn2.ID {
		t.Errorf("GlobalXmin should be %d after committing first, got %d", txn2.ID, xmin3)
	}
}

func TestSnapshotActiveTxns(t *testing.T) {
	m := newTestManager(t)

	txn1 := m.Begin()
	txn2 := m.Begin()

	// txn2's snapshot should contain txn1 as active
	if !txn2.Snapshot.ActiveTxns[txn1.ID] {
		t.Error("snapshot should contain earlier active txn")
	}

	// txn1's snapshot should NOT contain txn2 (started later)
	if txn1.Snapshot.ActiveTxns[txn2.ID] {
		t.Error("snapshot should not contain later txn")
	}
}

func TestNextCommandID(t *testing.T) {
	m := newTestManager(t)
	txn := m.Begin()

	cid1 := txn.NextCommandID()
	cid2 := txn.NextCommandID()

	if cid1 != 1 {
		t.Errorf("first CommandID = %d, want 1", cid1)
	}
	if cid2 != 2 {
		t.Errorf("second CommandID = %d, want 2", cid2)
	}
}

func TestSetNextTxnID(t *testing.T) {
	m := newTestManager(t)

	m.SetNextTxnID(types.TxnID(100))
	txn := m.Begin()

	if txn.ID < types.TxnID(100) {
		t.Errorf("txn ID = %d, want >= 100", txn.ID)
	}
}

func TestIsTxnCommitted(t *testing.T) {
	m := newTestManager(t)

	txn1 := m.Begin()
	txn2 := m.Begin()
	txn1ID := txn1.ID
	txn2ID := txn2.ID

	// Neither committed yet
	if m.IsTxnCommitted(txn1ID) {
		t.Error("txn1 should not be committed yet")
	}

	m.Commit(txn1)
	if !m.IsTxnCommitted(txn1ID) {
		t.Error("txn1 should be committed")
	}

	// Rolled back txn should NOT be marked committed
	m.Rollback(txn2)
	if m.IsTxnCommitted(txn2ID) {
		t.Error("rolled back txn2 should not be committed")
	}
}

func TestPruneCommittedBefore(t *testing.T) {
	m := newTestManager(t)

	txn1 := m.Begin()
	txn2 := m.Begin()
	txn3 := m.Begin()
	m.Commit(txn1)
	m.Commit(txn2)
	m.Commit(txn3)

	if !m.IsTxnCommitted(txn1.ID) || !m.IsTxnCommitted(txn2.ID) || !m.IsTxnCommitted(txn3.ID) {
		t.Fatal("all three should be committed")
	}

	// Prune committed txns before txn3's ID
	m.PruneCommittedBefore(txn3.ID)

	if m.IsTxnCommitted(txn1.ID) {
		t.Error("txn1 should be pruned")
	}
	if m.IsTxnCommitted(txn2.ID) {
		t.Error("txn2 should be pruned")
	}
	if !m.IsTxnCommitted(txn3.ID) {
		t.Error("txn3 should NOT be pruned (not < cutoff)")
	}
}

func TestManagerWithNilWALWriter(t *testing.T) {
	m := NewManager(nil)

	txn := m.Begin()
	if txn == nil {
		t.Fatal("Begin() returned nil with nil WAL writer")
	}
	if err := m.Commit(txn); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}
