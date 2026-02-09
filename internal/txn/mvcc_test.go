package txn

import (
	"minidb/pkg/types"
	"testing"
)

func TestSnapshotIsVisibleCommittedInsert(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: make(map[types.TxnID]bool),
	}

	// Committed insert (XMin < Xmin, not deleted)
	tuple := &types.Tuple{XMin: types.TxnID(3), XMax: types.InvalidTxnID}
	if !snap.IsVisible(tuple) {
		t.Error("committed insert should be visible")
	}
}

func TestSnapshotIsVisibleUncommittedInsert(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: map[types.TxnID]bool{types.TxnID(7): true},
	}

	// Insert by active transaction
	tuple := &types.Tuple{XMin: types.TxnID(7), XMax: types.InvalidTxnID}
	if snap.IsVisible(tuple) {
		t.Error("insert by active txn should not be visible")
	}
}

func TestSnapshotIsVisibleFutureInsert(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: make(map[types.TxnID]bool),
	}

	// Insert by future transaction (>= Xmax)
	tuple := &types.Tuple{XMin: types.TxnID(15), XMax: types.InvalidTxnID}
	if snap.IsVisible(tuple) {
		t.Error("future insert should not be visible")
	}
}

func TestSnapshotIsVisibleCommittedDelete(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: make(map[types.TxnID]bool),
	}

	// Committed insert + committed delete
	tuple := &types.Tuple{XMin: types.TxnID(3), XMax: types.TxnID(4)}
	if snap.IsVisible(tuple) {
		t.Error("tuple with visible delete should not be visible")
	}
}

func TestSnapshotIsVisibleUncommittedDelete(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: map[types.TxnID]bool{types.TxnID(7): true},
	}

	// Committed insert + delete by active txn
	tuple := &types.Tuple{XMin: types.TxnID(3), XMax: types.TxnID(7)}
	if !snap.IsVisible(tuple) {
		t.Error("tuple with invisible delete should still be visible")
	}
}

func TestSnapshotIsVisibleFutureDelete(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: make(map[types.TxnID]bool),
	}

	// Committed insert + future delete
	tuple := &types.Tuple{XMin: types.TxnID(3), XMax: types.TxnID(15)}
	if !snap.IsVisible(tuple) {
		t.Error("tuple with future delete should still be visible")
	}
}

func TestSnapshotIsVisibleInvalidXMin(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: make(map[types.TxnID]bool),
	}

	tuple := &types.Tuple{XMin: types.InvalidTxnID, XMax: types.InvalidTxnID}
	if snap.IsVisible(tuple) {
		t.Error("tuple with InvalidTxnID XMin should not be visible")
	}
}

func TestIsVisibleForUpdateNoConflict(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: make(map[types.TxnID]bool),
	}

	tuple := &types.Tuple{XMin: types.TxnID(3), XMax: types.InvalidTxnID}
	visible, conflict := snap.IsVisibleForUpdate(tuple, types.TxnID(8))
	if !visible {
		t.Error("should be visible for update")
	}
	if conflict != types.InvalidTxnID {
		t.Errorf("conflict = %d, want InvalidTxnID", conflict)
	}
}

func TestIsVisibleForUpdateWriteConflict(t *testing.T) {
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: map[types.TxnID]bool{types.TxnID(7): true},
	}

	// Tuple being deleted by active txn 7
	tuple := &types.Tuple{XMin: types.TxnID(3), XMax: types.TxnID(7)}
	visible, conflict := snap.IsVisibleForUpdate(tuple, types.TxnID(8))
	if visible {
		t.Error("should detect write conflict")
	}
	if conflict != types.TxnID(7) {
		t.Errorf("conflict = %d, want 7", conflict)
	}
}

func TestIsVisibleForUpdateSameTransaction(t *testing.T) {
	// Snapshot where txn 8 is active (our own transaction)
	snap := &Snapshot{
		Xmin:       types.TxnID(5),
		Xmax:       types.TxnID(10),
		ActiveTxns: map[types.TxnID]bool{types.TxnID(8): true},
	}

	// Tuple created by committed txn 3, deleted by our own txn 8
	// XMax=8 is our txn, so IsVisible sees XMax as active -> delete not visible -> tuple visible
	tuple := &types.Tuple{XMin: types.TxnID(3), XMax: types.TxnID(8)}
	visible, conflict := snap.IsVisibleForUpdate(tuple, types.TxnID(8))
	if !visible {
		t.Error("should be visible for update by same txn")
	}
	if conflict != types.InvalidTxnID {
		t.Errorf("conflict = %d, want InvalidTxnID", conflict)
	}
}

