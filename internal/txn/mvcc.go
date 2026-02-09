package txn

import (
	"minidb/pkg/types"
)

// Snapshot represents a point-in-time view of the database.
type Snapshot struct {
	// All transactions with ID < Xmin are committed
	Xmin types.TxnID
	
	// All transactions with ID >= Xmax are not visible
	Xmax types.TxnID
	
	// Transactions that were active when snapshot was taken
	ActiveTxns map[types.TxnID]bool
}

// IsVisible determines if a tuple version is visible to this snapshot.
// This is the core of MVCC visibility logic.
func (s *Snapshot) IsVisible(tuple *types.Tuple) bool {
	// Rule 1: Check if the creating transaction is visible
	if !s.isTxnVisible(tuple.XMin) {
		return false
	}
	
	// Rule 2: Check if tuple has been deleted
	if tuple.XMax == types.InvalidTxnID {
		// Not deleted, visible
		return true
	}
	
	// Rule 3: Check if the deleting transaction is visible
	if s.isTxnVisible(tuple.XMax) {
		// Deletion is visible, so tuple is not visible
		return false
	}
	
	// Deletion is not yet visible, tuple is still visible
	return true
}

// isTxnVisible checks if a transaction's effects are visible.
func (s *Snapshot) isTxnVisible(txnID types.TxnID) bool {
	// Transaction ID is 0 (invalid) - not visible
	if txnID == types.InvalidTxnID {
		return false
	}
	
	// Transaction started after our snapshot
	if txnID >= s.Xmax {
		return false
	}
	
	// Transaction was active when snapshot was taken
	if s.ActiveTxns[txnID] {
		return false
	}
	
	// Transaction committed before our snapshot
	return true
}

// IsVisibleForUpdate checks visibility for UPDATE/DELETE operations.
// More restrictive - needs to check for conflicts.
func (s *Snapshot) IsVisibleForUpdate(tuple *types.Tuple, myTxnID types.TxnID) (visible bool, conflict types.TxnID) {
	// First check basic visibility
	if !s.IsVisible(tuple) {
		return false, types.InvalidTxnID
	}
	
	// Check if another active transaction has modified this tuple
	if tuple.XMax != types.InvalidTxnID && tuple.XMax != myTxnID {
		// Someone else has deleted/updated this tuple
		if s.ActiveTxns[tuple.XMax] || tuple.XMax >= s.Xmax {
			// The deleting transaction is still active or started after us
			// This is a write-write conflict
			return false, tuple.XMax
		}
	}
	
	return true, types.InvalidTxnID
}

