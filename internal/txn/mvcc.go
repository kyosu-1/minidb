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

// MVCCStore provides multi-version storage for tuples.
type MVCCStore struct {
	// Table -> RowID -> list of versions (newest first)
	versions map[uint32]map[uint64][]*types.Tuple
	
	// Next row ID per table
	nextRowID map[uint32]uint64
}

// NewMVCCStore creates a new MVCC store.
func NewMVCCStore() *MVCCStore {
	return &MVCCStore{
		versions:  make(map[uint32]map[uint64][]*types.Tuple),
		nextRowID: make(map[uint32]uint64),
	}
}

// Insert adds a new tuple version.
func (store *MVCCStore) Insert(tableID uint32, txnID types.TxnID, cid types.CommandID, data []byte) (uint64, *types.Tuple) {
	if store.versions[tableID] == nil {
		store.versions[tableID] = make(map[uint64][]*types.Tuple)
	}
	
	store.nextRowID[tableID]++
	rowID := store.nextRowID[tableID]
	
	tuple := &types.Tuple{
		XMin:    txnID,
		XMax:    types.InvalidTxnID,
		Cid:     cid,
		TableID: tableID,
		RowID:   rowID,
		Data:    data,
	}
	
	store.versions[tableID][rowID] = []*types.Tuple{tuple}
	
	return rowID, tuple
}

// Update creates a new version of an existing tuple.
func (store *MVCCStore) Update(tableID uint32, rowID uint64, txnID types.TxnID, cid types.CommandID, newData []byte, snapshot *Snapshot) (*types.Tuple, *types.Tuple, error) {
	versions := store.versions[tableID][rowID]
	if len(versions) == 0 {
		return nil, nil, nil // Row not found
	}
	
	// Find visible version
	var oldVersion *types.Tuple
	for _, v := range versions {
		if snapshot.IsVisible(v) {
			oldVersion = v
			break
		}
	}
	
	if oldVersion == nil {
		return nil, nil, nil // No visible version
	}
	
	// Check for write-write conflict
	if oldVersion.XMax != types.InvalidTxnID && oldVersion.XMax != txnID {
		// Already being updated by another transaction
		return nil, nil, &WriteConflictError{
			TableID:     tableID,
			RowID:       rowID,
			ConflictTxn: oldVersion.XMax,
		}
	}
	
	// Mark old version as deleted by this transaction
	oldVersion.XMax = txnID
	
	// Create new version
	newVersion := &types.Tuple{
		XMin:    txnID,
		XMax:    types.InvalidTxnID,
		Cid:     cid,
		TableID: tableID,
		RowID:   rowID,
		Data:    newData,
	}
	
	// Add new version at the front
	store.versions[tableID][rowID] = append([]*types.Tuple{newVersion}, versions...)
	
	return oldVersion, newVersion, nil
}

// Delete marks a tuple as deleted.
func (store *MVCCStore) Delete(tableID uint32, rowID uint64, txnID types.TxnID, snapshot *Snapshot) (*types.Tuple, error) {
	versions := store.versions[tableID][rowID]
	if len(versions) == 0 {
		return nil, nil
	}
	
	// Find visible version
	for _, v := range versions {
		if snapshot.IsVisible(v) {
			// Check for write-write conflict
			if v.XMax != types.InvalidTxnID && v.XMax != txnID {
				return nil, &WriteConflictError{
					TableID:     tableID,
					RowID:       rowID,
					ConflictTxn: v.XMax,
				}
			}
			
			v.XMax = txnID
			return v, nil
		}
	}
	
	return nil, nil
}

// Read returns the visible version of a tuple.
func (store *MVCCStore) Read(tableID uint32, rowID uint64, snapshot *Snapshot) *types.Tuple {
	versions := store.versions[tableID][rowID]
	
	for _, v := range versions {
		if snapshot.IsVisible(v) {
			return v
		}
	}
	
	return nil
}

// Scan returns all visible tuples in a table.
func (store *MVCCStore) Scan(tableID uint32, snapshot *Snapshot) []*types.Tuple {
	var result []*types.Tuple
	
	rows := store.versions[tableID]
	for _, versions := range rows {
		for _, v := range versions {
			if snapshot.IsVisible(v) {
				result = append(result, v)
				break // Only one visible version per row
			}
		}
	}
	
	return result
}

// RollbackTransaction reverts all changes made by a transaction.
func (store *MVCCStore) RollbackTransaction(txnID types.TxnID) {
	for _, rows := range store.versions {
		for rowID, versions := range rows {
			// Remove versions created by this transaction
			filtered := versions[:0]
			for _, v := range versions {
				if v.XMin != txnID {
					// Restore XMax if this transaction deleted it
					if v.XMax == txnID {
						v.XMax = types.InvalidTxnID
					}
					filtered = append(filtered, v)
				}
			}
			rows[rowID] = filtered
		}
	}
}

// GarbageCollect removes versions that are no longer visible to any transaction.
func (store *MVCCStore) GarbageCollect(oldestActiveTxn types.TxnID) int {
	removed := 0
	
	for _, rows := range store.versions {
		for rowID, versions := range rows {
			if len(versions) <= 1 {
				continue
			}
			
			// Keep versions that might still be needed
			filtered := versions[:0]
			for i, v := range versions {
				// Keep if:
				// 1. It's the newest version
				// 2. It might be visible to some active transaction
				if i == 0 || v.XMax == types.InvalidTxnID || v.XMax >= oldestActiveTxn {
					filtered = append(filtered, v)
				} else {
					removed++
				}
			}
			rows[rowID] = filtered
		}
	}
	
	return removed
}

// SetNextRowID sets the next row ID for a table (used during recovery).
func (store *MVCCStore) SetNextRowID(tableID uint32, nextID uint64) {
	store.nextRowID[tableID] = nextID
}

// GetNextRowID returns the next row ID for a table.
func (store *MVCCStore) GetNextRowID(tableID uint32) uint64 {
	return store.nextRowID[tableID]
}

// RestoreTuple adds a tuple during recovery.
func (store *MVCCStore) RestoreTuple(tuple *types.Tuple) {
	if store.versions[tuple.TableID] == nil {
		store.versions[tuple.TableID] = make(map[uint64][]*types.Tuple)
	}
	
	versions := store.versions[tuple.TableID][tuple.RowID]
	store.versions[tuple.TableID][tuple.RowID] = append([]*types.Tuple{tuple}, versions...)
	
	// Update next row ID if necessary
	if tuple.RowID >= store.nextRowID[tuple.TableID] {
		store.nextRowID[tuple.TableID] = tuple.RowID + 1
	}
}

// WriteConflictError indicates a write-write conflict.
type WriteConflictError struct {
	TableID     uint32
	RowID       uint64
	ConflictTxn types.TxnID
}

func (e *WriteConflictError) Error() string {
	return "write-write conflict detected"
}
