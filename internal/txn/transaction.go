// Package txn implements transaction management.
package txn

import (
	"fmt"
	"minidb/internal/wal"
	"minidb/pkg/types"
	"sync"
	"sync/atomic"
)

// Manager handles transaction lifecycle and coordination.
type Manager struct {
	mu sync.RWMutex
	
	// Transaction ID generator (atomic for thread safety)
	nextTxnID uint64
	
	// Active transactions
	activeTxns map[types.TxnID]*Transaction
	
	// WAL writer
	walWriter *wal.Writer
	
	// Global snapshot for visibility
	globalXmin types.TxnID // Oldest active transaction
}

// Transaction represents an active transaction.
type Transaction struct {
	ID        types.TxnID
	Status    types.TxnStatus
	StartTS   types.TxnID       // Start timestamp for snapshot
	Snapshot  *Snapshot         // Visibility snapshot
	CommandID types.CommandID   // Current command within transaction
	
	// Undo information
	LastLSN   types.LSN
	
	// Locks held (simplified - in real DB would be more complex)
	HeldLocks map[string]LockMode
	
	mu sync.Mutex
}

// LockMode represents the type of lock.
type LockMode int

const (
	LockShared LockMode = iota
	LockExclusive
)

// NewManager creates a new transaction manager.
func NewManager(walWriter *wal.Writer) *Manager {
	return &Manager{
		nextTxnID:  1,
		activeTxns: make(map[types.TxnID]*Transaction),
		walWriter:  walWriter,
		globalXmin: types.MaxTxnID,
	}
}

// Begin starts a new transaction.
func (m *Manager) Begin() *Transaction {
	txnID := types.TxnID(atomic.AddUint64(&m.nextTxnID, 1))
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Create snapshot of currently active transactions
	snapshot := m.createSnapshotLocked()
	
	txn := &Transaction{
		ID:        txnID,
		Status:    types.TxnStatusRunning,
		StartTS:   txnID,
		Snapshot:  snapshot,
		CommandID: 0,
		HeldLocks: make(map[string]LockMode),
	}
	
	m.activeTxns[txnID] = txn
	m.updateGlobalXmin()
	
	// Log BEGIN
	if m.walWriter != nil {
		txn.LastLSN = m.walWriter.LogBegin(txnID)
	}
	
	return txn
}

// Commit commits a transaction.
func (m *Manager) Commit(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	
	if txn.Status != types.TxnStatusRunning {
		return fmt.Errorf("transaction %d is not running (status: %s)", txn.ID, txn.Status)
	}
	
	// Log COMMIT and force to disk
	if m.walWriter != nil {
		lsn, err := m.walWriter.LogCommit(txn.ID)
		if err != nil {
			return fmt.Errorf("failed to log commit: %w", err)
		}
		txn.LastLSN = lsn
	}
	
	txn.Status = types.TxnStatusCommitted
	
	// Release locks
	txn.HeldLocks = nil
	
	// Remove from active transactions
	m.mu.Lock()
	delete(m.activeTxns, txn.ID)
	m.updateGlobalXmin()
	m.mu.Unlock()
	
	return nil
}

// Rollback aborts a transaction.
func (m *Manager) Rollback(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	
	if txn.Status != types.TxnStatusRunning {
		return fmt.Errorf("transaction %d is not running (status: %s)", txn.ID, txn.Status)
	}
	
	txn.Status = types.TxnStatusAborted
	
	// Log ABORT
	if m.walWriter != nil {
		txn.LastLSN = m.walWriter.LogAbort(txn.ID)
	}
	
	// Release locks
	txn.HeldLocks = nil
	
	// Remove from active transactions
	m.mu.Lock()
	delete(m.activeTxns, txn.ID)
	m.updateGlobalXmin()
	m.mu.Unlock()
	
	return nil
}

// createSnapshotLocked creates a visibility snapshot (must hold m.mu).
func (m *Manager) createSnapshotLocked() *Snapshot {
	snap := &Snapshot{
		Xmin:       types.MaxTxnID,
		Xmax:       types.TxnID(atomic.LoadUint64(&m.nextTxnID)),
		ActiveTxns: make(map[types.TxnID]bool),
	}
	
	for txnID := range m.activeTxns {
		snap.ActiveTxns[txnID] = true
		if txnID < snap.Xmin {
			snap.Xmin = txnID
		}
	}
	
	if snap.Xmin == types.MaxTxnID {
		snap.Xmin = snap.Xmax
	}
	
	return snap
}

// updateGlobalXmin updates the global minimum transaction ID.
func (m *Manager) updateGlobalXmin() {
	m.globalXmin = types.MaxTxnID
	for txnID := range m.activeTxns {
		if txnID < m.globalXmin {
			m.globalXmin = txnID
		}
	}
}

// GetGlobalXmin returns the oldest active transaction ID.
func (m *Manager) GetGlobalXmin() types.TxnID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.globalXmin
}

// GetActiveTxns returns a list of active transaction IDs.
func (m *Manager) GetActiveTxns() []types.TxnID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	txns := make([]types.TxnID, 0, len(m.activeTxns))
	for txnID := range m.activeTxns {
		txns = append(txns, txnID)
	}
	return txns
}

// GetTransaction returns a transaction by ID.
func (m *Manager) GetTransaction(txnID types.TxnID) *Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeTxns[txnID]
}

// NextCommandID increments and returns the command ID for a transaction.
func (txn *Transaction) NextCommandID() types.CommandID {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.CommandID++
	return txn.CommandID
}

// SetNextTxnID sets the next transaction ID (used during recovery).
func (m *Manager) SetNextTxnID(id types.TxnID) {
	atomic.StoreUint64(&m.nextTxnID, uint64(id))
}
