package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"minidb/pkg/types"
	"os"
	"sort"
)

// RecoveryManager handles ARIES-style crash recovery.
type RecoveryManager struct {
	walPath string
	
	// Analysis phase results
	activeTxnTable map[types.TxnID]*TxnEntry    // Active Transaction Table
	dirtyPageTable map[types.PageID]types.LSN   // Dirty Page Table (PageID -> RecLSN)
	
	// Callback for applying redo/undo
	redoCallback func(record *LogRecord) error
	undoCallback func(record *LogRecord) error

	// Callback to get page LSN for redo skip check
	pageLSNCallback func(types.PageID) types.LSN

	// WAL writer for CLR records during undo
	walWriter *Writer
}

// TxnEntry represents an entry in the Active Transaction Table.
type TxnEntry struct {
	TxnID     types.TxnID
	Status    types.TxnStatus
	LastLSN   types.LSN
	UndoNext  types.LSN // For CLR chaining
}

// NewRecoveryManager creates a new recovery manager.
func NewRecoveryManager(walPath string, walWriter *Writer) *RecoveryManager {
	return &RecoveryManager{
		walPath:        walPath,
		activeTxnTable: make(map[types.TxnID]*TxnEntry),
		dirtyPageTable: make(map[types.PageID]types.LSN),
		walWriter:      walWriter,
	}
}

// SetCallbacks sets the redo and undo callbacks.
func (rm *RecoveryManager) SetCallbacks(redo, undo func(*LogRecord) error) {
	rm.redoCallback = redo
	rm.undoCallback = undo
}

// SetPageLSNCallback sets the callback to get page LSN for redo skip check.
func (rm *RecoveryManager) SetPageLSNCallback(cb func(types.PageID) types.LSN) {
	rm.pageLSNCallback = cb
}

// Recover performs full ARIES recovery: Analysis -> Redo -> Undo.
func (rm *RecoveryManager) Recover() error {
	fmt.Println("=== Starting ARIES Recovery ===")
	
	// Phase 1: Analysis
	fmt.Println("\n--- Phase 1: Analysis ---")
	checkpointLSN, err := rm.analysisPhase()
	if err != nil {
		return fmt.Errorf("analysis phase failed: %w", err)
	}
	fmt.Printf("Checkpoint LSN: %d\n", checkpointLSN)
	fmt.Printf("Active transactions: %d\n", len(rm.activeTxnTable))
	fmt.Printf("Dirty pages: %d\n", len(rm.dirtyPageTable))
	
	// Phase 2: Redo
	fmt.Println("\n--- Phase 2: Redo ---")
	if err := rm.redoPhase(); err != nil {
		return fmt.Errorf("redo phase failed: %w", err)
	}
	
	// Phase 3: Undo
	fmt.Println("\n--- Phase 3: Undo ---")
	if err := rm.undoPhase(); err != nil {
		return fmt.Errorf("undo phase failed: %w", err)
	}
	
	fmt.Println("\n=== Recovery Complete ===")
	return nil
}

// analysisPhase scans the log to rebuild ATT and DPT.
func (rm *RecoveryManager) analysisPhase() (types.LSN, error) {
	file, err := os.Open(rm.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No WAL, nothing to recover
		}
		return 0, err
	}
	defer file.Close()
	
	// Skip header
	file.Seek(walFileHeader, 0)
	
	// Find last checkpoint
	var lastCheckpointLSN types.LSN = 0
	var lastCheckpointRecord *LogRecord
	
	records, err := rm.readAllRecords(file)
	if err != nil {
		return 0, err
	}
	
	// First pass: find checkpoint
	for _, record := range records {
		if record.Type == types.LogRecordCheckpoint {
			lastCheckpointLSN = record.LSN
			lastCheckpointRecord = record
		}
	}
	
	// Initialize from checkpoint if found
	if lastCheckpointRecord != nil {
		for _, txnID := range lastCheckpointRecord.ActiveTxns {
			rm.activeTxnTable[txnID] = &TxnEntry{
				TxnID:  txnID,
				Status: types.TxnStatusRunning,
			}
		}
		for pageID, recLSN := range lastCheckpointRecord.DirtyPages {
			rm.dirtyPageTable[pageID] = recLSN
		}
	}
	
	// Second pass: scan from checkpoint
	for _, record := range records {
		if lastCheckpointLSN > 0 && record.LSN <= lastCheckpointLSN {
			continue
		}
		
		switch record.Type {
		case types.LogRecordBegin:
			rm.activeTxnTable[record.TxnID] = &TxnEntry{
				TxnID:   record.TxnID,
				Status:  types.TxnStatusRunning,
				LastLSN: record.LSN,
			}
			
		case types.LogRecordCommit:
			delete(rm.activeTxnTable, record.TxnID)
			
		case types.LogRecordAbort:
			if entry, ok := rm.activeTxnTable[record.TxnID]; ok {
				entry.Status = types.TxnStatusAborted
				entry.LastLSN = record.LSN
			}
			
		case types.LogRecordUpdate, types.LogRecordInsert, types.LogRecordDelete:
			if entry, ok := rm.activeTxnTable[record.TxnID]; ok {
				entry.LastLSN = record.LSN
			}
			// Add to dirty page table
			if _, exists := rm.dirtyPageTable[record.PageID]; !exists {
				rm.dirtyPageTable[record.PageID] = record.LSN // RecLSN
			}
			
		case types.LogRecordCLR:
			if entry, ok := rm.activeTxnTable[record.TxnID]; ok {
				entry.LastLSN = record.LSN
				entry.UndoNext = record.UndoNextLSN
			}
		}
	}
	
	return lastCheckpointLSN, nil
}

// redoPhase replays all logged actions from the minimum RecLSN.
func (rm *RecoveryManager) redoPhase() error {
	if len(rm.dirtyPageTable) == 0 {
		fmt.Println("No dirty pages, skipping redo")
		return nil
	}
	
	// Find minimum RecLSN
	var minRecLSN types.LSN = types.LSN(^uint64(0))
	for _, recLSN := range rm.dirtyPageTable {
		if recLSN < minRecLSN {
			minRecLSN = recLSN
		}
	}
	
	fmt.Printf("Redo starting from LSN: %d\n", minRecLSN)
	
	file, err := os.Open(rm.walPath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	file.Seek(walFileHeader, 0)
	records, err := rm.readAllRecords(file)
	if err != nil {
		return err
	}
	
	redoCount := 0
	for _, record := range records {
		if record.LSN < minRecLSN {
			continue
		}
		
		// Only redo data-modifying records
		if record.Type != types.LogRecordUpdate &&
			record.Type != types.LogRecordInsert &&
			record.Type != types.LogRecordDelete &&
			record.Type != types.LogRecordCLR {
			continue
		}
		
		// Check if page is in DPT
		recLSN, inDPT := rm.dirtyPageTable[record.PageID]
		if !inDPT {
			continue
		}

		// Check if record LSN < RecLSN
		if record.LSN < recLSN {
			continue
		}

		// Check pageLSN: skip if page already has this change
		if rm.pageLSNCallback != nil {
			pageLSN := rm.pageLSNCallback(record.PageID)
			if pageLSN >= record.LSN {
				continue
			}
		}

		// Apply redo
		if rm.redoCallback != nil {
			fmt.Printf("REDO: %s\n", record.String())
			if err := rm.redoCallback(record); err != nil {
				return fmt.Errorf("redo failed for LSN %d: %w", record.LSN, err)
			}
			redoCount++
		}
	}
	
	fmt.Printf("Redo applied %d operations\n", redoCount)
	return nil
}

// undoPhase rolls back all incomplete transactions.
func (rm *RecoveryManager) undoPhase() error {
	if len(rm.activeTxnTable) == 0 {
		fmt.Println("No active transactions, skipping undo")
		return nil
	}
	
	fmt.Printf("Undo for %d active transactions\n", len(rm.activeTxnTable))
	
	// Collect all LSNs to undo
	toUndo := make([]types.LSN, 0)
	for _, entry := range rm.activeTxnTable {
		if entry.UndoNext != 0 {
			toUndo = append(toUndo, entry.UndoNext)
		} else if entry.LastLSN != 0 {
			toUndo = append(toUndo, entry.LastLSN)
		}
	}
	
	// Read all records into memory for random access
	file, err := os.Open(rm.walPath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	file.Seek(walFileHeader, 0)
	records, err := rm.readAllRecords(file)
	if err != nil {
		return err
	}
	
	// Build LSN -> Record map
	recordMap := make(map[types.LSN]*LogRecord)
	for _, record := range records {
		recordMap[record.LSN] = record
	}
	
	undoCount := 0
	
	// Process in reverse LSN order
	for len(toUndo) > 0 {
		// Sort descending and take max
		sort.Slice(toUndo, func(i, j int) bool {
			return toUndo[i] > toUndo[j]
		})
		
		lsn := toUndo[0]
		toUndo = toUndo[1:]
		
		record, ok := recordMap[lsn]
		if !ok {
			continue
		}
		
		// Skip non-data records
		if record.Type != types.LogRecordUpdate &&
			record.Type != types.LogRecordInsert &&
			record.Type != types.LogRecordDelete {
			if record.Type == types.LogRecordCLR {
				// CLR: follow UndoNextLSN to skip already-compensated records
				if record.UndoNextLSN != 0 {
					toUndo = append(toUndo, record.UndoNextLSN)
				}
			} else {
				// Other non-data records (BEGIN, etc.): follow PrevLSN
				if record.PrevLSN != 0 {
					toUndo = append(toUndo, record.PrevLSN)
				}
			}
			continue
		}
		
		// Apply undo
		if rm.undoCallback != nil {
			fmt.Printf("UNDO: %s\n", record.String())
			if err := rm.undoCallback(record); err != nil {
				return fmt.Errorf("undo failed for LSN %d: %w", record.LSN, err)
			}
			undoCount++
		}
		
		// Write CLR
		if rm.walWriter != nil {
			rm.walWriter.LogCLR(
				record.TxnID,
				record.TableID,
				record.RowID,
				record.PageID,
				record.SlotNum,
				record.PrevLSN,
				record.BeforeImage,
			)
		}
		
		// Follow PrevLSN
		if record.PrevLSN != 0 {
			toUndo = append(toUndo, record.PrevLSN)
		}
	}
	
	// Write abort records for all active transactions
	if rm.walWriter != nil {
		for txnID := range rm.activeTxnTable {
			rm.walWriter.LogAbort(txnID)
		}
	}
	
	fmt.Printf("Undo applied %d operations\n", undoCount)
	return nil
}

// readAllRecords reads all log records from the current file position.
func (rm *RecoveryManager) readAllRecords(file *os.File) ([]*LogRecord, error) {
	var records []*LogRecord
	
	for {
		// Read length prefix
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(file, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			break // Incomplete record
		}
		
		recordLen := binary.LittleEndian.Uint32(lenBuf)
		recordBuf := make([]byte, recordLen)
		_, err = io.ReadFull(file, recordBuf)
		if err != nil {
			break
		}
		
		record, _, err := Deserialize(recordBuf)
		if err != nil {
			break
		}
		
		records = append(records, record)
	}
	
	return records, nil
}

// GetActiveTxnTable returns the active transaction table after analysis.
func (rm *RecoveryManager) GetActiveTxnTable() map[types.TxnID]*TxnEntry {
	return rm.activeTxnTable
}

// GetDirtyPageTable returns the dirty page table after analysis.
func (rm *RecoveryManager) GetDirtyPageTable() map[types.PageID]types.LSN {
	return rm.dirtyPageTable
}
