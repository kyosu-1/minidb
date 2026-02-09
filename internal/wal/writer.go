package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"minidb/pkg/types"
	"os"
	"sync"
)

// Writer handles WAL log writing and flushing.
type Writer struct {
	mu       sync.Mutex
	file     *os.File
	filePath string
	
	// Current LSN (monotonically increasing)
	currentLSN types.LSN
	
	// Flushed LSN (everything up to this is on disk)
	flushedLSN types.LSN
	
	// Buffer for batching writes
	buffer    []byte
	bufferLSN types.LSN // LSN of first record in buffer
	
	// Transaction tracking for PrevLSN
	txnLastLSN map[types.TxnID]types.LSN

	// Max TxnID seen in WAL (for recovery)
	maxTxnID types.TxnID
}

const (
	walBufferSize  = 64 * 1024 // 64KB buffer
	walFileHeader  = 16        // Magic(8) + Version(4) + Reserved(4)
	walMagic       = uint64(0x4D494E4944425741) // "MINIDBWA"
	walVersion     = uint32(1)
)

// NewWriter creates a new WAL writer.
func NewWriter(path string) (*Writer, error) {
	w := &Writer{
		filePath:   path,
		currentLSN: 1,
		flushedLSN: 0,
		buffer:     make([]byte, 0, walBufferSize),
		txnLastLSN: make(map[types.TxnID]types.LSN),
	}
	
	// Open or create the WAL file
	var err error
	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		// Create new file
		w.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to create WAL file: %w", err)
		}
		// Write header
		if err := w.writeHeader(); err != nil {
			w.file.Close()
			return nil, err
		}
	} else {
		// Open existing file
		w.file, err = os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL file: %w", err)
		}
		// Read and validate header, find last LSN
		if err := w.readHeader(); err != nil {
			w.file.Close()
			return nil, err
		}
		if err := w.findLastLSN(); err != nil {
			w.file.Close()
			return nil, err
		}
	}
	
	return w, nil
}

func (w *Writer) writeHeader() error {
	header := make([]byte, walFileHeader)
	binary.LittleEndian.PutUint64(header[0:8], walMagic)
	binary.LittleEndian.PutUint32(header[8:12], walVersion)
	_, err := w.file.Write(header)
	return err
}

func (w *Writer) readHeader() error {
	header := make([]byte, walFileHeader)
	n, err := w.file.Read(header)
	if err != nil {
		return fmt.Errorf("failed to read WAL header: %w", err)
	}
	if n < walFileHeader {
		return fmt.Errorf("incomplete WAL header")
	}
	
	magic := binary.LittleEndian.Uint64(header[0:8])
	if magic != walMagic {
		return fmt.Errorf("invalid WAL magic number")
	}
	
	version := binary.LittleEndian.Uint32(header[8:12])
	if version != walVersion {
		return fmt.Errorf("unsupported WAL version: %d", version)
	}
	
	return nil
}

func (w *Writer) findLastLSN() error {
	// Seek to end to find last valid LSN
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	
	if info.Size() <= walFileHeader {
		// Empty WAL
		w.currentLSN = 1
		w.flushedLSN = 0
		return nil
	}
	
	// Scan through all records to find the last one
	w.file.Seek(walFileHeader, 0)
	lastLSN := types.LSN(0)
	
	for {
		// Read record length prefix
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(w.file, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			break // Possibly incomplete record
		}
		
		recordLen := binary.LittleEndian.Uint32(lenBuf)
		recordBuf := make([]byte, recordLen)
		_, err = io.ReadFull(w.file, recordBuf)
		if err != nil {
			break
		}
		
		record, _, err := Deserialize(recordBuf)
		if err != nil {
			break
		}
		
		lastLSN = record.LSN

		// Track max TxnID
		if record.TxnID > w.maxTxnID {
			w.maxTxnID = record.TxnID
		}

		// Track transaction's last LSN
		if record.Type != types.LogRecordCheckpoint {
			w.txnLastLSN[record.TxnID] = record.LSN
		}
		
		// Clean up committed/aborted transactions
		if record.Type == types.LogRecordCommit || record.Type == types.LogRecordAbort {
			delete(w.txnLastLSN, record.TxnID)
		}
	}
	
	w.currentLSN = lastLSN + 1
	w.flushedLSN = lastLSN
	
	// Seek to end for appending
	w.file.Seek(0, 2)
	
	return nil
}

// Append adds a log record to the buffer.
func (w *Writer) Append(record *LogRecord) types.LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Assign LSN
	record.LSN = w.currentLSN
	w.currentLSN++
	
	// Set PrevLSN for this transaction
	if prev, ok := w.txnLastLSN[record.TxnID]; ok {
		record.PrevLSN = prev
	} else {
		record.PrevLSN = types.InvalidLSN
	}
	
	// Update transaction's last LSN
	if record.Type != types.LogRecordCheckpoint {
		w.txnLastLSN[record.TxnID] = record.LSN
	}
	
	// Serialize and add to buffer
	data := record.Serialize()
	
	// Write length prefix + data
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	
	if len(w.buffer) == 0 {
		w.bufferLSN = record.LSN
	}
	
	w.buffer = append(w.buffer, lenBuf...)
	w.buffer = append(w.buffer, data...)
	
	// Auto-flush if buffer is full
	if len(w.buffer) >= walBufferSize {
		w.flushLocked()
	}
	
	return record.LSN
}

// Force ensures all records up to the given LSN are on disk.
func (w *Writer) Force(lsn types.LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if lsn <= w.flushedLSN {
		return nil // Already flushed
	}
	
	return w.flushLocked()
}

func (w *Writer) flushLocked() error {
	if len(w.buffer) == 0 {
		return nil
	}
	
	// Write buffer to file
	_, err := w.file.Write(w.buffer)
	if err != nil {
		return fmt.Errorf("failed to write WAL: %w", err)
	}
	
	// Sync to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}
	
	w.flushedLSN = w.currentLSN - 1
	w.buffer = w.buffer[:0]
	
	return nil
}

// Flush writes all buffered records to disk.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

// LogBegin logs a transaction begin.
func (w *Writer) LogBegin(txnID types.TxnID) types.LSN {
	return w.Append(&LogRecord{
		TxnID: txnID,
		Type:  types.LogRecordBegin,
	})
}

// LogCommit logs a transaction commit and forces to disk.
func (w *Writer) LogCommit(txnID types.TxnID) (types.LSN, error) {
	lsn := w.Append(&LogRecord{
		TxnID: txnID,
		Type:  types.LogRecordCommit,
	})
	
	// CRITICAL: Force commit record to disk for durability
	if err := w.Force(lsn); err != nil {
		return lsn, err
	}
	
	// Clean up transaction tracking
	w.mu.Lock()
	delete(w.txnLastLSN, txnID)
	w.mu.Unlock()
	
	return lsn, nil
}

// LogAbort logs a transaction abort.
func (w *Writer) LogAbort(txnID types.TxnID) types.LSN {
	lsn := w.Append(&LogRecord{
		TxnID: txnID,
		Type:  types.LogRecordAbort,
	})
	
	w.mu.Lock()
	delete(w.txnLastLSN, txnID)
	w.mu.Unlock()
	
	return lsn
}

// LogUpdate logs an update operation.
func (w *Writer) LogUpdate(txnID types.TxnID, tableID uint32, rowID uint64, pageID types.PageID, slotNum uint16, before, after []byte) types.LSN {
	return w.Append(&LogRecord{
		TxnID:       txnID,
		Type:        types.LogRecordUpdate,
		TableID:     tableID,
		RowID:       rowID,
		PageID:      pageID,
		SlotNum:     slotNum,
		BeforeImage: before,
		AfterImage:  after,
	})
}

// LogInsert logs an insert operation.
func (w *Writer) LogInsert(txnID types.TxnID, tableID uint32, rowID uint64, pageID types.PageID, slotNum uint16, data []byte) types.LSN {
	return w.Append(&LogRecord{
		TxnID:      txnID,
		Type:       types.LogRecordInsert,
		TableID:    tableID,
		RowID:      rowID,
		PageID:     pageID,
		SlotNum:    slotNum,
		AfterImage: data,
	})
}

// LogDelete logs a delete operation.
func (w *Writer) LogDelete(txnID types.TxnID, tableID uint32, rowID uint64, pageID types.PageID, slotNum uint16, data []byte) types.LSN {
	return w.Append(&LogRecord{
		TxnID:       txnID,
		Type:        types.LogRecordDelete,
		TableID:     tableID,
		RowID:       rowID,
		PageID:      pageID,
		SlotNum:     slotNum,
		BeforeImage: data,
	})
}

// LogCheckpoint logs a checkpoint.
func (w *Writer) LogCheckpoint(activeTxns []types.TxnID, dirtyPages map[types.PageID]types.LSN) (types.LSN, error) {
	lsn := w.Append(&LogRecord{
		TxnID:      types.InvalidTxnID,
		Type:       types.LogRecordCheckpoint,
		ActiveTxns: activeTxns,
		DirtyPages: dirtyPages,
	})
	
	return lsn, w.Force(lsn)
}

// LogCLR logs a compensation log record during UNDO.
func (w *Writer) LogCLR(txnID types.TxnID, tableID uint32, rowID uint64, pageID types.PageID, slotNum uint16, undoNextLSN types.LSN, data []byte) types.LSN {
	return w.Append(&LogRecord{
		TxnID:       txnID,
		Type:        types.LogRecordCLR,
		TableID:     tableID,
		RowID:       rowID,
		PageID:      pageID,
		SlotNum:     slotNum,
		AfterImage:  data,
		UndoNextLSN: undoNextLSN,
	})
}

// GetCurrentLSN returns the next LSN to be assigned.
func (w *Writer) GetCurrentLSN() types.LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLSN
}

// GetFlushedLSN returns the last LSN guaranteed to be on disk.
func (w *Writer) GetFlushedLSN() types.LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushedLSN
}

// Close closes the WAL file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if err := w.flushLocked(); err != nil {
		return err
	}
	
	return w.file.Close()
}

// GetTxnLastLSN returns the last LSN for a transaction (for UNDO).
func (w *Writer) GetTxnLastLSN(txnID types.TxnID) types.LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.txnLastLSN[txnID]
}

// GetMaxTxnID returns the maximum TxnID seen in the WAL.
func (w *Writer) GetMaxTxnID() types.TxnID {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxTxnID
}
