// Package wal implements Write-Ahead Logging for crash recovery.
package wal

import (
	"encoding/binary"
	"fmt"
	"minidb/pkg/types"
)

// LogRecord represents a single WAL log entry.
type LogRecord struct {
	LSN       types.LSN
	PrevLSN   types.LSN           // Previous LSN for this transaction
	TxnID     types.TxnID
	Type      types.LogRecordType
	TableID   uint32
	RowID     uint64
	PageID    types.PageID
	SlotNum   uint16

	// For UPDATE/INSERT/DELETE
	BeforeImage []byte // Old value (for UNDO)
	AfterImage  []byte // New value (for REDO)
	
	// For CHECKPOINT
	ActiveTxns  []types.TxnID
	DirtyPages  map[types.PageID]types.LSN // PageID -> RecLSN
	
	// For CLR (Compensation Log Record)
	UndoNextLSN types.LSN
}

// Header size: LSN(8) + PrevLSN(8) + TxnID(8) + Type(1) + TableID(4) + RowID(8) + PageID(4) + SlotNum(2) + BeforeLen(4) + AfterLen(4)
const logRecordHeaderSize = 51

// Serialize converts the log record to bytes.
func (r *LogRecord) Serialize() []byte {
	beforeLen := len(r.BeforeImage)
	afterLen := len(r.AfterImage)
	
	// Calculate total size
	size := logRecordHeaderSize + beforeLen + afterLen
	
	// Add checkpoint data if present
	var checkpointData []byte
	if r.Type == types.LogRecordCheckpoint {
		checkpointData = r.serializeCheckpoint()
		size += 4 + len(checkpointData) // length prefix + data
	}
	
	// Add CLR data if present
	if r.Type == types.LogRecordCLR {
		size += 8 // UndoNextLSN
	}
	
	buf := make([]byte, size)
	offset := 0
	
	// Write header
	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.LSN))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.PrevLSN))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.TxnID))
	offset += 8
	buf[offset] = byte(r.Type)
	offset += 1
	binary.LittleEndian.PutUint32(buf[offset:], r.TableID)
	offset += 4
	binary.LittleEndian.PutUint64(buf[offset:], r.RowID)
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], uint32(r.PageID))
	offset += 4
	binary.LittleEndian.PutUint16(buf[offset:], r.SlotNum)
	offset += 2
	binary.LittleEndian.PutUint32(buf[offset:], uint32(beforeLen))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(afterLen))
	offset += 4
	
	// Write before/after images
	copy(buf[offset:], r.BeforeImage)
	offset += beforeLen
	copy(buf[offset:], r.AfterImage)
	offset += afterLen
	
	// Write checkpoint data
	if r.Type == types.LogRecordCheckpoint {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(checkpointData)))
		offset += 4
		copy(buf[offset:], checkpointData)
		offset += len(checkpointData)
	}
	
	// Write CLR data
	if r.Type == types.LogRecordCLR {
		binary.LittleEndian.PutUint64(buf[offset:], uint64(r.UndoNextLSN))
	}
	
	return buf
}

func (r *LogRecord) serializeCheckpoint() []byte {
	// Format: NumActiveTxns(4) + [TxnID(8)...] + NumDirtyPages(4) + [PageID(4) + RecLSN(8)...]
	size := 4 + len(r.ActiveTxns)*8 + 4 + len(r.DirtyPages)*12
	buf := make([]byte, size)
	offset := 0
	
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(r.ActiveTxns)))
	offset += 4
	for _, txn := range r.ActiveTxns {
		binary.LittleEndian.PutUint64(buf[offset:], uint64(txn))
		offset += 8
	}
	
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(r.DirtyPages)))
	offset += 4
	for pageID, recLSN := range r.DirtyPages {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(pageID))
		offset += 4
		binary.LittleEndian.PutUint64(buf[offset:], uint64(recLSN))
		offset += 8
	}
	
	return buf
}

// Deserialize creates a log record from bytes.
func Deserialize(buf []byte) (*LogRecord, int, error) {
	if len(buf) < logRecordHeaderSize {
		return nil, 0, fmt.Errorf("buffer too small for log record header")
	}
	
	offset := 0
	r := &LogRecord{}
	
	r.LSN = types.LSN(binary.LittleEndian.Uint64(buf[offset:]))
	offset += 8
	r.PrevLSN = types.LSN(binary.LittleEndian.Uint64(buf[offset:]))
	offset += 8
	r.TxnID = types.TxnID(binary.LittleEndian.Uint64(buf[offset:]))
	offset += 8
	r.Type = types.LogRecordType(buf[offset])
	offset += 1
	r.TableID = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	r.RowID = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	r.PageID = types.PageID(binary.LittleEndian.Uint32(buf[offset:]))
	offset += 4
	r.SlotNum = binary.LittleEndian.Uint16(buf[offset:])
	offset += 2
	beforeLen := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	afterLen := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	
	// Read before/after images
	if len(buf) < offset+int(beforeLen)+int(afterLen) {
		return nil, 0, fmt.Errorf("buffer too small for log record data")
	}
	
	if beforeLen > 0 {
		r.BeforeImage = make([]byte, beforeLen)
		copy(r.BeforeImage, buf[offset:offset+int(beforeLen)])
		offset += int(beforeLen)
	}
	
	if afterLen > 0 {
		r.AfterImage = make([]byte, afterLen)
		copy(r.AfterImage, buf[offset:offset+int(afterLen)])
		offset += int(afterLen)
	}
	
	// Read checkpoint data
	if r.Type == types.LogRecordCheckpoint {
		if len(buf) < offset+4 {
			return nil, 0, fmt.Errorf("buffer too small for checkpoint length")
		}
		checkpointLen := binary.LittleEndian.Uint32(buf[offset:])
		offset += 4
		if len(buf) < offset+int(checkpointLen) {
			return nil, 0, fmt.Errorf("buffer too small for checkpoint data")
		}
		r.deserializeCheckpoint(buf[offset : offset+int(checkpointLen)])
		offset += int(checkpointLen)
	}
	
	// Read CLR data
	if r.Type == types.LogRecordCLR {
		if len(buf) < offset+8 {
			return nil, 0, fmt.Errorf("buffer too small for CLR data")
		}
		r.UndoNextLSN = types.LSN(binary.LittleEndian.Uint64(buf[offset:]))
		offset += 8
	}
	
	return r, offset, nil
}

func (r *LogRecord) deserializeCheckpoint(buf []byte) {
	offset := 0
	
	numActiveTxns := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	r.ActiveTxns = make([]types.TxnID, numActiveTxns)
	for i := uint32(0); i < numActiveTxns; i++ {
		r.ActiveTxns[i] = types.TxnID(binary.LittleEndian.Uint64(buf[offset:]))
		offset += 8
	}
	
	numDirtyPages := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	r.DirtyPages = make(map[types.PageID]types.LSN, numDirtyPages)
	for i := uint32(0); i < numDirtyPages; i++ {
		pageID := types.PageID(binary.LittleEndian.Uint32(buf[offset:]))
		offset += 4
		recLSN := types.LSN(binary.LittleEndian.Uint64(buf[offset:]))
		offset += 8
		r.DirtyPages[pageID] = recLSN
	}
}

func (r *LogRecord) String() string {
	return fmt.Sprintf("LogRecord{LSN:%d, TxnID:%d, Type:%s, Table:%d, Row:%d}",
		r.LSN, r.TxnID, r.Type.String(), r.TableID, r.RowID)
}
