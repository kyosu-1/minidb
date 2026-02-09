// Package types provides common type definitions for minidb.
package types

import (
	"encoding/binary"
	"fmt"
)

// PageID represents a unique identifier for a page.
type PageID uint32

// TxnID represents a transaction identifier.
type TxnID uint64

// LSN (Log Sequence Number) represents a position in the WAL.
type LSN uint64

// CommandID represents the order of operations within a transaction.
type CommandID uint32

// Constants
const (
	PageSize       = 4096
	InvalidPageID  = PageID(0)
	InvalidTxnID   = TxnID(0)
	InvalidLSN     = LSN(0)
	MaxTxnID       = TxnID(^uint64(0))
)

// TxnStatus represents the state of a transaction.
type TxnStatus int

const (
	TxnStatusRunning TxnStatus = iota
	TxnStatusCommitted
	TxnStatusAborted
)

func (s TxnStatus) String() string {
	switch s {
	case TxnStatusRunning:
		return "RUNNING"
	case TxnStatusCommitted:
		return "COMMITTED"
	case TxnStatusAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// LogRecordType represents the type of a WAL log record.
type LogRecordType uint8

const (
	LogRecordBegin LogRecordType = iota
	LogRecordCommit
	LogRecordAbort
	LogRecordUpdate
	LogRecordInsert
	LogRecordDelete
	LogRecordCheckpoint
	LogRecordCLR // Compensation Log Record for UNDO
)

func (t LogRecordType) String() string {
	names := []string{"BEGIN", "COMMIT", "ABORT", "UPDATE", "INSERT", "DELETE", "CHECKPOINT", "CLR"}
	if int(t) < len(names) {
		return names[t]
	}
	return "UNKNOWN"
}

// Tuple represents a row in a table with MVCC metadata.
type Tuple struct {
	XMin     TxnID     // Transaction that created this version
	XMax     TxnID     // Transaction that deleted this version (0 if alive)
	Cid      CommandID // Command ID within transaction
	TableID  uint32    // Table identifier
	RowID    uint64    // Row identifier
	Data     []byte    // Actual row data
}

// IsDeleted returns true if this tuple version has been deleted.
func (t *Tuple) IsDeleted() bool {
	return t.XMax != InvalidTxnID
}

// Clone creates a deep copy of the tuple.
func (t *Tuple) Clone() *Tuple {
	data := make([]byte, len(t.Data))
	copy(data, t.Data)
	return &Tuple{
		XMin:    t.XMin,
		XMax:    t.XMax,
		Cid:     t.Cid,
		TableID: t.TableID,
		RowID:   t.RowID,
		Data:    data,
	}
}

// Serialize converts the tuple to bytes.
func (t *Tuple) Serialize() []byte {
	// Format: XMin(8) + XMax(8) + Cid(4) + TableID(4) + RowID(8) + DataLen(4) + Data
	buf := make([]byte, 36+len(t.Data))
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t.XMin))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(t.XMax))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(t.Cid))
	binary.LittleEndian.PutUint32(buf[20:24], t.TableID)
	binary.LittleEndian.PutUint64(buf[24:32], t.RowID)
	binary.LittleEndian.PutUint32(buf[32:36], uint32(len(t.Data)))
	copy(buf[36:], t.Data)
	return buf
}

// DeserializeTuple creates a tuple from bytes.
func DeserializeTuple(buf []byte) (*Tuple, error) {
	if len(buf) < 36 {
		return nil, fmt.Errorf("buffer too small for tuple header")
	}
	dataLen := binary.LittleEndian.Uint32(buf[32:36])
	if len(buf) < 36+int(dataLen) {
		return nil, fmt.Errorf("buffer too small for tuple data")
	}
	data := make([]byte, dataLen)
	copy(data, buf[36:36+dataLen])
	return &Tuple{
		XMin:    TxnID(binary.LittleEndian.Uint64(buf[0:8])),
		XMax:    TxnID(binary.LittleEndian.Uint64(buf[8:16])),
		Cid:     CommandID(binary.LittleEndian.Uint32(buf[16:20])),
		TableID: binary.LittleEndian.Uint32(buf[20:24]),
		RowID:   binary.LittleEndian.Uint64(buf[24:32]),
		Data:    data,
	}, nil
}

// Value represents a SQL value.
type Value struct {
	Type    ValueType
	IsNull  bool
	IntVal  int64
	StrVal  string
	BoolVal bool
}

type ValueType int

const (
	ValueTypeNull ValueType = iota
	ValueTypeInt
	ValueTypeString
	ValueTypeBool
)

func (v Value) String() string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case ValueTypeInt:
		return fmt.Sprintf("%d", v.IntVal)
	case ValueTypeString:
		return v.StrVal
	case ValueTypeBool:
		return fmt.Sprintf("%t", v.BoolVal)
	default:
		return "NULL"
	}
}

// Row represents a row of values.
type Row struct {
	Values []Value
}

// Schema represents a table schema.
type Schema struct {
	TableName string
	Columns   []Column
}

// Column represents a column definition.
type Column struct {
	Name     string
	Type     ValueType
	Nullable bool
}
