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
	InvalidPageID  = PageID(0xFFFFFFFF)
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

// SerializeRow encodes a row as compact binary using the schema's column order.
//
// Format:
//
//	NullBitmap: ceil(numColumns/8) bytes, LSB first (bit i=1 → column i is NULL)
//	Column values in schema order (NULLs skipped):
//	  INT    → int64 little-endian (8 bytes)
//	  STRING → uint16 LE length + UTF-8 bytes
//	  BOOL   → 1 byte (0x00=false, 0x01=true)
func SerializeRow(schema *Schema, values map[string]Value) ([]byte, error) {
	numCols := len(schema.Columns)
	bitmapLen := (numCols + 7) / 8
	// Pre-allocate with estimated size
	buf := make([]byte, bitmapLen, bitmapLen+numCols*8)

	for i, col := range schema.Columns {
		val, ok := values[col.Name]
		if !ok || val.IsNull {
			// Set null bit: byte = i/8, bit = i%8
			buf[i/8] |= 1 << (uint(i) % 8)
			continue
		}
		switch col.Type {
		case ValueTypeInt:
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(val.IntVal))
			buf = append(buf, b...)
		case ValueTypeString:
			sLen := len(val.StrVal)
			if sLen > 65535 {
				return nil, fmt.Errorf("string too long for column %s: %d bytes", col.Name, sLen)
			}
			b := make([]byte, 2)
			binary.LittleEndian.PutUint16(b, uint16(sLen))
			buf = append(buf, b...)
			buf = append(buf, val.StrVal...)
		case ValueTypeBool:
			if val.BoolVal {
				buf = append(buf, 0x01)
			} else {
				buf = append(buf, 0x00)
			}
		default:
			return nil, fmt.Errorf("unsupported column type for column %s", col.Name)
		}
	}
	return buf, nil
}

// DeserializeRow decodes binary row data back into a map using the schema.
func DeserializeRow(schema *Schema, data []byte) (map[string]Value, error) {
	numCols := len(schema.Columns)
	bitmapLen := (numCols + 7) / 8

	if len(data) < bitmapLen {
		return nil, fmt.Errorf("data too short: need at least %d bytes for null bitmap, got %d", bitmapLen, len(data))
	}

	result := make(map[string]Value, numCols)
	offset := bitmapLen

	for i, col := range schema.Columns {
		// Check null bit
		if data[i/8]&(1<<(uint(i)%8)) != 0 {
			result[col.Name] = Value{IsNull: true}
			continue
		}

		switch col.Type {
		case ValueTypeInt:
			if offset+8 > len(data) {
				return nil, fmt.Errorf("data truncated reading INT column %s", col.Name)
			}
			v := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			result[col.Name] = Value{Type: ValueTypeInt, IntVal: v}
			offset += 8
		case ValueTypeString:
			if offset+2 > len(data) {
				return nil, fmt.Errorf("data truncated reading STRING length for column %s", col.Name)
			}
			sLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if offset+sLen > len(data) {
				return nil, fmt.Errorf("data truncated reading STRING data for column %s", col.Name)
			}
			result[col.Name] = Value{Type: ValueTypeString, StrVal: string(data[offset : offset+sLen])}
			offset += sLen
		case ValueTypeBool:
			if offset+1 > len(data) {
				return nil, fmt.Errorf("data truncated reading BOOL column %s", col.Name)
			}
			result[col.Name] = Value{Type: ValueTypeBool, BoolVal: data[offset] != 0}
			offset++
		default:
			return nil, fmt.Errorf("unsupported column type for column %s", col.Name)
		}
	}
	return result, nil
}
