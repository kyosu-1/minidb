package wal

import (
	"bytes"
	"minidb/pkg/types"
	"testing"
)

func TestLogRecordSerializeDeserialize(t *testing.T) {
	tests := []struct {
		name   string
		record *LogRecord
	}{
		{
			name: "BEGIN",
			record: &LogRecord{
				LSN:   1,
				TxnID: types.TxnID(1),
				Type:  types.LogRecordBegin,
			},
		},
		{
			name: "COMMIT",
			record: &LogRecord{
				LSN:     2,
				PrevLSN: 1,
				TxnID:   types.TxnID(1),
				Type:    types.LogRecordCommit,
			},
		},
		{
			name: "ABORT",
			record: &LogRecord{
				LSN:   3,
				TxnID: types.TxnID(2),
				Type:  types.LogRecordAbort,
			},
		},
		{
			name: "INSERT",
			record: &LogRecord{
				LSN:        4,
				TxnID:      types.TxnID(1),
				Type:       types.LogRecordInsert,
				TableID:    1,
				RowID:      100,
				PageID:     types.PageID(5),
				SlotNum:    2,
				AfterImage: []byte("inserted data"),
			},
		},
		{
			name: "UPDATE",
			record: &LogRecord{
				LSN:         5,
				PrevLSN:     4,
				TxnID:       types.TxnID(1),
				Type:        types.LogRecordUpdate,
				TableID:     1,
				RowID:       100,
				PageID:      types.PageID(5),
				SlotNum:     2,
				BeforeImage: []byte("old data"),
				AfterImage:  []byte("new data"),
			},
		},
		{
			name: "DELETE",
			record: &LogRecord{
				LSN:         6,
				TxnID:       types.TxnID(1),
				Type:        types.LogRecordDelete,
				TableID:     2,
				RowID:       50,
				PageID:      types.PageID(3),
				SlotNum:     0,
				BeforeImage: []byte("deleted data"),
			},
		},
		{
			name: "CLR",
			record: &LogRecord{
				LSN:         7,
				TxnID:       types.TxnID(1),
				Type:        types.LogRecordCLR,
				TableID:     1,
				RowID:       100,
				PageID:      types.PageID(5),
				SlotNum:     2,
				AfterImage:  []byte("compensation"),
				UndoNextLSN: types.LSN(3),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.record.Serialize()
			got, consumed, err := Deserialize(buf)
			if err != nil {
				t.Fatalf("Deserialize() error = %v", err)
			}
			if consumed != len(buf) {
				t.Errorf("consumed = %d, want %d", consumed, len(buf))
			}
			if got.LSN != tt.record.LSN {
				t.Errorf("LSN = %d, want %d", got.LSN, tt.record.LSN)
			}
			if got.PrevLSN != tt.record.PrevLSN {
				t.Errorf("PrevLSN = %d, want %d", got.PrevLSN, tt.record.PrevLSN)
			}
			if got.TxnID != tt.record.TxnID {
				t.Errorf("TxnID = %d, want %d", got.TxnID, tt.record.TxnID)
			}
			if got.Type != tt.record.Type {
				t.Errorf("Type = %d, want %d", got.Type, tt.record.Type)
			}
			if got.TableID != tt.record.TableID {
				t.Errorf("TableID = %d, want %d", got.TableID, tt.record.TableID)
			}
			if got.RowID != tt.record.RowID {
				t.Errorf("RowID = %d, want %d", got.RowID, tt.record.RowID)
			}
			if got.PageID != tt.record.PageID {
				t.Errorf("PageID = %d, want %d", got.PageID, tt.record.PageID)
			}
			if got.SlotNum != tt.record.SlotNum {
				t.Errorf("SlotNum = %d, want %d", got.SlotNum, tt.record.SlotNum)
			}
			if !bytes.Equal(got.BeforeImage, tt.record.BeforeImage) {
				t.Errorf("BeforeImage mismatch")
			}
			if !bytes.Equal(got.AfterImage, tt.record.AfterImage) {
				t.Errorf("AfterImage mismatch")
			}
			if got.Type == types.LogRecordCLR && got.UndoNextLSN != tt.record.UndoNextLSN {
				t.Errorf("UndoNextLSN = %d, want %d", got.UndoNextLSN, tt.record.UndoNextLSN)
			}
		})
	}
}

func TestCheckpointSerializeDeserialize(t *testing.T) {
	record := &LogRecord{
		LSN:   10,
		TxnID: types.InvalidTxnID,
		Type:  types.LogRecordCheckpoint,
		ActiveTxns: []types.TxnID{
			types.TxnID(1),
			types.TxnID(2),
			types.TxnID(5),
		},
		DirtyPages: map[types.PageID]types.LSN{
			types.PageID(0): types.LSN(3),
			types.PageID(2): types.LSN(7),
		},
	}

	buf := record.Serialize()
	got, consumed, err := Deserialize(buf)
	if err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}
	if consumed != len(buf) {
		t.Errorf("consumed = %d, want %d", consumed, len(buf))
	}
	if len(got.ActiveTxns) != 3 {
		t.Errorf("ActiveTxns length = %d, want 3", len(got.ActiveTxns))
	}
	if len(got.DirtyPages) != 2 {
		t.Errorf("DirtyPages length = %d, want 2", len(got.DirtyPages))
	}

	// Verify dirty pages values
	for pageID, recLSN := range record.DirtyPages {
		gotLSN, ok := got.DirtyPages[pageID]
		if !ok {
			t.Errorf("DirtyPages missing pageID %d", pageID)
		}
		if gotLSN != recLSN {
			t.Errorf("DirtyPages[%d] = %d, want %d", pageID, gotLSN, recLSN)
		}
	}
}

func TestDeserializeTruncatedHeader(t *testing.T) {
	_, _, err := Deserialize(make([]byte, 10))
	if err == nil {
		t.Fatal("expected error for truncated header")
	}
}

func TestDeserializeTruncatedData(t *testing.T) {
	record := &LogRecord{
		LSN:        1,
		TxnID:      1,
		Type:       types.LogRecordInsert,
		AfterImage: []byte("test"),
	}
	buf := record.Serialize()
	// Truncate after header but before data
	_, _, err := Deserialize(buf[:logRecordHeaderSize])
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

func TestDeserializeTruncatedCheckpoint(t *testing.T) {
	record := &LogRecord{
		LSN:        1,
		TxnID:      0,
		Type:       types.LogRecordCheckpoint,
		ActiveTxns: []types.TxnID{1},
		DirtyPages: map[types.PageID]types.LSN{0: 1},
	}
	buf := record.Serialize()
	// Truncate checkpoint data
	_, _, err := Deserialize(buf[:logRecordHeaderSize+2])
	if err == nil {
		t.Fatal("expected error for truncated checkpoint length")
	}
}

func TestDeserializeTruncatedCLR(t *testing.T) {
	record := &LogRecord{
		LSN:         1,
		TxnID:       1,
		Type:        types.LogRecordCLR,
		UndoNextLSN: 5,
	}
	buf := record.Serialize()
	// Truncate CLR UndoNextLSN
	_, _, err := Deserialize(buf[:logRecordHeaderSize+2])
	if err == nil {
		t.Fatal("expected error for truncated CLR data")
	}
}

func TestLogRecordString(t *testing.T) {
	r := &LogRecord{
		LSN:     1,
		TxnID:   types.TxnID(42),
		Type:    types.LogRecordInsert,
		TableID: 1,
		RowID:   100,
	}
	s := r.String()
	if s == "" {
		t.Error("String() should not return empty string")
	}
}

func TestEmptyCheckpoint(t *testing.T) {
	record := &LogRecord{
		LSN:        1,
		TxnID:      0,
		Type:       types.LogRecordCheckpoint,
		ActiveTxns: []types.TxnID{},
		DirtyPages: map[types.PageID]types.LSN{},
	}
	buf := record.Serialize()
	got, _, err := Deserialize(buf)
	if err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}
	if len(got.ActiveTxns) != 0 {
		t.Errorf("ActiveTxns = %d, want 0", len(got.ActiveTxns))
	}
	if len(got.DirtyPages) != 0 {
		t.Errorf("DirtyPages = %d, want 0", len(got.DirtyPages))
	}
}
