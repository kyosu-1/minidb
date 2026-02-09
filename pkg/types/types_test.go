package types

import (
	"bytes"
	"testing"
)

func TestConstants(t *testing.T) {
	if PageSize != 4096 {
		t.Errorf("PageSize = %d, want 4096", PageSize)
	}
	if InvalidPageID != PageID(0xFFFFFFFF) {
		t.Errorf("InvalidPageID = %d, want 0xFFFFFFFF", InvalidPageID)
	}
	if InvalidTxnID != TxnID(0) {
		t.Errorf("InvalidTxnID = %d, want 0", InvalidTxnID)
	}
	if InvalidLSN != LSN(0) {
		t.Errorf("InvalidLSN = %d, want 0", InvalidLSN)
	}
	if MaxTxnID != TxnID(^uint64(0)) {
		t.Errorf("MaxTxnID = %d, want max uint64", MaxTxnID)
	}
}

func TestTxnStatusString(t *testing.T) {
	tests := []struct {
		status TxnStatus
		want   string
	}{
		{TxnStatusRunning, "RUNNING"},
		{TxnStatusCommitted, "COMMITTED"},
		{TxnStatusAborted, "ABORTED"},
		{TxnStatus(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("TxnStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
		}
	}
}

func TestLogRecordTypeString(t *testing.T) {
	tests := []struct {
		rt   LogRecordType
		want string
	}{
		{LogRecordBegin, "BEGIN"},
		{LogRecordCommit, "COMMIT"},
		{LogRecordAbort, "ABORT"},
		{LogRecordUpdate, "UPDATE"},
		{LogRecordInsert, "INSERT"},
		{LogRecordDelete, "DELETE"},
		{LogRecordCheckpoint, "CHECKPOINT"},
		{LogRecordCLR, "CLR"},
		{LogRecordType(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.rt.String(); got != tt.want {
			t.Errorf("LogRecordType(%d).String() = %q, want %q", tt.rt, got, tt.want)
		}
	}
}

func TestTupleSerializeDeserializeRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		tuple *Tuple
	}{
		{
			name: "basic tuple",
			tuple: &Tuple{
				XMin:    TxnID(1),
				XMax:    TxnID(2),
				Cid:     CommandID(3),
				TableID: 4,
				RowID:   5,
				Data:    []byte("hello world"),
			},
		},
		{
			name: "empty data",
			tuple: &Tuple{
				XMin:    TxnID(10),
				XMax:    InvalidTxnID,
				Cid:     CommandID(0),
				TableID: 1,
				RowID:   100,
				Data:    []byte{},
			},
		},
		{
			name: "large data",
			tuple: &Tuple{
				XMin:    TxnID(100),
				XMax:    TxnID(200),
				Cid:     CommandID(5),
				TableID: 42,
				RowID:   999,
				Data:    bytes.Repeat([]byte("x"), 1000),
			},
		},
		{
			name: "max values",
			tuple: &Tuple{
				XMin:    MaxTxnID,
				XMax:    MaxTxnID,
				Cid:     CommandID(^uint32(0)),
				TableID: ^uint32(0),
				RowID:   ^uint64(0),
				Data:    []byte{0xFF, 0x00, 0x01},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.tuple.Serialize()
			got, err := DeserializeTuple(buf)
			if err != nil {
				t.Fatalf("DeserializeTuple() error = %v", err)
			}
			if got.XMin != tt.tuple.XMin {
				t.Errorf("XMin = %d, want %d", got.XMin, tt.tuple.XMin)
			}
			if got.XMax != tt.tuple.XMax {
				t.Errorf("XMax = %d, want %d", got.XMax, tt.tuple.XMax)
			}
			if got.Cid != tt.tuple.Cid {
				t.Errorf("Cid = %d, want %d", got.Cid, tt.tuple.Cid)
			}
			if got.TableID != tt.tuple.TableID {
				t.Errorf("TableID = %d, want %d", got.TableID, tt.tuple.TableID)
			}
			if got.RowID != tt.tuple.RowID {
				t.Errorf("RowID = %d, want %d", got.RowID, tt.tuple.RowID)
			}
			if !bytes.Equal(got.Data, tt.tuple.Data) {
				t.Errorf("Data mismatch")
			}
		})
	}
}

func TestDeserializeTupleTooSmallHeader(t *testing.T) {
	_, err := DeserializeTuple(make([]byte, 35))
	if err == nil {
		t.Fatal("expected error for buffer smaller than header")
	}
}

func TestDeserializeTupleTruncatedData(t *testing.T) {
	tuple := &Tuple{
		XMin: 1, XMax: 0, Cid: 0, TableID: 1, RowID: 1,
		Data: []byte("hello"),
	}
	buf := tuple.Serialize()
	// Truncate: keep header but cut data short
	_, err := DeserializeTuple(buf[:37])
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

func TestTupleClone(t *testing.T) {
	original := &Tuple{
		XMin:    TxnID(1),
		XMax:    TxnID(2),
		Cid:     CommandID(3),
		TableID: 4,
		RowID:   5,
		Data:    []byte("original"),
	}

	clone := original.Clone()

	// Check values match
	if clone.XMin != original.XMin || clone.XMax != original.XMax ||
		clone.Cid != original.Cid || clone.TableID != original.TableID ||
		clone.RowID != original.RowID {
		t.Error("cloned metadata doesn't match original")
	}
	if !bytes.Equal(clone.Data, original.Data) {
		t.Error("cloned data doesn't match original")
	}

	// Verify deep copy: modifying clone shouldn't affect original
	clone.Data[0] = 'X'
	if original.Data[0] == 'X' {
		t.Error("Clone did not make a deep copy of Data")
	}
}

func TestTupleIsDeleted(t *testing.T) {
	tests := []struct {
		name    string
		xmax    TxnID
		deleted bool
	}{
		{"not deleted (InvalidTxnID)", InvalidTxnID, false},
		{"deleted (txn 1)", TxnID(1), true},
		{"deleted (txn max)", MaxTxnID, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple := &Tuple{XMax: tt.xmax}
			if got := tuple.IsDeleted(); got != tt.deleted {
				t.Errorf("IsDeleted() = %v, want %v", got, tt.deleted)
			}
		})
	}
}

func TestValueString(t *testing.T) {
	tests := []struct {
		name string
		val  Value
		want string
	}{
		{"null", Value{IsNull: true}, "NULL"},
		{"int", Value{Type: ValueTypeInt, IntVal: 42}, "42"},
		{"string", Value{Type: ValueTypeString, StrVal: "hello"}, "hello"},
		{"bool true", Value{Type: ValueTypeBool, BoolVal: true}, "true"},
		{"bool false", Value{Type: ValueTypeBool, BoolVal: false}, "false"},
		{"unknown type", Value{Type: ValueType(99)}, "NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.val.String(); got != tt.want {
				t.Errorf("Value.String() = %q, want %q", got, tt.want)
			}
		})
	}
}
