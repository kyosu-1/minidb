package types

import (
	"bytes"
	"encoding/json"
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

// --- SerializeRow / DeserializeRow tests ---

func TestSerializeDeserializeRowRoundTrip(t *testing.T) {
	schema := &Schema{
		TableName: "users",
		Columns: []Column{
			{Name: "id", Type: ValueTypeInt},
			{Name: "name", Type: ValueTypeString},
			{Name: "active", Type: ValueTypeBool},
		},
	}

	values := map[string]Value{
		"id":     {Type: ValueTypeInt, IntVal: 1},
		"name":   {Type: ValueTypeString, StrVal: "Alice"},
		"active": {Type: ValueTypeBool, BoolVal: true},
	}

	data, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	// Expected: 1 byte bitmap + 8 (int64) + 2+5 (string) + 1 (bool) = 17 bytes
	if len(data) != 17 {
		t.Errorf("expected 17 bytes, got %d", len(data))
	}

	got, err := DeserializeRow(schema, data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if got["id"].IntVal != 1 {
		t.Errorf("id: expected 1, got %d", got["id"].IntVal)
	}
	if got["name"].StrVal != "Alice" {
		t.Errorf("name: expected Alice, got %s", got["name"].StrVal)
	}
	if got["active"].BoolVal != true {
		t.Errorf("active: expected true, got %v", got["active"].BoolVal)
	}
}

func TestRowNullHandling(t *testing.T) {
	schema := &Schema{
		TableName: "t",
		Columns: []Column{
			{Name: "a", Type: ValueTypeInt},
			{Name: "b", Type: ValueTypeString},
			{Name: "c", Type: ValueTypeBool},
		},
	}

	// Partial NULLs: only "a" is set
	values := map[string]Value{
		"a": {Type: ValueTypeInt, IntVal: 42},
	}

	data, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	got, err := DeserializeRow(schema, data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if got["a"].IntVal != 42 || got["a"].IsNull {
		t.Errorf("a: expected 42 (non-null), got %v", got["a"])
	}
	if !got["b"].IsNull {
		t.Errorf("b: expected NULL, got %v", got["b"])
	}
	if !got["c"].IsNull {
		t.Errorf("c: expected NULL, got %v", got["c"])
	}
}

func TestRowAllNulls(t *testing.T) {
	schema := &Schema{
		TableName: "t",
		Columns: []Column{
			{Name: "a", Type: ValueTypeInt},
			{Name: "b", Type: ValueTypeString},
		},
	}

	values := map[string]Value{}

	data, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	// Only bitmap, no column data
	if len(data) != 1 {
		t.Errorf("expected 1 byte (bitmap only), got %d", len(data))
	}

	got, err := DeserializeRow(schema, data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if !got["a"].IsNull {
		t.Errorf("a: expected NULL")
	}
	if !got["b"].IsNull {
		t.Errorf("b: expected NULL")
	}
}

func TestRowEmptyString(t *testing.T) {
	schema := &Schema{
		TableName: "t",
		Columns: []Column{
			{Name: "s", Type: ValueTypeString},
		},
	}

	values := map[string]Value{
		"s": {Type: ValueTypeString, StrVal: ""},
	}

	data, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	got, err := DeserializeRow(schema, data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if got["s"].IsNull {
		t.Error("expected non-null empty string, got NULL")
	}
	if got["s"].StrVal != "" {
		t.Errorf("expected empty string, got %q", got["s"].StrVal)
	}
}

func TestRowNegativeInt(t *testing.T) {
	schema := &Schema{
		TableName: "t",
		Columns: []Column{
			{Name: "n", Type: ValueTypeInt},
		},
	}

	values := map[string]Value{
		"n": {Type: ValueTypeInt, IntVal: -9999},
	}

	data, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	got, err := DeserializeRow(schema, data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if got["n"].IntVal != -9999 {
		t.Errorf("expected -9999, got %d", got["n"].IntVal)
	}
}

func TestRowMultiByteBitmap(t *testing.T) {
	// 9 columns requires 2 bytes for the null bitmap
	columns := make([]Column, 9)
	values := make(map[string]Value)
	for i := 0; i < 9; i++ {
		name := string(rune('a' + i))
		columns[i] = Column{Name: name, Type: ValueTypeInt}
		values[name] = Value{Type: ValueTypeInt, IntVal: int64(i + 1)}
	}

	schema := &Schema{TableName: "t", Columns: columns}

	data, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	// 2 bytes bitmap + 9*8 bytes = 74
	if len(data) != 74 {
		t.Errorf("expected 74 bytes, got %d", len(data))
	}

	got, err := DeserializeRow(schema, data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	for i := 0; i < 9; i++ {
		name := string(rune('a' + i))
		if got[name].IntVal != int64(i+1) {
			t.Errorf("column %s: expected %d, got %d", name, i+1, got[name].IntVal)
		}
	}
}

func TestRowMultiByteBitmapWithNulls(t *testing.T) {
	// 9 columns, columns 0 and 8 are NULL
	columns := make([]Column, 9)
	values := make(map[string]Value)
	for i := 0; i < 9; i++ {
		name := string(rune('a' + i))
		columns[i] = Column{Name: name, Type: ValueTypeInt}
		if i != 0 && i != 8 {
			values[name] = Value{Type: ValueTypeInt, IntVal: int64(i)}
		}
	}

	schema := &Schema{TableName: "t", Columns: columns}

	data, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	got, err := DeserializeRow(schema, data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if !got["a"].IsNull {
		t.Error("column a should be NULL")
	}
	if !got["i"].IsNull {
		t.Error("column i should be NULL")
	}
	if got["b"].IntVal != 1 {
		t.Errorf("column b: expected 1, got %d", got["b"].IntVal)
	}
}

func TestRowTruncatedData(t *testing.T) {
	schema := &Schema{
		TableName: "t",
		Columns: []Column{
			{Name: "id", Type: ValueTypeInt},
		},
	}

	// Too short for bitmap
	_, err := DeserializeRow(schema, []byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}

	// Bitmap present but INT data truncated
	_, err = DeserializeRow(schema, []byte{0x00, 0x01, 0x02})
	if err == nil {
		t.Error("expected error for truncated INT data")
	}
}

func TestRowBoolFalse(t *testing.T) {
	schema := &Schema{
		TableName: "t",
		Columns: []Column{
			{Name: "flag", Type: ValueTypeBool},
		},
	}

	values := map[string]Value{
		"flag": {Type: ValueTypeBool, BoolVal: false},
	}

	data, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	got, err := DeserializeRow(schema, data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if got["flag"].IsNull {
		t.Error("expected non-null false, got NULL")
	}
	if got["flag"].BoolVal != false {
		t.Errorf("expected false, got %v", got["flag"].BoolVal)
	}
}

func TestRowSizeComparisonVsJSON(t *testing.T) {
	schema := &Schema{
		TableName: "users",
		Columns: []Column{
			{Name: "id", Type: ValueTypeInt},
			{Name: "name", Type: ValueTypeString},
			{Name: "active", Type: ValueTypeBool},
		},
	}

	values := map[string]Value{
		"id":     {Type: ValueTypeInt, IntVal: 1},
		"name":   {Type: ValueTypeString, StrVal: "Alice"},
		"active": {Type: ValueTypeBool, BoolVal: true},
	}

	binData, err := SerializeRow(schema, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}

	jsonData, err := json.Marshal(values)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	t.Logf("Binary size: %d bytes", len(binData))
	t.Logf("JSON size:   %d bytes", len(jsonData))
	t.Logf("Ratio:       %.1fx smaller", float64(len(jsonData))/float64(len(binData)))

	if len(binData) >= len(jsonData) {
		t.Errorf("binary (%d) should be smaller than JSON (%d)", len(binData), len(jsonData))
	}
}
