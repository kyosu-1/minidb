package storage

import (
	"bytes"
	"minidb/pkg/types"
	"path/filepath"
	"testing"
)

func newTestHeapSetup(t *testing.T) (*BufferPool, *DiskManager) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	dm, err := NewDiskManager(path)
	if err != nil {
		t.Fatalf("NewDiskManager() error = %v", err)
	}
	bp := NewBufferPool(dm, 100)
	return bp, dm
}

func TestTableHeapInsertGet(t *testing.T) {
	bp, _ := newTestHeapSetup(t)

	th, err := NewTableHeap(bp, 1)
	if err != nil {
		t.Fatalf("NewTableHeap() error = %v", err)
	}

	tuple := &types.Tuple{
		XMin:    types.TxnID(1),
		XMax:    types.InvalidTxnID,
		Cid:     0,
		TableID: 1,
		RowID:   1,
		Data:    []byte("hello"),
	}

	pageID, slotNum, err := th.Insert(tuple)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	got, err := th.Get(pageID, slotNum)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got.Data, tuple.Data) {
		t.Errorf("Data = %q, want %q", got.Data, tuple.Data)
	}
	if got.XMin != tuple.XMin {
		t.Errorf("XMin = %d, want %d", got.XMin, tuple.XMin)
	}
}

func TestTableHeapUpdate(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	th, _ := NewTableHeap(bp, 1)

	tuple := &types.Tuple{
		XMin: 1, XMax: types.InvalidTxnID, TableID: 1, RowID: 1,
		Data: []byte("original"),
	}
	pageID, slotNum, _ := th.Insert(tuple)

	updated := &types.Tuple{
		XMin: 1, XMax: types.InvalidTxnID, TableID: 1, RowID: 1,
		Data: []byte("updated"),
	}
	if err := th.Update(pageID, slotNum, updated); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, _ := th.Get(pageID, slotNum)
	if !bytes.Equal(got.Data, []byte("updated")) {
		t.Errorf("after update Data = %q, want %q", got.Data, "updated")
	}
}

func TestTableHeapDelete(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	th, _ := NewTableHeap(bp, 1)

	tuple := &types.Tuple{
		XMin: 1, XMax: types.InvalidTxnID, TableID: 1, RowID: 1,
		Data: []byte("delete me"),
	}
	pageID, slotNum, _ := th.Insert(tuple)

	if err := th.Delete(pageID, slotNum); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	_, err := th.Get(pageID, slotNum)
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestTableHeapScan(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	th, _ := NewTableHeap(bp, 1)

	for i := 0; i < 5; i++ {
		tuple := &types.Tuple{
			XMin: 1, XMax: types.InvalidTxnID, TableID: 1, RowID: uint64(i + 1),
			Data: []byte{byte(i)},
		}
		th.Insert(tuple)
	}

	results, err := th.Scan()
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(results) != 5 {
		t.Errorf("Scan() returned %d tuples, want 5", len(results))
	}
}

func TestTableHeapPageOverflow(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	th, _ := NewTableHeap(bp, 1)

	// Insert tuples with large data to force page overflow
	largeData := bytes.Repeat([]byte("x"), 500)
	insertedCount := 0
	for i := 0; i < 20; i++ {
		tuple := &types.Tuple{
			XMin: 1, XMax: types.InvalidTxnID, TableID: 1, RowID: uint64(i + 1),
			Data: largeData,
		}
		_, _, err := th.Insert(tuple)
		if err != nil {
			t.Fatalf("Insert(%d) error = %v", i, err)
		}
		insertedCount++
	}

	// Should have multiple pages
	meta := th.GetMeta()
	if meta.FirstPage == meta.LastPage {
		t.Error("expected multiple pages after overflow")
	}

	// Scan should return all tuples
	results, err := th.Scan()
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(results) != insertedCount {
		t.Errorf("Scan() returned %d, want %d", len(results), insertedCount)
	}
}

func TestTableHeapMultiPageScan(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	th, _ := NewTableHeap(bp, 1)

	// Insert enough to span multiple pages
	data := bytes.Repeat([]byte("a"), 300)
	count := 30
	for i := 0; i < count; i++ {
		tuple := &types.Tuple{
			XMin: 1, XMax: types.InvalidTxnID, TableID: 1, RowID: uint64(i + 1),
			Data: data,
		}
		th.Insert(tuple)
	}

	results, err := th.Scan()
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(results) != count {
		t.Errorf("multi-page Scan() = %d, want %d", len(results), count)
	}
}

// --- Catalog tests ---

func TestCatalogCreateTable(t *testing.T) {
	bp, _ := newTestHeapSetup(t)

	catalog, err := NewCatalog(bp)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	schema := &types.Schema{
		TableName: "users",
		Columns: []types.Column{
			{Name: "id", Type: types.ValueTypeInt, Nullable: false},
			{Name: "name", Type: types.ValueTypeString, Nullable: true},
		},
	}

	tableID, err := catalog.CreateTable(schema)
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	if tableID == 0 {
		t.Error("tableID should not be 0")
	}

	got := catalog.GetSchema("users")
	if got == nil {
		t.Fatal("GetSchema() returned nil")
	}
	if got.TableName != "users" {
		t.Errorf("TableName = %q, want %q", got.TableName, "users")
	}
	if len(got.Columns) != 2 {
		t.Errorf("Columns count = %d, want 2", len(got.Columns))
	}
}

func TestCatalogDuplicateTable(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	catalog, _ := NewCatalog(bp)

	schema := &types.Schema{TableName: "users", Columns: []types.Column{{Name: "id", Type: types.ValueTypeInt}}}
	catalog.CreateTable(schema)

	_, err := catalog.CreateTable(schema)
	if err == nil {
		t.Fatal("expected error for duplicate table name")
	}
}

func TestCatalogSerializeDeserialize(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	catalog, _ := NewCatalog(bp)

	schema := &types.Schema{
		TableName: "products",
		Columns: []types.Column{
			{Name: "id", Type: types.ValueTypeInt, Nullable: false},
			{Name: "name", Type: types.ValueTypeString, Nullable: true},
			{Name: "active", Type: types.ValueTypeBool, Nullable: false},
		},
	}
	catalog.CreateTable(schema)
	catalogPageID := catalog.GetCatalogPageID()

	// Load catalog from same buffer pool (simulating reopen)
	catalog2, err := LoadCatalog(bp, catalogPageID)
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}

	got := catalog2.GetSchema("products")
	if got == nil {
		t.Fatal("schema not found after load")
	}
	if len(got.Columns) != 3 {
		t.Errorf("Columns = %d, want 3", len(got.Columns))
	}
	if got.Columns[0].Name != "id" || got.Columns[0].Type != types.ValueTypeInt {
		t.Errorf("Column 0 = %v, want {id, Int}", got.Columns[0])
	}
}

func TestCatalogIndexRoot(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	catalog, _ := NewCatalog(bp)

	schema := &types.Schema{TableName: "t", Columns: []types.Column{{Name: "id", Type: types.ValueTypeInt}}}
	tableID, _ := catalog.CreateTable(schema)

	// Initially no index
	_, ok := catalog.GetIndexRoot(tableID)
	if ok {
		t.Error("expected no index root initially")
	}

	// Set index root
	catalog.SetIndexRoot(tableID, types.PageID(42))

	root, ok := catalog.GetIndexRoot(tableID)
	if !ok {
		t.Fatal("index root not found")
	}
	if root != types.PageID(42) {
		t.Errorf("IndexRoot = %d, want 42", root)
	}
}

func TestCatalogGetAllTables(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	catalog, _ := NewCatalog(bp)

	tables := catalog.GetAllTables()
	if len(tables) != 0 {
		t.Errorf("initial GetAllTables() = %d, want 0", len(tables))
	}

	catalog.CreateTable(&types.Schema{TableName: "a", Columns: []types.Column{{Name: "id", Type: types.ValueTypeInt}}})
	catalog.CreateTable(&types.Schema{TableName: "b", Columns: []types.Column{{Name: "id", Type: types.ValueTypeInt}}})

	tables = catalog.GetAllTables()
	if len(tables) != 2 {
		t.Errorf("GetAllTables() = %d, want 2", len(tables))
	}
}

func TestTableHeapGetMeta(t *testing.T) {
	bp, _ := newTestHeapSetup(t)
	th, _ := NewTableHeap(bp, 7)

	meta := th.GetMeta()
	if meta.TableID != 7 {
		t.Errorf("TableID = %d, want 7", meta.TableID)
	}
	if meta.FirstPage == types.InvalidPageID {
		t.Error("FirstPage should be valid")
	}
}
