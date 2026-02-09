package index

import (
	"bytes"
	"fmt"
	"minidb/internal/storage"
	"minidb/pkg/types"
	"path/filepath"
	"testing"
)

func newTestBTree(t *testing.T, keySize int) *BTree {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	dm, err := storage.NewDiskManager(path)
	if err != nil {
		t.Fatalf("NewDiskManager() error = %v", err)
	}
	bp := storage.NewBufferPool(dm, 200)
	bt, err := NewBTree(bp, keySize)
	if err != nil {
		t.Fatalf("NewBTree() error = %v", err)
	}
	return bt
}

func TestInsertAndSearch(t *testing.T) {
	bt := newTestBTree(t, 8)

	key := []byte("testkey1")
	rid := RID{PageID: types.PageID(1), SlotNum: 0, TableID: 1}

	if err := bt.Insert(key, rid); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	got, found := bt.Search(key)
	if !found {
		t.Fatal("Search() returned false")
	}
	if got.PageID != rid.PageID || got.SlotNum != rid.SlotNum || got.TableID != rid.TableID {
		t.Errorf("Search() = %v, want %v", got, rid)
	}
}

func TestSearchNotFound(t *testing.T) {
	bt := newTestBTree(t, 8)

	_, found := bt.Search([]byte("missing"))
	if found {
		t.Error("Search() should return false for non-existent key")
	}
}

func TestInsertMultipleAndSearchAll(t *testing.T) {
	bt := newTestBTree(t, 8)

	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, k := range keys {
		rid := RID{PageID: types.PageID(i), SlotNum: uint16(i), TableID: 1}
		if err := bt.Insert([]byte(k), rid); err != nil {
			t.Fatalf("Insert(%q) error = %v", k, err)
		}
	}

	for i, k := range keys {
		got, found := bt.Search([]byte(k))
		if !found {
			t.Errorf("Search(%q) not found", k)
			continue
		}
		if got.PageID != types.PageID(i) {
			t.Errorf("Search(%q).PageID = %d, want %d", k, got.PageID, i)
		}
	}
}

func TestDelete(t *testing.T) {
	bt := newTestBTree(t, 8)

	key := []byte("delkey")
	rid := RID{PageID: 1, SlotNum: 0, TableID: 1}
	bt.Insert(key, rid)

	if !bt.Delete(key) {
		t.Error("Delete() returned false")
	}

	_, found := bt.Search(key)
	if found {
		t.Error("Search() should return false after delete")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	bt := newTestBTree(t, 8)

	if bt.Delete([]byte("missing")) {
		t.Error("Delete() should return false for non-existent key")
	}
}

func TestScanAll(t *testing.T) {
	bt := newTestBTree(t, 8)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		rid := RID{PageID: types.PageID(i), SlotNum: uint16(i), TableID: 1}
		bt.Insert(key, rid)
	}

	results := bt.ScanAll()
	if len(results) != 10 {
		t.Errorf("ScanAll() = %d, want 10", len(results))
	}
}

func TestLeafSplit(t *testing.T) {
	bt := newTestBTree(t, 8)

	// Insert enough keys to trigger a leaf split
	for i := 0; i < bt.order; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		rid := RID{PageID: types.PageID(i), SlotNum: 0, TableID: 1}
		if err := bt.Insert(key, rid); err != nil {
			t.Fatalf("Insert(%d) error = %v", i, err)
		}
	}

	// All keys should still be searchable
	for i := 0; i < bt.order; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		_, found := bt.Search(key)
		if !found {
			t.Errorf("key%04d not found after split", i)
		}
	}
}

func TestLargeInsert(t *testing.T) {
	bt := newTestBTree(t, 8)

	count := 200
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		rid := RID{PageID: types.PageID(i), SlotNum: 0, TableID: 1}
		if err := bt.Insert(key, rid); err != nil {
			t.Fatalf("Insert(%d) error = %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		_, found := bt.Search(key)
		if !found {
			t.Errorf("key%04d not found after large insert", i)
		}
	}

	// ScanAll should return all
	results := bt.ScanAll()
	if len(results) != count {
		t.Errorf("ScanAll() = %d, want %d", len(results), count)
	}
}

func TestRIDSerializeDeserialize(t *testing.T) {
	rid := RID{
		PageID:  types.PageID(42),
		SlotNum: 7,
		TableID: 99,
	}

	buf := rid.Serialize()
	got := DeserializeRID(buf)

	if got.PageID != rid.PageID {
		t.Errorf("PageID = %d, want %d", got.PageID, rid.PageID)
	}
	if got.SlotNum != rid.SlotNum {
		t.Errorf("SlotNum = %d, want %d", got.SlotNum, rid.SlotNum)
	}
	if got.TableID != rid.TableID {
		t.Errorf("TableID = %d, want %d", got.TableID, rid.TableID)
	}
}

func TestDuplicateKeyUpdate(t *testing.T) {
	bt := newTestBTree(t, 8)

	key := []byte("dup_key")
	rid1 := RID{PageID: 1, SlotNum: 0, TableID: 1}
	rid2 := RID{PageID: 2, SlotNum: 1, TableID: 1}

	bt.Insert(key, rid1)
	bt.Insert(key, rid2) // Should update existing

	got, found := bt.Search(key)
	if !found {
		t.Fatal("key not found after update")
	}
	if got.PageID != rid2.PageID {
		t.Errorf("PageID = %d, want %d (updated value)", got.PageID, rid2.PageID)
	}
}

func TestGetRootPageID(t *testing.T) {
	bt := newTestBTree(t, 8)

	rootID := bt.GetRootPageID()
	if rootID == types.InvalidPageID {
		t.Error("root page ID should be valid")
	}
}

func TestEncodeKeyIntOrdering(t *testing.T) {
	vals := []int64{-100, -1, 0, 1, 100, 1000}
	var prev []byte
	for _, v := range vals {
		key := EncodeKey(types.Value{Type: types.ValueTypeInt, IntVal: v}, 64)
		if prev != nil && bytes.Compare(prev, key) >= 0 {
			t.Errorf("EncodeKey(%d) should be > EncodeKey of previous value, but byte order is wrong", v)
		}
		prev = key
	}
}

func TestEncodeKeyStringOrdering(t *testing.T) {
	vals := []string{"alice", "bob", "charlie"}
	var prev []byte
	for _, v := range vals {
		key := EncodeKey(types.Value{Type: types.ValueTypeString, StrVal: v}, 64)
		if prev != nil && bytes.Compare(prev, key) >= 0 {
			t.Errorf("EncodeKey(%q) should be > EncodeKey of previous value", v)
		}
		prev = key
	}
}

func TestNormalizeKey(t *testing.T) {
	bt := newTestBTree(t, 8)

	// Short key should be padded
	short := bt.normalizeKey([]byte("hi"))
	if len(short) != 8 {
		t.Errorf("normalized short key len = %d, want 8", len(short))
	}

	// Long key should be truncated
	long := bt.normalizeKey([]byte("this is a very long key"))
	if len(long) != 8 {
		t.Errorf("normalized long key len = %d, want 8", len(long))
	}
	if !bytes.Equal(long, []byte("this is ")) {
		t.Errorf("truncated key = %q, want %q", long, "this is ")
	}
}
