package storage

import (
	"minidb/pkg/types"
	"os"
	"path/filepath"
	"testing"
)

func newTestDiskManager(t *testing.T) (*DiskManager, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	dm, err := NewDiskManager(path)
	if err != nil {
		t.Fatalf("NewDiskManager() error = %v", err)
	}
	return dm, path
}

func TestNewDiskManagerCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	dm, err := NewDiskManager(path)
	if err != nil {
		t.Fatalf("NewDiskManager() error = %v", err)
	}
	defer dm.Close()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("database file not created")
	}
	if dm.GetNumPages() != 0 {
		t.Errorf("NumPages = %d, want 0", dm.GetNumPages())
	}
}

func TestDiskManagerInvalidMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.db")

	// Write invalid data
	os.WriteFile(path, make([]byte, diskHeaderSize), 0644)

	_, err := NewDiskManager(path)
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestAllocatePage(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Close()

	for i := 0; i < 3; i++ {
		id, err := dm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error = %v", err)
		}
		if id != types.PageID(i) {
			t.Errorf("AllocatePage() = %d, want %d", id, i)
		}
	}
	if dm.GetNumPages() != 3 {
		t.Errorf("NumPages = %d, want 3", dm.GetNumPages())
	}
}

func TestWriteReadPageRoundTrip(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Close()

	id, _ := dm.AllocatePage()
	page := NewPage(id, PageTypeData)
	page.InsertTuple([]byte("hello"))
	page.SetLSN(types.LSN(42))

	if err := dm.WritePage(page); err != nil {
		t.Fatalf("WritePage() error = %v", err)
	}

	got, err := dm.ReadPage(id)
	if err != nil {
		t.Fatalf("ReadPage() error = %v", err)
	}
	if got.ID != id {
		t.Errorf("ID = %d, want %d", got.ID, id)
	}
	if got.GetLSN() != types.LSN(42) {
		t.Errorf("LSN = %d, want 42", got.GetLSN())
	}
	if got.GetSlotCount() != 1 {
		t.Errorf("SlotCount = %d, want 1", got.GetSlotCount())
	}
}

func TestReadPageOutOfRange(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Close()

	_, err := dm.ReadPage(types.PageID(0))
	if err == nil {
		t.Fatal("expected error for reading non-existent page")
	}

	dm.AllocatePage()
	_, err = dm.ReadPage(types.PageID(5))
	if err == nil {
		t.Fatal("expected error for reading out-of-range page")
	}
}

func TestCloseReopenPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create and write
	dm, err := NewDiskManager(path)
	if err != nil {
		t.Fatalf("NewDiskManager() error = %v", err)
	}

	id, _ := dm.AllocatePage()
	page := NewPage(id, PageTypeData)
	page.InsertTuple([]byte("persistent"))
	dm.WritePage(page)
	dm.Close()

	// Reopen and read
	dm2, err := NewDiskManager(path)
	if err != nil {
		t.Fatalf("Reopen NewDiskManager() error = %v", err)
	}
	defer dm2.Close()

	if dm2.GetNumPages() != 1 {
		t.Errorf("NumPages after reopen = %d, want 1", dm2.GetNumPages())
	}

	got, err := dm2.ReadPage(id)
	if err != nil {
		t.Fatalf("ReadPage() after reopen error = %v", err)
	}
	data, err := got.GetTuple(0)
	if err != nil {
		t.Fatalf("GetTuple() error = %v", err)
	}
	if string(data) != "persistent" {
		t.Errorf("data = %q, want %q", data, "persistent")
	}
}
