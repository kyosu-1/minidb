package storage

import (
	"bytes"
	"minidb/pkg/types"
	"testing"
)

func TestNewPageInitialState(t *testing.T) {
	p := NewPage(0, PageTypeData)

	if p.ID != 0 {
		t.Errorf("ID = %d, want 0", p.ID)
	}
	if p.Type != PageTypeData {
		t.Errorf("Type = %d, want %d", p.Type, PageTypeData)
	}
	if p.GetSlotCount() != 0 {
		t.Errorf("SlotCount = %d, want 0", p.GetSlotCount())
	}
	if p.GetFreeSpaceOffset() != PageHeaderSize {
		t.Errorf("FreeSpaceOffset = %d, want %d", p.GetFreeSpaceOffset(), PageHeaderSize)
	}
	if p.GetFreeSpaceEnd() != PageSize {
		t.Errorf("FreeSpaceEnd = %d, want %d", p.GetFreeSpaceEnd(), PageSize)
	}
	if p.GetNextPageID() != types.InvalidPageID {
		t.Errorf("NextPageID = %d, want InvalidPageID", p.GetNextPageID())
	}
}

func TestInsertTuple(t *testing.T) {
	p := NewPage(0, PageTypeData)
	data := []byte("hello")

	slot, err := p.InsertTuple(data)
	if err != nil {
		t.Fatalf("InsertTuple() error = %v", err)
	}
	if slot != 0 {
		t.Errorf("slot = %d, want 0", slot)
	}
	if p.GetSlotCount() != 1 {
		t.Errorf("SlotCount = %d, want 1", p.GetSlotCount())
	}
	if !p.IsDirty {
		t.Error("page should be dirty after insert")
	}
}

func TestInsertMultipleTuples(t *testing.T) {
	p := NewPage(0, PageTypeData)

	for i := 0; i < 5; i++ {
		slot, err := p.InsertTuple([]byte("data"))
		if err != nil {
			t.Fatalf("InsertTuple(%d) error = %v", i, err)
		}
		if slot != uint16(i) {
			t.Errorf("slot = %d, want %d", slot, i)
		}
	}
	if p.GetSlotCount() != 5 {
		t.Errorf("SlotCount = %d, want 5", p.GetSlotCount())
	}
}

func TestInsertTuplePageFull(t *testing.T) {
	p := NewPage(0, PageTypeData)
	// Fill the page with large tuples
	bigData := make([]byte, 500)
	for {
		_, err := p.InsertTuple(bigData)
		if err != nil {
			if err != ErrPageFull {
				t.Fatalf("expected ErrPageFull, got %v", err)
			}
			break
		}
	}
}

func TestGetTuple(t *testing.T) {
	p := NewPage(0, PageTypeData)
	data := []byte("test data")

	slot, _ := p.InsertTuple(data)
	got, err := p.GetTuple(slot)
	if err != nil {
		t.Fatalf("GetTuple() error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("GetTuple() = %q, want %q", got, data)
	}
}

func TestGetTupleInvalidSlot(t *testing.T) {
	p := NewPage(0, PageTypeData)

	_, err := p.GetTuple(0)
	if err != ErrSlotNotFound {
		t.Errorf("expected ErrSlotNotFound, got %v", err)
	}

	p.InsertTuple([]byte("data"))
	_, err = p.GetTuple(1)
	if err != ErrSlotNotFound {
		t.Errorf("expected ErrSlotNotFound for slot 1, got %v", err)
	}
}

func TestUpdateTupleSameSize(t *testing.T) {
	p := NewPage(0, PageTypeData)
	slot, _ := p.InsertTuple([]byte("hello"))

	err := p.UpdateTuple(slot, []byte("world"))
	if err != nil {
		t.Fatalf("UpdateTuple() error = %v", err)
	}

	got, _ := p.GetTuple(slot)
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("after update got %q, want %q", got, "world")
	}
}

func TestUpdateTupleSmaller(t *testing.T) {
	p := NewPage(0, PageTypeData)
	slot, _ := p.InsertTuple([]byte("hello world"))

	err := p.UpdateTuple(slot, []byte("hi"))
	if err != nil {
		t.Fatalf("UpdateTuple() error = %v", err)
	}

	got, _ := p.GetTuple(slot)
	if !bytes.Equal(got, []byte("hi")) {
		t.Errorf("after update got %q, want %q", got, "hi")
	}
}

func TestUpdateTupleLargerRelocate(t *testing.T) {
	p := NewPage(0, PageTypeData)
	slot, _ := p.InsertTuple([]byte("hi"))

	err := p.UpdateTuple(slot, []byte("hello world, this is longer"))
	if err != nil {
		t.Fatalf("UpdateTuple() error = %v", err)
	}

	got, _ := p.GetTuple(slot)
	if !bytes.Equal(got, []byte("hello world, this is longer")) {
		t.Errorf("after update got %q, want expected", got)
	}
}

func TestUpdateTupleTooLarge(t *testing.T) {
	p := NewPage(0, PageTypeData)
	slot, _ := p.InsertTuple([]byte("hi"))

	huge := make([]byte, PageSize)
	err := p.UpdateTuple(slot, huge)
	if err != ErrPageFull {
		t.Errorf("expected ErrPageFull, got %v", err)
	}
}

func TestUpdateTupleInvalidSlot(t *testing.T) {
	p := NewPage(0, PageTypeData)
	err := p.UpdateTuple(0, []byte("data"))
	if err != ErrSlotNotFound {
		t.Errorf("expected ErrSlotNotFound, got %v", err)
	}
}

func TestDeleteTuple(t *testing.T) {
	p := NewPage(0, PageTypeData)
	slot, _ := p.InsertTuple([]byte("data"))

	err := p.DeleteTuple(slot)
	if err != nil {
		t.Fatalf("DeleteTuple() error = %v", err)
	}

	_, err = p.GetTuple(slot)
	if err != ErrSlotNotFound {
		t.Errorf("expected ErrSlotNotFound after delete, got %v", err)
	}
}

func TestDeleteTupleInvalidSlot(t *testing.T) {
	p := NewPage(0, PageTypeData)
	err := p.DeleteTuple(0)
	if err != ErrSlotNotFound {
		t.Errorf("expected ErrSlotNotFound, got %v", err)
	}
}

func TestGetAllTuples(t *testing.T) {
	p := NewPage(0, PageTypeData)
	p.InsertTuple([]byte("a"))
	p.InsertTuple([]byte("b"))
	slot2, _ := p.InsertTuple([]byte("c"))
	p.InsertTuple([]byte("d"))

	// Delete slot 2
	p.DeleteTuple(slot2)

	tuples := p.GetAllTuples()
	if len(tuples) != 3 {
		t.Fatalf("GetAllTuples() returned %d tuples, want 3", len(tuples))
	}

	// Verify deleted slot is skipped
	for _, tp := range tuples {
		if tp.SlotNum == slot2 {
			t.Error("deleted slot should not appear in GetAllTuples")
		}
	}
}

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	p := NewPage(42, PageTypeBTree)
	p.InsertTuple([]byte("data1"))
	p.InsertTuple([]byte("data2"))
	p.SetLSN(types.LSN(100))
	p.SetNextPageID(types.PageID(7))

	serialized := p.Serialize()

	p2 := &Page{}
	p2.Deserialize(serialized)

	if p2.ID != 42 {
		t.Errorf("ID = %d, want 42", p2.ID)
	}
	if p2.Type != PageTypeBTree {
		t.Errorf("Type = %d, want %d", p2.Type, PageTypeBTree)
	}
	if p2.LSN != types.LSN(100) {
		t.Errorf("LSN = %d, want 100", p2.LSN)
	}
	if p2.NextPageID != types.PageID(7) {
		t.Errorf("NextPageID = %d, want 7", p2.NextPageID)
	}
	if p2.GetSlotCount() != 2 {
		t.Errorf("SlotCount = %d, want 2", p2.GetSlotCount())
	}

	got, _ := p2.GetTuple(0)
	if !bytes.Equal(got, []byte("data1")) {
		t.Errorf("tuple 0 = %q, want %q", got, "data1")
	}
	got, _ = p2.GetTuple(1)
	if !bytes.Equal(got, []byte("data2")) {
		t.Errorf("tuple 1 = %q, want %q", got, "data2")
	}
}

func TestSetGetLSN(t *testing.T) {
	p := NewPage(0, PageTypeData)
	p.SetLSN(types.LSN(999))
	if p.GetLSN() != types.LSN(999) {
		t.Errorf("GetLSN() = %d, want 999", p.GetLSN())
	}
}

func TestSetGetNextPageID(t *testing.T) {
	p := NewPage(0, PageTypeData)
	p.SetNextPageID(types.PageID(42))
	if p.GetNextPageID() != types.PageID(42) {
		t.Errorf("GetNextPageID() = %d, want 42", p.GetNextPageID())
	}
	if !p.IsDirty {
		t.Error("page should be dirty after SetNextPageID")
	}
}

func TestFreeSpace(t *testing.T) {
	p := NewPage(0, PageTypeData)
	initialFree := p.FreeSpace()
	// Initial free = PageSize - PageHeaderSize - slotSize (one slot reservation)
	expected := PageSize - PageHeaderSize - slotSize
	if initialFree != expected {
		t.Errorf("initial FreeSpace = %d, want %d", initialFree, expected)
	}

	data := make([]byte, 100)
	p.InsertTuple(data)
	afterInsert := p.FreeSpace()
	// After insert: free should decrease by data size + slot entry
	if afterInsert >= initialFree {
		t.Errorf("FreeSpace should decrease after insert: before=%d, after=%d", initialFree, afterInsert)
	}
}
