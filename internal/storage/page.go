// Package storage implements disk-based page storage.
package storage

import (
	"encoding/binary"
	"errors"
	"minidb/pkg/types"
)

const (
	PageSize       = 4096
	PageHeaderSize = 28

	// Page types
	PageTypeData    = 1
	PageTypeBTree   = 2
	PageTypeCatalog = 3
)

var (
	ErrPageFull     = errors.New("page is full")
	ErrSlotNotFound = errors.New("slot not found")
)

// Page represents a fixed-size disk page.
//
// Layout:
// +-------------------+
// | Header (24 bytes) |
// +-------------------+
// | Free Space        |
// |                   |
// +-------------------+
// | Tuple Data ←      |
// +-------------------+
// | Slot Array →      |
// +-------------------+
//
// Header format:
//   PageID (4) + PageType (1) + Reserved (3) + LSN (8) +
//   SlotCount (2) + FreeSpaceOffset (2) + FreeSpaceEnd (2) + NextPageID (4) + Reserved (2)
type Page struct {
	ID         types.PageID
	Type       uint8
	LSN        types.LSN
	NextPageID types.PageID
	IsDirty    bool
	PinCount   int
	Data       [PageSize]byte
}

// NewPage creates a new empty page.
func NewPage(id types.PageID, pageType uint8) *Page {
	p := &Page{
		ID:   id,
		Type: pageType,
	}
	p.init()
	return p
}

func (p *Page) init() {
	// Clear data
	for i := range p.Data {
		p.Data[i] = 0
	}

	// Write header
	binary.LittleEndian.PutUint32(p.Data[0:4], uint32(p.ID))
	p.Data[4] = p.Type
	binary.LittleEndian.PutUint64(p.Data[8:16], uint64(p.LSN))

	// Slot count = 0
	binary.LittleEndian.PutUint16(p.Data[16:18], 0)

	// Free space starts after header
	binary.LittleEndian.PutUint16(p.Data[18:20], PageHeaderSize)

	// Free space ends at page end (slot array grows backwards)
	binary.LittleEndian.PutUint16(p.Data[20:22], PageSize)

	// NextPageID = InvalidPageID
	p.NextPageID = types.InvalidPageID
	binary.LittleEndian.PutUint32(p.Data[22:26], uint32(types.InvalidPageID))
}

// Header accessors
func (p *Page) GetSlotCount() uint16 {
	return binary.LittleEndian.Uint16(p.Data[16:18])
}

func (p *Page) setSlotCount(count uint16) {
	binary.LittleEndian.PutUint16(p.Data[16:18], count)
}

func (p *Page) GetFreeSpaceOffset() uint16 {
	return binary.LittleEndian.Uint16(p.Data[18:20])
}

func (p *Page) setFreeSpaceOffset(offset uint16) {
	binary.LittleEndian.PutUint16(p.Data[18:20], offset)
}

func (p *Page) GetFreeSpaceEnd() uint16 {
	return binary.LittleEndian.Uint16(p.Data[20:22])
}

func (p *Page) setFreeSpaceEnd(end uint16) {
	binary.LittleEndian.PutUint16(p.Data[20:22], end)
}

func (p *Page) SetLSN(lsn types.LSN) {
	p.LSN = lsn
	binary.LittleEndian.PutUint64(p.Data[8:16], uint64(lsn))
}

func (p *Page) GetLSN() types.LSN {
	return types.LSN(binary.LittleEndian.Uint64(p.Data[8:16]))
}

func (p *Page) GetNextPageID() types.PageID {
	return types.PageID(binary.LittleEndian.Uint32(p.Data[22:26]))
}

func (p *Page) SetNextPageID(nextID types.PageID) {
	p.NextPageID = nextID
	binary.LittleEndian.PutUint32(p.Data[22:26], uint32(nextID))
	p.IsDirty = true
}

// Slot format: Offset (2 bytes) + Length (2 bytes)
const slotSize = 4

// getSlot returns the offset and length for a slot.
func (p *Page) getSlot(slotNum uint16) (offset uint16, length uint16) {
	slotPos := PageSize - (int(slotNum)+1)*slotSize
	offset = binary.LittleEndian.Uint16(p.Data[slotPos : slotPos+2])
	length = binary.LittleEndian.Uint16(p.Data[slotPos+2 : slotPos+4])
	return
}

// setSlot sets the offset and length for a slot.
func (p *Page) setSlot(slotNum uint16, offset, length uint16) {
	slotPos := PageSize - (int(slotNum)+1)*slotSize
	binary.LittleEndian.PutUint16(p.Data[slotPos:slotPos+2], offset)
	binary.LittleEndian.PutUint16(p.Data[slotPos+2:slotPos+4], length)
}

// FreeSpace returns the amount of free space available.
func (p *Page) FreeSpace() int {
	freeEnd := int(p.GetFreeSpaceEnd())
	freeOffset := int(p.GetFreeSpaceOffset())
	// Account for new slot entry
	return freeEnd - freeOffset - slotSize
}

// InsertTuple inserts a tuple into the page.
// Returns the slot number or error if page is full.
func (p *Page) InsertTuple(data []byte) (uint16, error) {
	dataLen := len(data)

	// Check if there's enough space
	if p.FreeSpace() < dataLen {
		return 0, ErrPageFull
	}

	// Allocate space from the end (growing backwards)
	freeEnd := p.GetFreeSpaceEnd()
	newEnd := freeEnd - uint16(dataLen)
	p.setFreeSpaceEnd(newEnd)

	// Copy data
	copy(p.Data[newEnd:freeEnd], data)

	// Add slot entry
	slotNum := p.GetSlotCount()
	p.setSlot(slotNum, newEnd, uint16(dataLen))
	p.setSlotCount(slotNum + 1)

	p.IsDirty = true
	return slotNum, nil
}

// GetTuple returns the tuple data at the given slot.
func (p *Page) GetTuple(slotNum uint16) ([]byte, error) {
	if slotNum >= p.GetSlotCount() {
		return nil, ErrSlotNotFound
	}

	offset, length := p.getSlot(slotNum)
	if length == 0 {
		return nil, ErrSlotNotFound // Deleted slot
	}

	data := make([]byte, length)
	copy(data, p.Data[offset:offset+length])
	return data, nil
}

// UpdateTuple updates the tuple at the given slot.
// If new data is larger, returns ErrPageFull.
func (p *Page) UpdateTuple(slotNum uint16, data []byte) error {
	if slotNum >= p.GetSlotCount() {
		return ErrSlotNotFound
	}

	offset, oldLen := p.getSlot(slotNum)
	newLen := uint16(len(data))

	if newLen <= oldLen {
		// Fits in existing space
		copy(p.Data[offset:], data)
		p.setSlot(slotNum, offset, newLen)
		p.IsDirty = true
		return nil
	}

	// Need to relocate - check free space
	if p.FreeSpace() < int(newLen) {
		return ErrPageFull
	}

	// Mark old slot as deleted (length = 0)
	p.setSlot(slotNum, offset, 0)

	// Allocate new space
	freeEnd := p.GetFreeSpaceEnd()
	newEnd := freeEnd - newLen
	p.setFreeSpaceEnd(newEnd)

	// Copy new data
	copy(p.Data[newEnd:freeEnd], data)

	// Update slot
	p.setSlot(slotNum, newEnd, newLen)
	p.IsDirty = true
	return nil
}

// DeleteTuple marks a tuple as deleted.
func (p *Page) DeleteTuple(slotNum uint16) error {
	if slotNum >= p.GetSlotCount() {
		return ErrSlotNotFound
	}

	offset, _ := p.getSlot(slotNum)
	p.setSlot(slotNum, offset, 0) // Length = 0 means deleted
	p.IsDirty = true
	return nil
}

// GetAllTuples returns all non-deleted tuples with their slot numbers.
func (p *Page) GetAllTuples() []struct {
	SlotNum uint16
	Data    []byte
} {
	var tuples []struct {
		SlotNum uint16
		Data    []byte
	}

	count := p.GetSlotCount()
	for i := uint16(0); i < count; i++ {
		offset, length := p.getSlot(i)
		if length > 0 {
			data := make([]byte, length)
			copy(data, p.Data[offset:offset+length])
			tuples = append(tuples, struct {
				SlotNum uint16
				Data    []byte
			}{i, data})
		}
	}

	return tuples
}

// Serialize returns the raw page data.
func (p *Page) Serialize() []byte {
	data := make([]byte, PageSize)
	copy(data, p.Data[:])
	return data
}

// Deserialize loads page data from bytes.
func (p *Page) Deserialize(data []byte) {
	copy(p.Data[:], data)
	p.ID = types.PageID(binary.LittleEndian.Uint32(p.Data[0:4]))
	p.Type = p.Data[4]
	p.LSN = types.LSN(binary.LittleEndian.Uint64(p.Data[8:16]))
	p.NextPageID = types.PageID(binary.LittleEndian.Uint32(p.Data[22:26]))
}
