package storage

import (
	"encoding/binary"
	"fmt"
	"minidb/pkg/types"
	"os"
	"sync"
)

// DiskManager handles reading and writing pages to disk.
type DiskManager struct {
	mu       sync.Mutex
	file     *os.File
	filePath string
	numPages uint32
}

const (
	diskHeaderSize = 16 // Magic(8) + Version(4) + NumPages(4)
	diskMagic      = uint64(0x4D494E4944425044) // "MINIDBPD"
	diskVersion    = uint32(1)
)

// NewDiskManager creates or opens a database file.
func NewDiskManager(path string) (*DiskManager, error) {
	dm := &DiskManager{
		filePath: path,
	}

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Create new file
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to create data file: %w", err)
		}
		dm.file = file
		dm.numPages = 0

		// Write header
		if err := dm.writeHeader(); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		// Open existing file
		file, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open data file: %w", err)
		}
		dm.file = file

		// Read header
		if err := dm.readHeader(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return dm, nil
}

func (dm *DiskManager) writeHeader() error {
	header := make([]byte, diskHeaderSize)
	binary.LittleEndian.PutUint64(header[0:8], diskMagic)
	binary.LittleEndian.PutUint32(header[8:12], diskVersion)
	binary.LittleEndian.PutUint32(header[12:16], dm.numPages)

	_, err := dm.file.WriteAt(header, 0)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	return dm.file.Sync()
}

func (dm *DiskManager) readHeader() error {
	header := make([]byte, diskHeaderSize)
	n, err := dm.file.ReadAt(header, 0)
	if err != nil || n < diskHeaderSize {
		return fmt.Errorf("failed to read header: %w", err)
	}

	magic := binary.LittleEndian.Uint64(header[0:8])
	if magic != diskMagic {
		return fmt.Errorf("invalid data file magic")
	}

	version := binary.LittleEndian.Uint32(header[8:12])
	if version != diskVersion {
		return fmt.Errorf("unsupported data file version: %d", version)
	}

	dm.numPages = binary.LittleEndian.Uint32(header[12:16])
	return nil
}

func (dm *DiskManager) updateNumPages() error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, dm.numPages)
	_, err := dm.file.WriteAt(buf, 12)
	return err
}

// pageOffset returns the file offset for a page.
func (dm *DiskManager) pageOffset(pageID types.PageID) int64 {
	return int64(diskHeaderSize) + int64(pageID)*int64(PageSize)
}

// ReadPage reads a page from disk.
func (dm *DiskManager) ReadPage(pageID types.PageID) (*Page, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if uint32(pageID) >= dm.numPages {
		return nil, fmt.Errorf("page %d does not exist", pageID)
	}

	data := make([]byte, PageSize)
	offset := dm.pageOffset(pageID)

	n, err := dm.file.ReadAt(data, offset)
	if err != nil || n != PageSize {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	page := &Page{}
	page.Deserialize(data)
	return page, nil
}

// WritePage writes a page to disk.
func (dm *DiskManager) WritePage(page *Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := dm.pageOffset(page.ID)
	data := page.Serialize()

	n, err := dm.file.WriteAt(data, offset)
	if err != nil || n != PageSize {
		return fmt.Errorf("failed to write page %d: %w", page.ID, err)
	}

	return nil
}

// AllocatePage allocates a new page and returns its ID.
func (dm *DiskManager) AllocatePage() (types.PageID, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	pageID := types.PageID(dm.numPages)
	dm.numPages++

	// Update header
	if err := dm.updateNumPages(); err != nil {
		dm.numPages--
		return 0, err
	}

	// Initialize empty page on disk
	page := NewPage(pageID, PageTypeData)
	offset := dm.pageOffset(pageID)

	_, err := dm.file.WriteAt(page.Serialize(), offset)
	if err != nil {
		dm.numPages--
		dm.updateNumPages()
		return 0, err
	}

	return pageID, nil
}

// Sync flushes all pending writes to disk.
func (dm *DiskManager) Sync() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.file.Sync()
}

// GetNumPages returns the total number of pages.
func (dm *DiskManager) GetNumPages() uint32 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.numPages
}

// Close closes the disk manager.
func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.file.Close()
}
