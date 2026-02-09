package storage

import (
	"container/list"
	"fmt"
	"minidb/pkg/types"
	"sync"
)

// BufferPool manages page caching with LRU eviction.
type BufferPool struct {
	mu          sync.Mutex
	diskManager *DiskManager
	
	// Page cache
	pages    map[types.PageID]*Page
	capacity int
	
	// LRU tracking
	lruList  *list.List
	lruMap   map[types.PageID]*list.Element
	
	// Statistics
	hits   uint64
	misses uint64
}

// NewBufferPool creates a new buffer pool.
func NewBufferPool(diskManager *DiskManager, capacity int) *BufferPool {
	return &BufferPool{
		diskManager: diskManager,
		pages:       make(map[types.PageID]*Page),
		capacity:    capacity,
		lruList:     list.New(),
		lruMap:      make(map[types.PageID]*list.Element),
	}
}

// FetchPage retrieves a page, reading from disk if necessary.
func (bp *BufferPool) FetchPage(pageID types.PageID) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	// Check cache
	if page, ok := bp.pages[pageID]; ok {
		bp.hits++
		bp.touchLRU(pageID)
		page.PinCount++
		return page, nil
	}
	
	bp.misses++
	
	// Read from disk
	page, err := bp.diskManager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	
	// Make room if needed
	if len(bp.pages) >= bp.capacity {
		if err := bp.evictOne(); err != nil {
			return nil, fmt.Errorf("eviction failed: %w", err)
		}
	}
	
	// Add to cache
	bp.pages[pageID] = page
	bp.addToLRU(pageID)
	page.PinCount = 1
	
	return page, nil
}

// NewPage creates a new page and adds it to the buffer pool.
func (bp *BufferPool) NewPage(pageType uint8) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	// Allocate on disk
	pageID, err := bp.diskManager.AllocatePage()
	if err != nil {
		return nil, err
	}
	
	// Make room if needed
	if len(bp.pages) >= bp.capacity {
		if err := bp.evictOne(); err != nil {
			return nil, fmt.Errorf("eviction failed: %w", err)
		}
	}
	
	// Create page
	page := NewPage(pageID, pageType)
	page.IsDirty = true
	page.PinCount = 1
	
	bp.pages[pageID] = page
	bp.addToLRU(pageID)
	
	return page, nil
}

// UnpinPage decrements the pin count for a page.
func (bp *BufferPool) UnpinPage(pageID types.PageID, isDirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	if page, ok := bp.pages[pageID]; ok {
		if isDirty {
			page.IsDirty = true
		}
		if page.PinCount > 0 {
			page.PinCount--
		}
	}
}

// FlushPage writes a page to disk.
func (bp *BufferPool) FlushPage(pageID types.PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	page, ok := bp.pages[pageID]
	if !ok {
		return nil // Not in buffer pool
	}
	
	if page.IsDirty {
		if err := bp.diskManager.WritePage(page); err != nil {
			return err
		}
		page.IsDirty = false
	}
	
	return nil
}

// FlushAllPages writes all dirty pages to disk.
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	for _, page := range bp.pages {
		if page.IsDirty {
			if err := bp.diskManager.WritePage(page); err != nil {
				return err
			}
			page.IsDirty = false
		}
	}
	
	return bp.diskManager.Sync()
}

// evictOne evicts one page from the buffer pool.
// Must be called with lock held.
func (bp *BufferPool) evictOne() error {
	// Find LRU page that's not pinned
	for e := bp.lruList.Back(); e != nil; e = e.Prev() {
		pageID := e.Value.(types.PageID)
		page := bp.pages[pageID]
		
		if page.PinCount == 0 {
			// Flush if dirty
			if page.IsDirty {
				if err := bp.diskManager.WritePage(page); err != nil {
					return err
				}
			}
			
			// Remove from cache
			delete(bp.pages, pageID)
			bp.lruList.Remove(e)
			delete(bp.lruMap, pageID)
			
			return nil
		}
	}
	
	return fmt.Errorf("all pages are pinned, cannot evict")
}

// addToLRU adds a page to the LRU list (most recently used).
func (bp *BufferPool) addToLRU(pageID types.PageID) {
	e := bp.lruList.PushFront(pageID)
	bp.lruMap[pageID] = e
}

// touchLRU moves a page to the front (most recently used).
func (bp *BufferPool) touchLRU(pageID types.PageID) {
	if e, ok := bp.lruMap[pageID]; ok {
		bp.lruList.MoveToFront(e)
	}
}

// GetPage returns a page without pinning (for read-only access).
func (bp *BufferPool) GetPage(pageID types.PageID) *Page {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.pages[pageID]
}

// GetDirtyPages returns all dirty pages for checkpointing.
func (bp *BufferPool) GetDirtyPages() map[types.PageID]types.LSN {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	dirty := make(map[types.PageID]types.LSN)
	for pageID, page := range bp.pages {
		if page.IsDirty {
			dirty[pageID] = page.LSN
		}
	}
	return dirty
}

// Stats returns buffer pool statistics.
func (bp *BufferPool) Stats() (hits, misses uint64, cached int) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.hits, bp.misses, len(bp.pages)
}

// MarkDirty marks a page as dirty.
func (bp *BufferPool) MarkDirty(pageID types.PageID) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	if page, ok := bp.pages[pageID]; ok {
		page.IsDirty = true
	}
}

// SetPageLSN sets the LSN for a page.
func (bp *BufferPool) SetPageLSN(pageID types.PageID, lsn types.LSN) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	if page, ok := bp.pages[pageID]; ok {
		page.SetLSN(lsn)
		page.IsDirty = true
	}
}

// GetPageLSN returns the LSN for a page.
func (bp *BufferPool) GetPageLSN(pageID types.PageID) types.LSN {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	if page, ok := bp.pages[pageID]; ok {
		return page.GetLSN()
	}
	return types.InvalidLSN
}
