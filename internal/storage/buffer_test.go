package storage

import (
	"minidb/pkg/types"
	"path/filepath"
	"testing"
)

func newTestBufferPool(t *testing.T, capacity int) *BufferPool {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	dm, err := NewDiskManager(path)
	if err != nil {
		t.Fatalf("NewDiskManager() error = %v", err)
	}
	return NewBufferPool(dm, capacity)
}

func TestBufferPoolNewPage(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	page, err := bp.NewPage(PageTypeData)
	if err != nil {
		t.Fatalf("NewPage() error = %v", err)
	}
	if page.PinCount != 1 {
		t.Errorf("PinCount = %d, want 1", page.PinCount)
	}
	if !page.IsDirty {
		t.Error("new page should be dirty")
	}
}

func TestBufferPoolFetchPageCacheHit(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	page, _ := bp.NewPage(PageTypeData)
	pageID := page.ID
	bp.UnpinPage(pageID, true)

	// Fetch should hit cache
	fetched, err := bp.FetchPage(pageID)
	if err != nil {
		t.Fatalf("FetchPage() error = %v", err)
	}
	if fetched.ID != pageID {
		t.Errorf("fetched page ID = %d, want %d", fetched.ID, pageID)
	}

	hits, misses, _ := bp.Stats()
	if hits != 1 {
		t.Errorf("hits = %d, want 1", hits)
	}
	if misses != 0 {
		t.Errorf("misses = %d, want 0", misses)
	}
}

func TestBufferPoolFetchPageCacheMiss(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	dm, _ := NewDiskManager(path)

	// Write a page to disk first
	id, _ := dm.AllocatePage()
	page := NewPage(id, PageTypeData)
	page.InsertTuple([]byte("from disk"))
	dm.WritePage(page)

	// New buffer pool (empty cache)
	bp := NewBufferPool(dm, 10)

	fetched, err := bp.FetchPage(id)
	if err != nil {
		t.Fatalf("FetchPage() error = %v", err)
	}
	if fetched.PinCount != 1 {
		t.Errorf("PinCount = %d, want 1", fetched.PinCount)
	}

	_, misses, _ := bp.Stats()
	if misses != 1 {
		t.Errorf("misses = %d, want 1", misses)
	}
}

func TestBufferPoolUnpin(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	page, _ := bp.NewPage(PageTypeData)
	pageID := page.ID

	if page.PinCount != 1 {
		t.Errorf("initial PinCount = %d, want 1", page.PinCount)
	}

	bp.UnpinPage(pageID, false)
	if page.PinCount != 0 {
		t.Errorf("after unpin PinCount = %d, want 0", page.PinCount)
	}

	// Unpin when already 0 should stay at 0
	bp.UnpinPage(pageID, false)
	if page.PinCount != 0 {
		t.Errorf("after double unpin PinCount = %d, want 0", page.PinCount)
	}
}

func TestBufferPoolUnpinDirtyFlag(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	page, _ := bp.NewPage(PageTypeData)
	pageID := page.ID
	page.IsDirty = false // Reset

	bp.UnpinPage(pageID, true)
	if !page.IsDirty {
		t.Error("page should be dirty after UnpinPage with isDirty=true")
	}
}

func TestBufferPoolEviction(t *testing.T) {
	bp := newTestBufferPool(t, 3)

	// Fill buffer pool
	pages := make([]types.PageID, 3)
	for i := 0; i < 3; i++ {
		p, err := bp.NewPage(PageTypeData)
		if err != nil {
			t.Fatalf("NewPage(%d) error = %v", i, err)
		}
		pages[i] = p.ID
		bp.UnpinPage(p.ID, true)
	}

	// Allocating a 4th page should evict one
	p4, err := bp.NewPage(PageTypeData)
	if err != nil {
		t.Fatalf("NewPage(4th) error = %v", err)
	}
	bp.UnpinPage(p4.ID, true)

	_, _, cached := bp.Stats()
	if cached != 3 {
		t.Errorf("cached = %d, want 3", cached)
	}
}

func TestBufferPoolEvictionPinnedPageNotEvicted(t *testing.T) {
	bp := newTestBufferPool(t, 2)

	// Create and keep first page pinned
	p1, _ := bp.NewPage(PageTypeData)
	// Don't unpin p1

	// Create and unpin second page
	p2, _ := bp.NewPage(PageTypeData)
	bp.UnpinPage(p2.ID, true)

	// Third page should evict p2 (not p1 which is pinned)
	_, err := bp.NewPage(PageTypeData)
	if err != nil {
		t.Fatalf("NewPage(3rd) error = %v", err)
	}

	// p1 should still be in buffer pool
	got := bp.GetPage(p1.ID)
	if got == nil {
		t.Error("pinned page was evicted")
	}
}

func TestBufferPoolEvictionAllPinned(t *testing.T) {
	bp := newTestBufferPool(t, 2)

	// Pin all pages
	bp.NewPage(PageTypeData) // pinned
	bp.NewPage(PageTypeData) // pinned

	// Should fail since all pages are pinned
	_, err := bp.NewPage(PageTypeData)
	if err == nil {
		t.Fatal("expected error when all pages are pinned")
	}
}

func TestBufferPoolEvictionDirtyPageFlushed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	dm, _ := NewDiskManager(path)
	bp := NewBufferPool(dm, 2)

	// Create dirty page
	p1, _ := bp.NewPage(PageTypeData)
	p1.InsertTuple([]byte("dirty data"))
	p1ID := p1.ID
	bp.UnpinPage(p1ID, true)

	// Create second page
	p2, _ := bp.NewPage(PageTypeData)
	bp.UnpinPage(p2.ID, true)

	// Third page triggers eviction, dirty page should be flushed
	bp.NewPage(PageTypeData)

	// Read from disk to verify flush
	readPage, err := dm.ReadPage(p1ID)
	if err != nil {
		t.Fatalf("ReadPage() error = %v", err)
	}
	data, _ := readPage.GetTuple(0)
	if string(data) != "dirty data" {
		t.Errorf("evicted dirty page data = %q, want %q", data, "dirty data")
	}
}

func TestBufferPoolLRUOrder(t *testing.T) {
	bp := newTestBufferPool(t, 3)

	// Create 3 pages
	p1, _ := bp.NewPage(PageTypeData)
	bp.UnpinPage(p1.ID, true)
	p2, _ := bp.NewPage(PageTypeData)
	bp.UnpinPage(p2.ID, true)
	p3, _ := bp.NewPage(PageTypeData)
	bp.UnpinPage(p3.ID, true)

	// Access p1 again to make it recently used
	bp.FetchPage(p1.ID)
	bp.UnpinPage(p1.ID, false)

	// Add p4 - should evict p2 (least recently used)
	p4, err := bp.NewPage(PageTypeData)
	if err != nil {
		t.Fatalf("NewPage(4th) error = %v", err)
	}
	bp.UnpinPage(p4.ID, true)

	// p1 should still be cached (recently accessed)
	if bp.GetPage(p1.ID) == nil {
		t.Error("recently used page was evicted")
	}
	// p2 should be evicted
	if bp.GetPage(p2.ID) != nil {
		t.Error("LRU page was not evicted")
	}
}

func TestBufferPoolFlushPage(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	page, _ := bp.NewPage(PageTypeData)
	page.InsertTuple([]byte("flush test"))
	pageID := page.ID

	if err := bp.FlushPage(pageID); err != nil {
		t.Fatalf("FlushPage() error = %v", err)
	}
	if page.IsDirty {
		t.Error("page should not be dirty after flush")
	}
}

func TestBufferPoolFlushAllPages(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	for i := 0; i < 3; i++ {
		p, _ := bp.NewPage(PageTypeData)
		p.InsertTuple([]byte("data"))
		bp.UnpinPage(p.ID, true)
	}

	if err := bp.FlushAllPages(); err != nil {
		t.Fatalf("FlushAllPages() error = %v", err)
	}

	dirty := bp.GetDirtyPages()
	if len(dirty) != 0 {
		t.Errorf("dirty pages after FlushAllPages = %d, want 0", len(dirty))
	}
}

func TestBufferPoolGetDirtyPages(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	p1, _ := bp.NewPage(PageTypeData)
	bp.UnpinPage(p1.ID, true)

	p2, _ := bp.NewPage(PageTypeData)
	p2.IsDirty = false
	bp.UnpinPage(p2.ID, false)

	dirty := bp.GetDirtyPages()
	if _, ok := dirty[p1.ID]; !ok {
		t.Error("dirty page p1 not in GetDirtyPages")
	}
}

func TestBufferPoolSetGetPageLSN(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	page, _ := bp.NewPage(PageTypeData)
	pageID := page.ID
	bp.UnpinPage(pageID, true)

	bp.SetPageLSN(pageID, types.LSN(42))

	got := bp.GetPageLSN(pageID)
	if got != types.LSN(42) {
		t.Errorf("GetPageLSN() = %d, want 42", got)
	}

	// Non-existent page returns InvalidLSN
	got = bp.GetPageLSN(types.PageID(9999))
	if got != types.InvalidLSN {
		t.Errorf("GetPageLSN(missing) = %d, want InvalidLSN", got)
	}
}

func TestBufferPoolMarkDirty(t *testing.T) {
	bp := newTestBufferPool(t, 10)

	page, _ := bp.NewPage(PageTypeData)
	page.IsDirty = false
	bp.MarkDirty(page.ID)
	if !page.IsDirty {
		t.Error("page should be dirty after MarkDirty")
	}
}
