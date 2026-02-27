// Package engine provides the main database engine.
package engine

import (
	"fmt"
	"minidb/internal/index"
	"minidb/internal/sql"
	"minidb/internal/storage"
	"minidb/internal/txn"
	"minidb/internal/wal"
	"minidb/pkg/types"
	"os"
	"path/filepath"
)

// Engine represents the database engine.
type Engine struct {
	dataDir     string
	walWriter   *wal.Writer
	diskManager *storage.DiskManager
	bufferPool  *storage.BufferPool
	catalog     *storage.Catalog
	txnManager  *txn.Manager
	executor    *sql.Executor
	indexes     map[uint32]*index.BTree // tableID -> index
}

// Config holds engine configuration.
type Config struct {
	DataDir        string
	BufferPoolSize int
}

const (
	defaultBufferPoolSize = 1024 // 1024 pages = 4MB
	metaFileName          = "minidb.meta"
)

// New creates a new database engine.
func New(cfg Config) (*Engine, error) {
	if cfg.BufferPoolSize == 0 {
		cfg.BufferPoolSize = defaultBufferPoolSize
	}

	// Create data directory if needed
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	walPath := filepath.Join(cfg.DataDir, "wal.log")
	dataPath := filepath.Join(cfg.DataDir, "data.db")
	metaPath := filepath.Join(cfg.DataDir, metaFileName)

	// Initialize WAL writer
	walWriter, err := wal.NewWriter(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL writer: %w", err)
	}

	// Initialize disk manager
	diskManager, err := storage.NewDiskManager(dataPath)
	if err != nil {
		walWriter.Close()
		return nil, fmt.Errorf("failed to create disk manager: %w", err)
	}

	// Initialize buffer pool
	bufferPool := storage.NewBufferPool(diskManager, cfg.BufferPoolSize)

	// Initialize or load catalog
	var catalog *storage.Catalog
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		// New database
		catalog, err = storage.NewCatalog(bufferPool)
		if err != nil {
			diskManager.Close()
			walWriter.Close()
			return nil, fmt.Errorf("failed to create catalog: %w", err)
		}
		// Save meta
		if err := saveMeta(metaPath, catalog.GetCatalogPageID()); err != nil {
			diskManager.Close()
			walWriter.Close()
			return nil, err
		}
	} else {
		// Load existing database
		catalogPageID, err := loadMeta(metaPath)
		if err != nil {
			diskManager.Close()
			walWriter.Close()
			return nil, err
		}
		catalog, err = storage.LoadCatalog(bufferPool, catalogPageID)
		if err != nil {
			diskManager.Close()
			walWriter.Close()
			return nil, fmt.Errorf("failed to load catalog: %w", err)
		}
	}

	txnManager := txn.NewManager(walWriter)

	// Create executor
	executor := sql.NewExecutor(txnManager, walWriter)
	executor.SetStorage(catalog, bufferPool)

	e := &Engine{
		dataDir:     cfg.DataDir,
		walWriter:   walWriter,
		diskManager: diskManager,
		bufferPool:  bufferPool,
		catalog:     catalog,
		txnManager:  txnManager,
		executor:    executor,
		indexes:     make(map[uint32]*index.BTree),
	}

	// Load existing indexes
	e.loadIndexes()

	// Share indexes with executor
	e.executor.SetIndexes(e.indexes)

	// Perform recovery if needed
	if err := e.recover(); err != nil {
		e.Close()
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	return e, nil
}

func saveMeta(path string, catalogPageID types.PageID) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%d\n", catalogPageID)
	return err
}

func loadMeta(path string) (types.PageID, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var pageID types.PageID
	_, err = fmt.Fscanf(f, "%d\n", &pageID)
	return pageID, err
}

func (e *Engine) loadIndexes() {
	for _, tableName := range e.catalog.GetAllTables() {
		tableID, _ := e.catalog.GetTableID(tableName)
		if rootPageID, ok := e.catalog.GetIndexRoot(tableID); ok && rootPageID != types.InvalidPageID {
			e.indexes[tableID] = index.LoadBTree(e.bufferPool, rootPageID, 64)
		}
	}
}

// recover performs crash recovery.
func (e *Engine) recover() error {
	walPath := filepath.Join(e.dataDir, "wal.log")

	// Check if WAL exists
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		return nil // Nothing to recover
	}

	fmt.Println("Performing crash recovery...")

	rm := wal.NewRecoveryManager(walPath, e.walWriter)

	// Set recovery callbacks
	rm.SetCallbacks(
		func(record *wal.LogRecord) error {
			return e.applyRedo(record)
		},
		func(record *wal.LogRecord) error {
			return e.applyUndo(record)
		},
	)

	// Set pageLSN callback for redo skip check
	rm.SetPageLSNCallback(func(pageID types.PageID) types.LSN {
		page, err := e.bufferPool.FetchPage(pageID)
		if err != nil {
			return types.InvalidLSN
		}
		lsn := page.GetLSN()
		e.bufferPool.UnpinPage(pageID, false)
		return lsn
	})

	if err := rm.Recover(); err != nil {
		return err
	}

	// Flush all dirty pages after recovery
	if err := e.bufferPool.FlushAllPages(); err != nil {
		return fmt.Errorf("failed to flush pages after recovery: %w", err)
	}

	// Update transaction manager's next ID using max from WAL
	maxTxnID := e.walWriter.GetMaxTxnID()
	att := rm.GetActiveTxnTable()
	for txnID := range att {
		if txnID > maxTxnID {
			maxTxnID = txnID
		}
	}
	if maxTxnID > 0 {
		e.txnManager.SetNextTxnID(maxTxnID + 1)
	}

	return nil
}

func (e *Engine) applyRedo(record *wal.LogRecord) error {
	switch record.Type {
	case types.LogRecordInsert:
		// Redo insert: write tuple to page
		page, err := e.bufferPool.FetchPage(record.PageID)
		if err != nil {
			return err
		}
		if err := e.redoWriteSlot(page, record.SlotNum, record.AfterImage); err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("redo insert: %w", err)
		}
		page.SetLSN(record.LSN)
		e.bufferPool.UnpinPage(record.PageID, true)

	case types.LogRecordUpdate:
		// Redo update step 1: write new version at new location
		page, err := e.bufferPool.FetchPage(record.PageID)
		if err != nil {
			return err
		}
		if err := e.redoWriteSlot(page, record.SlotNum, record.AfterImage); err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("redo update new version: %w", err)
		}
		page.SetLSN(record.LSN)
		e.bufferPool.UnpinPage(record.PageID, true)

		// Redo update step 2: set XMax on old version
		// Old location is encoded in RowID: PageID<<16 | SlotNum
		oldPageID := types.PageID(record.RowID >> 16)
		oldSlotNum := uint16(record.RowID & 0xFFFF)
		if oldPageID != record.PageID || oldSlotNum != record.SlotNum {
			oldPage, err := e.bufferPool.FetchPage(oldPageID)
			if err != nil {
				return fmt.Errorf("redo update fetch old page: %w", err)
			}
			tupleData, err := oldPage.GetTuple(oldSlotNum)
			if err == nil {
				tuple, err := types.DeserializeTuple(tupleData)
				if err == nil && tuple.XMax == types.InvalidTxnID {
					tuple.XMax = record.TxnID
					oldPage.UpdateTuple(oldSlotNum, tuple.Serialize())
				}
			}
			oldPage.SetLSN(record.LSN)
			e.bufferPool.UnpinPage(oldPageID, true)
		}

	case types.LogRecordDelete:
		// Redo delete: set XMax on tuple
		page, err := e.bufferPool.FetchPage(record.PageID)
		if err != nil {
			return err
		}
		tupleData, err := page.GetTuple(record.SlotNum)
		if err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("redo delete get tuple: %w", err)
		}
		tuple, err := types.DeserializeTuple(tupleData)
		if err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("redo delete deserialize: %w", err)
		}
		tuple.XMax = record.TxnID
		if err := page.UpdateTuple(record.SlotNum, tuple.Serialize()); err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("redo delete update: %w", err)
		}
		page.SetLSN(record.LSN)
		e.bufferPool.UnpinPage(record.PageID, true)

	case types.LogRecordCLR:
		// Redo CLR: apply the compensation
		page, err := e.bufferPool.FetchPage(record.PageID)
		if err != nil {
			return err
		}
		if record.AfterImage != nil {
			if err := e.redoWriteSlot(page, record.SlotNum, record.AfterImage); err != nil {
				e.bufferPool.UnpinPage(record.PageID, true)
				return fmt.Errorf("redo CLR: %w", err)
			}
		}
		page.SetLSN(record.LSN)
		e.bufferPool.UnpinPage(record.PageID, true)
	}

	return nil
}

// redoWriteSlot writes data to a page slot during redo.
// If the slot already exists, it updates in place (idempotent redo).
// If the slot doesn't exist yet (page wasn't flushed before crash), it inserts.
func (e *Engine) redoWriteSlot(page *storage.Page, slotNum uint16, data []byte) error {
	if slotNum < page.GetSlotCount() {
		return page.UpdateTuple(slotNum, data)
	}
	newSlot, err := page.InsertTuple(data)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}
	if newSlot != slotNum {
		return fmt.Errorf("slot mismatch: expected %d, got %d", slotNum, newSlot)
	}
	return nil
}

func (e *Engine) applyUndo(record *wal.LogRecord) error {
	switch record.Type {
	case types.LogRecordInsert:
		// Undo insert: delete tuple from page
		page, err := e.bufferPool.FetchPage(record.PageID)
		if err != nil {
			return err
		}
		if err := page.DeleteTuple(record.SlotNum); err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("undo insert: %w", err)
		}
		e.bufferPool.UnpinPage(record.PageID, true)

	case types.LogRecordUpdate:
		// Undo update step 1: delete new version
		page, err := e.bufferPool.FetchPage(record.PageID)
		if err != nil {
			return err
		}
		if err := page.DeleteTuple(record.SlotNum); err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("undo update delete new version: %w", err)
		}
		e.bufferPool.UnpinPage(record.PageID, true)

		// Undo update step 2: clear XMax on old version
		oldPageID := types.PageID(record.RowID >> 16)
		oldSlotNum := uint16(record.RowID & 0xFFFF)
		oldPage, err := e.bufferPool.FetchPage(oldPageID)
		if err != nil {
			return fmt.Errorf("undo update fetch old page: %w", err)
		}
		tupleData, err := oldPage.GetTuple(oldSlotNum)
		if err != nil {
			e.bufferPool.UnpinPage(oldPageID, true)
			return fmt.Errorf("undo update get old tuple: %w", err)
		}
		tuple, err := types.DeserializeTuple(tupleData)
		if err != nil {
			e.bufferPool.UnpinPage(oldPageID, true)
			return fmt.Errorf("undo update deserialize: %w", err)
		}
		tuple.XMax = types.InvalidTxnID
		if err := oldPage.UpdateTuple(oldSlotNum, tuple.Serialize()); err != nil {
			e.bufferPool.UnpinPage(oldPageID, true)
			return fmt.Errorf("undo update restore old tuple: %w", err)
		}
		e.bufferPool.UnpinPage(oldPageID, true)

	case types.LogRecordDelete:
		// Undo delete: clear XMax
		page, err := e.bufferPool.FetchPage(record.PageID)
		if err != nil {
			return err
		}
		tupleData, err := page.GetTuple(record.SlotNum)
		if err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("undo delete get tuple: %w", err)
		}
		tuple, err := types.DeserializeTuple(tupleData)
		if err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("undo delete deserialize: %w", err)
		}
		tuple.XMax = types.InvalidTxnID
		if err := page.UpdateTuple(record.SlotNum, tuple.Serialize()); err != nil {
			e.bufferPool.UnpinPage(record.PageID, true)
			return fmt.Errorf("undo delete update: %w", err)
		}
		e.bufferPool.UnpinPage(record.PageID, true)
	}

	return nil
}

// Execute executes a SQL statement.
func (e *Engine) Execute(sqlStr string) *sql.Result {
	return e.executor.Execute(sqlStr)
}

// CreateIndex creates a B-Tree index on the specified column.
func (e *Engine) CreateIndex(tableName, columnName string) error {
	tableID, ok := e.catalog.GetTableID(tableName)
	if !ok {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Check if index already exists
	if _, exists := e.indexes[tableID]; exists {
		return fmt.Errorf("index already exists for table %s", tableName)
	}

	// Verify column exists
	schema := e.catalog.GetSchema(tableName)
	columnFound := false
	for _, col := range schema.Columns {
		if col.Name == columnName {
			columnFound = true
			break
		}
	}
	if !columnFound {
		return fmt.Errorf("column %s not found in table %s", columnName, tableName)
	}

	// Create B-Tree
	btree, err := index.NewBTree(e.bufferPool, 64)
	if err != nil {
		return err
	}

	// Index existing data
	heap := e.catalog.GetTableHeap(tableID)
	tuples, err := heap.Scan()
	if err != nil {
		return err
	}

	for _, t := range tuples {
		// Skip dead tuples
		if t.Tuple.IsDeleted() {
			continue
		}

		rowData, err := types.DeserializeRow(schema, t.Tuple.Data)
		if err != nil {
			continue
		}

		val, ok := rowData[columnName]
		if !ok {
			continue
		}

		key := index.EncodeKey(val, 64)
		rid := index.RID{
			PageID:  t.PageID,
			SlotNum: t.SlotNum,
			TableID: tableID,
		}

		btree.Insert(key, rid)
	}

	e.indexes[tableID] = btree
	e.catalog.SetIndexRoot(tableID, btree.GetRootPageID(), columnName)

	return nil
}

// Checkpoint creates a checkpoint.
func (e *Engine) Checkpoint() error {
	// Get dirty pages BEFORE flushing
	dirtyPages := e.bufferPool.GetDirtyPages()
	activeTxns := e.txnManager.GetActiveTxns()

	// Flush WAL first
	if err := e.walWriter.Flush(); err != nil {
		return err
	}

	// Then flush dirty pages
	if err := e.bufferPool.FlushAllPages(); err != nil {
		return err
	}

	// Write checkpoint record
	_, err := e.walWriter.LogCheckpoint(activeTxns, dirtyPages)
	return err
}

// Close shuts down the engine.
func (e *Engine) Close() error {
	// Flush any pending writes
	if err := e.walWriter.Flush(); err != nil {
		return err
	}

	// Flush all dirty pages
	if err := e.bufferPool.FlushAllPages(); err != nil {
		return err
	}

	// Sync disk
	if err := e.diskManager.Sync(); err != nil {
		return err
	}

	// Close files
	if err := e.diskManager.Close(); err != nil {
		return err
	}

	return e.walWriter.Close()
}

// Stats returns engine statistics.
func (e *Engine) Stats() map[string]interface{} {
	hits, misses, cached := e.bufferPool.Stats()
	hitRate := float64(0)
	if hits+misses > 0 {
		hitRate = float64(hits) / float64(hits+misses) * 100
	}

	return map[string]interface{}{
		"wal_current_lsn":    e.walWriter.GetCurrentLSN(),
		"wal_flushed_lsn":    e.walWriter.GetFlushedLSN(),
		"active_txns":        len(e.txnManager.GetActiveTxns()),
		"buffer_pool_hits":   hits,
		"buffer_pool_misses": misses,
		"buffer_pool_cached": cached,
		"buffer_hit_rate":    fmt.Sprintf("%.1f%%", hitRate),
		"disk_pages":         e.diskManager.GetNumPages(),
		"tables":             len(e.catalog.GetAllTables()),
	}
}

// GetCatalog returns the catalog (for executor).
func (e *Engine) GetCatalog() *storage.Catalog {
	return e.catalog
}

// GetBufferPool returns the buffer pool (for executor).
func (e *Engine) GetBufferPool() *storage.BufferPool {
	return e.bufferPool
}

// GetIndex returns the index for a table.
func (e *Engine) GetIndex(tableID uint32) *index.BTree {
	return e.indexes[tableID]
}

// VacuumResult holds the result of a VACUUM operation.
type VacuumResult struct {
	Tables []VacuumTableStats
}

// TotalRemoved returns the total number of dead tuples removed.
func (r *VacuumResult) TotalRemoved() int {
	total := 0
	for _, t := range r.Tables {
		total += t.TuplesRemoved
	}
	return total
}

// VacuumTableStats holds per-table VACUUM statistics.
type VacuumTableStats struct {
	TableName     string
	TuplesScanned int
	TuplesRemoved int
}

// Vacuum removes dead tuples from all tables.
func (e *Engine) Vacuum() (*VacuumResult, error) {
	globalXmin := e.txnManager.GetGlobalXmin()
	result := &VacuumResult{}

	for _, tableName := range e.catalog.GetAllTables() {
		tableID, ok := e.catalog.GetTableID(tableName)
		if !ok {
			continue
		}

		heap := e.catalog.GetTableHeap(tableID)
		tuples, err := heap.Scan()
		if err != nil {
			return nil, fmt.Errorf("vacuum scan %s: %w", tableName, err)
		}

		stats := VacuumTableStats{
			TableName:     tableName,
			TuplesScanned: len(tuples),
		}

		for _, t := range tuples {
			// Dead tuple conditions:
			// 1. XMax is set (deleted/updated)
			// 2. XMax < globalXmin (invisible to all active txns)
			// 3. XMax txn actually committed (not aborted)
			if t.Tuple.XMax != types.InvalidTxnID &&
				t.Tuple.XMax < globalXmin &&
				e.txnManager.IsTxnCommitted(t.Tuple.XMax) {
				if err := heap.Delete(t.PageID, t.SlotNum); err != nil {
					return nil, fmt.Errorf("vacuum delete %s: %w", tableName, err)
				}
				stats.TuplesRemoved++
			}
		}

		result.Tables = append(result.Tables, stats)
	}

	// Rebuild indexes for tables that have them
	for _, tableName := range e.catalog.GetAllTables() {
		tableID, ok := e.catalog.GetTableID(tableName)
		if !ok {
			continue
		}
		if _, exists := e.indexes[tableID]; !exists {
			continue
		}
		colName, ok := e.catalog.GetIndexColumn(tableID)
		if !ok {
			continue
		}

		schema := e.catalog.GetSchema(tableName)
		heap := e.catalog.GetTableHeap(tableID)

		newBtree, err := index.NewBTree(e.bufferPool, 64)
		if err != nil {
			return nil, fmt.Errorf("vacuum rebuild index %s: %w", tableName, err)
		}

		tuples, err := heap.Scan()
		if err != nil {
			return nil, fmt.Errorf("vacuum rescan %s: %w", tableName, err)
		}

		for _, t := range tuples {
			if t.Tuple.IsDeleted() {
				continue
			}
			rowData, err := types.DeserializeRow(schema, t.Tuple.Data)
			if err != nil {
				continue
			}
			val, ok := rowData[colName]
			if !ok {
				continue
			}
			key := index.EncodeKey(val, 64)
			rid := index.RID{PageID: t.PageID, SlotNum: t.SlotNum, TableID: tableID}
			newBtree.Insert(key, rid)
		}

		e.indexes[tableID] = newBtree
		e.catalog.SetIndexRoot(tableID, newBtree.GetRootPageID(), colName)
	}

	// Flush all modified pages
	if err := e.bufferPool.FlushAllPages(); err != nil {
		return nil, fmt.Errorf("vacuum flush: %w", err)
	}

	// Clean up committed txn records that are no longer needed
	e.txnManager.PruneCommittedBefore(globalXmin)

	return result, nil
}
