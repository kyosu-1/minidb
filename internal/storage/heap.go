package storage

import (
	"encoding/binary"
	"fmt"
	"minidb/pkg/types"
)

// TableHeap manages storage for a single table as a collection of pages.
type TableHeap struct {
	bufferPool *BufferPool
	tableID    uint32
	firstPage  types.PageID
	lastPage   types.PageID
}

// TableHeapMeta contains metadata for a table heap.
type TableHeapMeta struct {
	TableID   uint32
	FirstPage types.PageID
	LastPage  types.PageID
	RowCount  uint64
}

// NewTableHeap creates a new table heap.
func NewTableHeap(bufferPool *BufferPool, tableID uint32) (*TableHeap, error) {
	// Allocate first page
	page, err := bufferPool.NewPage(PageTypeData)
	if err != nil {
		return nil, err
	}
	
	th := &TableHeap{
		bufferPool: bufferPool,
		tableID:    tableID,
		firstPage:  page.ID,
		lastPage:   page.ID,
	}
	
	bufferPool.UnpinPage(page.ID, true)
	
	return th, nil
}

// LoadTableHeap loads an existing table heap.
func LoadTableHeap(bufferPool *BufferPool, tableID uint32, firstPage, lastPage types.PageID) *TableHeap {
	return &TableHeap{
		bufferPool: bufferPool,
		tableID:    tableID,
		firstPage:  firstPage,
		lastPage:   lastPage,
	}
}

// Insert inserts a tuple into the table.
// Returns the RID (page ID and slot number).
func (th *TableHeap) Insert(tuple *types.Tuple) (types.PageID, uint16, error) {
	data := tuple.Serialize()
	
	// Try to insert into last page
	page, err := th.bufferPool.FetchPage(th.lastPage)
	if err != nil {
		return 0, 0, err
	}
	
	slotNum, err := page.InsertTuple(data)
	if err == nil {
		th.bufferPool.UnpinPage(page.ID, true)
		return page.ID, slotNum, nil
	}
	
	// Page is full, allocate new page
	th.bufferPool.UnpinPage(page.ID, false)
	
	newPage, err := th.bufferPool.NewPage(PageTypeData)
	if err != nil {
		return 0, 0, err
	}
	
	// Link pages (store next page ID in header reserved area)
	// For simplicity, we just track first/last
	th.lastPage = newPage.ID
	
	slotNum, err = newPage.InsertTuple(data)
	if err != nil {
		th.bufferPool.UnpinPage(newPage.ID, true)
		return 0, 0, err
	}
	
	th.bufferPool.UnpinPage(newPage.ID, true)
	return newPage.ID, slotNum, nil
}

// Get retrieves a tuple by RID.
func (th *TableHeap) Get(pageID types.PageID, slotNum uint16) (*types.Tuple, error) {
	page, err := th.bufferPool.FetchPage(pageID)
	if err != nil {
		return nil, err
	}
	defer th.bufferPool.UnpinPage(pageID, false)
	
	data, err := page.GetTuple(slotNum)
	if err != nil {
		return nil, err
	}
	
	return types.DeserializeTuple(data)
}

// Update updates a tuple at the given RID.
func (th *TableHeap) Update(pageID types.PageID, slotNum uint16, tuple *types.Tuple) error {
	page, err := th.bufferPool.FetchPage(pageID)
	if err != nil {
		return err
	}
	defer th.bufferPool.UnpinPage(pageID, true)
	
	data := tuple.Serialize()
	return page.UpdateTuple(slotNum, data)
}

// Delete marks a tuple as deleted.
func (th *TableHeap) Delete(pageID types.PageID, slotNum uint16) error {
	page, err := th.bufferPool.FetchPage(pageID)
	if err != nil {
		return err
	}
	defer th.bufferPool.UnpinPage(pageID, true)
	
	return page.DeleteTuple(slotNum)
}

// Scan iterates over all tuples in the table.
func (th *TableHeap) Scan() ([]*TupleWithRID, error) {
	var results []*TupleWithRID
	
	currentPageID := th.firstPage
	
	for currentPageID != types.InvalidPageID {
		page, err := th.bufferPool.FetchPage(currentPageID)
		if err != nil {
			if currentPageID != th.firstPage {
				// Page doesn't exist yet, stop scanning
				break
			}
			return nil, err
		}
		
		tuples := page.GetAllTuples()
		for _, t := range tuples {
			tuple, err := types.DeserializeTuple(t.Data)
			if err != nil {
				continue
			}
			results = append(results, &TupleWithRID{
				Tuple:   tuple,
				PageID:  currentPageID,
				SlotNum: t.SlotNum,
			})
		}
		
		th.bufferPool.UnpinPage(currentPageID, false)
		
		// Move to next page
		if currentPageID == th.lastPage {
			break
		}
		currentPageID++
	}
	
	return results, nil
}

// TupleWithRID wraps a tuple with its location.
type TupleWithRID struct {
	Tuple   *types.Tuple
	PageID  types.PageID
	SlotNum uint16
}

// GetMeta returns the table heap metadata.
func (th *TableHeap) GetMeta() TableHeapMeta {
	return TableHeapMeta{
		TableID:   th.tableID,
		FirstPage: th.firstPage,
		LastPage:  th.lastPage,
	}
}

// GetFirstPage returns the first page ID.
func (th *TableHeap) GetFirstPage() types.PageID {
	return th.firstPage
}

// GetLastPage returns the last page ID.
func (th *TableHeap) GetLastPage() types.PageID {
	return th.lastPage
}

// SetLastPage updates the last page ID.
func (th *TableHeap) SetLastPage(pageID types.PageID) {
	th.lastPage = pageID
}

// Catalog manages database schema and table metadata.
type Catalog struct {
	bufferPool   *BufferPool
	catalogPage  types.PageID
	schemas      map[string]*types.Schema
	tableHeaps   map[uint32]*TableHeap
	tableIDs     map[string]uint32
	nextTableID  uint32
	indexRoots   map[uint32]types.PageID // tableID -> B-Tree root
}

// CatalogEntry represents a serialized catalog entry.
type CatalogEntry struct {
	TableID    uint32
	TableName  string
	FirstPage  types.PageID
	LastPage   types.PageID
	IndexRoot  types.PageID
	Columns    []types.Column
}

// NewCatalog creates a new catalog.
func NewCatalog(bufferPool *BufferPool) (*Catalog, error) {
	// Allocate catalog page
	page, err := bufferPool.NewPage(PageTypeCatalog)
	if err != nil {
		return nil, err
	}
	
	c := &Catalog{
		bufferPool:  bufferPool,
		catalogPage: page.ID,
		schemas:     make(map[string]*types.Schema),
		tableHeaps:  make(map[uint32]*TableHeap),
		tableIDs:    make(map[string]uint32),
		nextTableID: 1,
		indexRoots:  make(map[uint32]types.PageID),
	}
	
	bufferPool.UnpinPage(page.ID, true)
	
	return c, nil
}

// LoadCatalog loads the catalog from disk.
func LoadCatalog(bufferPool *BufferPool, catalogPageID types.PageID) (*Catalog, error) {
	c := &Catalog{
		bufferPool:  bufferPool,
		catalogPage: catalogPageID,
		schemas:     make(map[string]*types.Schema),
		tableHeaps:  make(map[uint32]*TableHeap),
		tableIDs:    make(map[string]uint32),
		nextTableID: 1,
		indexRoots:  make(map[uint32]types.PageID),
	}
	
	// Read catalog page
	page, err := bufferPool.FetchPage(catalogPageID)
	if err != nil {
		return nil, err
	}
	defer bufferPool.UnpinPage(catalogPageID, false)
	
	// Parse catalog entries
	c.deserialize(page)
	
	return c, nil
}

// CreateTable creates a new table.
func (c *Catalog) CreateTable(schema *types.Schema) (uint32, error) {
	if _, exists := c.tableIDs[schema.TableName]; exists {
		return 0, fmt.Errorf("table %s already exists", schema.TableName)
	}
	
	tableID := c.nextTableID
	c.nextTableID++
	
	// Create table heap
	heap, err := NewTableHeap(c.bufferPool, tableID)
	if err != nil {
		return 0, err
	}
	
	c.schemas[schema.TableName] = schema
	c.tableHeaps[tableID] = heap
	c.tableIDs[schema.TableName] = tableID
	
	// Save catalog
	c.serialize()
	
	return tableID, nil
}

// GetSchema returns the schema for a table.
func (c *Catalog) GetSchema(tableName string) *types.Schema {
	return c.schemas[tableName]
}

// GetTableID returns the table ID for a table name.
func (c *Catalog) GetTableID(tableName string) (uint32, bool) {
	id, ok := c.tableIDs[tableName]
	return id, ok
}

// GetTableHeap returns the table heap for a table ID.
func (c *Catalog) GetTableHeap(tableID uint32) *TableHeap {
	return c.tableHeaps[tableID]
}

// SetIndexRoot sets the B-Tree root for a table.
func (c *Catalog) SetIndexRoot(tableID uint32, rootPageID types.PageID) {
	c.indexRoots[tableID] = rootPageID
	c.serialize()
}

// GetIndexRoot returns the B-Tree root for a table.
func (c *Catalog) GetIndexRoot(tableID uint32) (types.PageID, bool) {
	root, ok := c.indexRoots[tableID]
	return root, ok
}

// GetCatalogPageID returns the catalog page ID.
func (c *Catalog) GetCatalogPageID() types.PageID {
	return c.catalogPage
}

// serialize saves the catalog to disk.
func (c *Catalog) serialize() {
	page, err := c.bufferPool.FetchPage(c.catalogPage)
	if err != nil {
		return
	}
	defer c.bufferPool.UnpinPage(c.catalogPage, true)
	
	// Clear page
	for i := PageHeaderSize; i < PageSize; i++ {
		page.Data[i] = 0
	}
	
	offset := PageHeaderSize
	
	// Write number of tables
	binary.LittleEndian.PutUint32(page.Data[offset:], uint32(len(c.schemas)))
	offset += 4
	
	// Write next table ID
	binary.LittleEndian.PutUint32(page.Data[offset:], c.nextTableID)
	offset += 4
	
	// Write each table entry
	for tableName, schema := range c.schemas {
		tableID := c.tableIDs[tableName]
		heap := c.tableHeaps[tableID]
		indexRoot := c.indexRoots[tableID]
		
		// Table ID
		binary.LittleEndian.PutUint32(page.Data[offset:], tableID)
		offset += 4
		
		// Table name length + name
		nameBytes := []byte(tableName)
		binary.LittleEndian.PutUint16(page.Data[offset:], uint16(len(nameBytes)))
		offset += 2
		copy(page.Data[offset:], nameBytes)
		offset += len(nameBytes)
		
		// First/Last page
		binary.LittleEndian.PutUint32(page.Data[offset:], uint32(heap.GetFirstPage()))
		offset += 4
		binary.LittleEndian.PutUint32(page.Data[offset:], uint32(heap.GetLastPage()))
		offset += 4
		
		// Index root
		binary.LittleEndian.PutUint32(page.Data[offset:], uint32(indexRoot))
		offset += 4
		
		// Number of columns
		binary.LittleEndian.PutUint16(page.Data[offset:], uint16(len(schema.Columns)))
		offset += 2
		
		// Each column
		for _, col := range schema.Columns {
			// Name
			colNameBytes := []byte(col.Name)
			binary.LittleEndian.PutUint16(page.Data[offset:], uint16(len(colNameBytes)))
			offset += 2
			copy(page.Data[offset:], colNameBytes)
			offset += len(colNameBytes)
			
			// Type
			page.Data[offset] = byte(col.Type)
			offset++
			
			// Nullable
			if col.Nullable {
				page.Data[offset] = 1
			} else {
				page.Data[offset] = 0
			}
			offset++
		}
	}
	
	page.IsDirty = true
}

// deserialize loads the catalog from a page.
func (c *Catalog) deserialize(page *Page) {
	offset := PageHeaderSize
	
	// Number of tables
	numTables := binary.LittleEndian.Uint32(page.Data[offset:])
	offset += 4
	
	// Next table ID
	c.nextTableID = binary.LittleEndian.Uint32(page.Data[offset:])
	offset += 4
	
	// Read each table entry
	for i := uint32(0); i < numTables; i++ {
		// Table ID
		tableID := binary.LittleEndian.Uint32(page.Data[offset:])
		offset += 4
		
		// Table name
		nameLen := binary.LittleEndian.Uint16(page.Data[offset:])
		offset += 2
		tableName := string(page.Data[offset : offset+int(nameLen)])
		offset += int(nameLen)
		
		// First/Last page
		firstPage := types.PageID(binary.LittleEndian.Uint32(page.Data[offset:]))
		offset += 4
		lastPage := types.PageID(binary.LittleEndian.Uint32(page.Data[offset:]))
		offset += 4
		
		// Index root
		indexRoot := types.PageID(binary.LittleEndian.Uint32(page.Data[offset:]))
		offset += 4
		
		// Number of columns
		numCols := binary.LittleEndian.Uint16(page.Data[offset:])
		offset += 2
		
		// Columns
		columns := make([]types.Column, numCols)
		for j := uint16(0); j < numCols; j++ {
			// Name
			colNameLen := binary.LittleEndian.Uint16(page.Data[offset:])
			offset += 2
			colName := string(page.Data[offset : offset+int(colNameLen)])
			offset += int(colNameLen)
			
			// Type
			colType := types.ValueType(page.Data[offset])
			offset++
			
			// Nullable
			nullable := page.Data[offset] == 1
			offset++
			
			columns[j] = types.Column{
				Name:     colName,
				Type:     colType,
				Nullable: nullable,
			}
		}
		
		// Create schema
		schema := &types.Schema{
			TableName: tableName,
			Columns:   columns,
		}
		
		// Create table heap
		heap := LoadTableHeap(c.bufferPool, tableID, firstPage, lastPage)
		
		c.schemas[tableName] = schema
		c.tableHeaps[tableID] = heap
		c.tableIDs[tableName] = tableID
		if indexRoot != types.InvalidPageID {
			c.indexRoots[tableID] = indexRoot
		}
	}
}

// GetAllTables returns all table names.
func (c *Catalog) GetAllTables() []string {
	tables := make([]string, 0, len(c.schemas))
	for name := range c.schemas {
		tables = append(tables, name)
	}
	return tables
}
