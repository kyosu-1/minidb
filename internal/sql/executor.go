package sql

import (
	"encoding/json"
	"fmt"
	"minidb/internal/storage"
	"minidb/internal/txn"
	"minidb/internal/wal"
	"minidb/pkg/types"
)

// Executor executes SQL statements.
type Executor struct {
	// In-memory MVCC store (for version tracking)
	mvccStore  *txn.MVCCStore
	txnManager *txn.Manager
	walWriter  *wal.Writer

	// Disk-based storage
	catalog    *storage.Catalog
	bufferPool *storage.BufferPool

	// Current transaction (for REPL mode)
	currentTxn *txn.Transaction
}

// Result represents the result of a query.
type Result struct {
	Columns []string
	Rows    []types.Row
	Message string
	Error   error
}

// NewExecutor creates a new SQL executor.
func NewExecutor(store *txn.MVCCStore, txnManager *txn.Manager, walWriter *wal.Writer) *Executor {
	return &Executor{
		mvccStore:  store,
		txnManager: txnManager,
		walWriter:  walWriter,
	}
}

// SetStorage sets the disk-based storage components.
func (e *Executor) SetStorage(catalog *storage.Catalog, bufferPool *storage.BufferPool) {
	e.catalog = catalog
	e.bufferPool = bufferPool
}

// Execute executes a SQL statement.
func (e *Executor) Execute(sqlStr string) *Result {
	parser := NewParser(sqlStr)
	stmt, err := parser.Parse()
	if err != nil {
		return &Result{Error: err}
	}

	switch s := stmt.(type) {
	case *BeginStmt:
		return e.executeBegin()
	case *CommitStmt:
		return e.executeCommit()
	case *RollbackStmt:
		return e.executeRollback()
	case *CreateTableStmt:
		return e.executeCreateTable(s)
	case *InsertStmt:
		return e.executeInsert(s)
	case *SelectStmt:
		return e.executeSelect(s)
	case *UpdateStmt:
		return e.executeUpdate(s)
	case *DeleteStmt:
		return e.executeDelete(s)
	default:
		return &Result{Error: fmt.Errorf("unknown statement type")}
	}
}

func (e *Executor) executeBegin() *Result {
	if e.currentTxn != nil {
		return &Result{Error: fmt.Errorf("transaction already in progress")}
	}
	e.currentTxn = e.txnManager.Begin()
	return &Result{Message: fmt.Sprintf("BEGIN (txn %d)", e.currentTxn.ID)}
}

func (e *Executor) executeCommit() *Result {
	if e.currentTxn == nil {
		return &Result{Error: fmt.Errorf("no transaction in progress")}
	}
	txnID := e.currentTxn.ID

	// Flush dirty pages
	if e.bufferPool != nil {
		e.bufferPool.FlushAllPages()
	}

	if err := e.txnManager.Commit(e.currentTxn); err != nil {
		return &Result{Error: err}
	}
	e.currentTxn = nil
	return &Result{Message: fmt.Sprintf("COMMIT (txn %d)", txnID)}
}

func (e *Executor) executeRollback() *Result {
	if e.currentTxn == nil {
		return &Result{Error: fmt.Errorf("no transaction in progress")}
	}
	txnID := e.currentTxn.ID

	// Rollback in MVCC store
	e.mvccStore.RollbackTransaction(e.currentTxn.ID)

	if err := e.txnManager.Rollback(e.currentTxn); err != nil {
		return &Result{Error: err}
	}
	e.currentTxn = nil
	return &Result{Message: fmt.Sprintf("ROLLBACK (txn %d)", txnID)}
}

func (e *Executor) executeCreateTable(stmt *CreateTableStmt) *Result {
	if e.catalog == nil {
		return &Result{Error: fmt.Errorf("storage not initialized")}
	}

	schema := &types.Schema{
		TableName: stmt.TableName,
		Columns:   make([]types.Column, len(stmt.Columns)),
	}

	for i, col := range stmt.Columns {
		schema.Columns[i] = types.Column{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}

	tableID, err := e.catalog.CreateTable(schema)
	if err != nil {
		return &Result{Error: err}
	}

	// Flush catalog page
	if e.bufferPool != nil {
		e.bufferPool.FlushAllPages()
	}

	return &Result{Message: fmt.Sprintf("CREATE TABLE %s (id=%d)", stmt.TableName, tableID)}
}

func (e *Executor) executeInsert(stmt *InsertStmt) *Result {
	if e.catalog == nil {
		return &Result{Error: fmt.Errorf("storage not initialized")}
	}

	schema := e.catalog.GetSchema(stmt.TableName)
	if schema == nil {
		return &Result{Error: fmt.Errorf("table %s does not exist", stmt.TableName)}
	}

	tableID, _ := e.catalog.GetTableID(stmt.TableName)
	heap := e.catalog.GetTableHeap(tableID)

	// Get or create transaction
	txn, autoCommit := e.getTransaction()
	cid := txn.NextCommandID()

	// Build row data
	rowData := make(map[string]types.Value)
	columns := stmt.Columns
	if len(columns) == 0 {
		for _, col := range schema.Columns {
			columns = append(columns, col.Name)
		}
	}

	if len(columns) != len(stmt.Values) {
		return &Result{Error: fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(stmt.Values))}
	}

	for i, colName := range columns {
		val := e.evaluateExpr(stmt.Values[i], nil)
		rowData[colName] = val
	}

	// Serialize row data
	data, _ := json.Marshal(rowData)

	// Create tuple with MVCC info
	tuple := &types.Tuple{
		XMin:    txn.ID,
		XMax:    types.InvalidTxnID,
		Cid:     cid,
		TableID: tableID,
		Data:    data,
	}

	// Insert into heap (disk)
	pageID, slotNum, err := heap.Insert(tuple)
	if err != nil {
		return &Result{Error: fmt.Errorf("insert failed: %w", err)}
	}

	tuple.RowID = uint64(pageID)<<16 | uint64(slotNum)

	// Also track in MVCC store for visibility
	e.mvccStore.RestoreTuple(tuple)

	// Log to WAL
	if e.walWriter != nil {
		e.walWriter.LogInsert(txn.ID, tableID, tuple.RowID, tuple.Serialize())
	}

	if autoCommit {
		if e.bufferPool != nil {
			e.bufferPool.FlushAllPages()
		}
		e.txnManager.Commit(txn)
	}

	return &Result{Message: fmt.Sprintf("INSERT 1 (page=%d, slot=%d)", pageID, slotNum)}
}

func (e *Executor) executeSelect(stmt *SelectStmt) *Result {
	if e.catalog == nil {
		return &Result{Error: fmt.Errorf("storage not initialized")}
	}

	schema := e.catalog.GetSchema(stmt.TableName)
	if schema == nil {
		return &Result{Error: fmt.Errorf("table %s does not exist", stmt.TableName)}
	}

	tableID, _ := e.catalog.GetTableID(stmt.TableName)
	heap := e.catalog.GetTableHeap(tableID)

	// Get or create transaction
	txn, autoCommit := e.getTransaction()

	// Scan heap from disk
	tuples, err := heap.Scan()
	if err != nil {
		return &Result{Error: fmt.Errorf("scan failed: %w", err)}
	}

	result := &Result{}

	// Determine columns
	if len(stmt.Columns) == 1 && stmt.Columns[0] == "*" {
		for _, col := range schema.Columns {
			result.Columns = append(result.Columns, col.Name)
		}
	} else {
		result.Columns = stmt.Columns
	}

	// Process tuples
	for _, t := range tuples {
		// Check MVCC visibility
		if !txn.Snapshot.IsVisible(t.Tuple) {
			continue
		}

		var rowData map[string]types.Value
		json.Unmarshal(t.Tuple.Data, &rowData)

		// Apply WHERE filter
		if stmt.Where != nil {
			if !e.evaluateCondition(stmt.Where, rowData) {
				continue
			}
		}

		// Build row
		row := types.Row{Values: make([]types.Value, len(result.Columns))}
		for i, colName := range result.Columns {
			if val, ok := rowData[colName]; ok {
				row.Values[i] = val
			} else {
				row.Values[i] = types.Value{IsNull: true}
			}
		}
		result.Rows = append(result.Rows, row)
	}

	if autoCommit {
		e.txnManager.Commit(txn)
	}

	result.Message = fmt.Sprintf("SELECT %d rows", len(result.Rows))
	return result
}

func (e *Executor) executeUpdate(stmt *UpdateStmt) *Result {
	if e.catalog == nil {
		return &Result{Error: fmt.Errorf("storage not initialized")}
	}

	schema := e.catalog.GetSchema(stmt.TableName)
	if schema == nil {
		return &Result{Error: fmt.Errorf("table %s does not exist", stmt.TableName)}
	}

	tableID, _ := e.catalog.GetTableID(stmt.TableName)
	heap := e.catalog.GetTableHeap(tableID)

	// Get or create transaction
	txn, autoCommit := e.getTransaction()
	cid := txn.NextCommandID()

	// Scan heap
	tuples, err := heap.Scan()
	if err != nil {
		return &Result{Error: fmt.Errorf("scan failed: %w", err)}
	}

	updated := 0
	for _, t := range tuples {
		// Check MVCC visibility
		if !txn.Snapshot.IsVisible(t.Tuple) {
			continue
		}

		var rowData map[string]types.Value
		json.Unmarshal(t.Tuple.Data, &rowData)

		// Apply WHERE filter
		if stmt.Where != nil {
			if !e.evaluateCondition(stmt.Where, rowData) {
				continue
			}
		}

		// Save old tuple for WAL
		oldTupleData := t.Tuple.Serialize()

		// Apply updates
		for colName, expr := range stmt.Set {
			rowData[colName] = e.evaluateExpr(expr, rowData)
		}

		// Mark old version as deleted
		t.Tuple.XMax = txn.ID

		// Create new version
		newData, _ := json.Marshal(rowData)
		newTuple := &types.Tuple{
			XMin:    txn.ID,
			XMax:    types.InvalidTxnID,
			Cid:     cid,
			TableID: tableID,
			RowID:   t.Tuple.RowID,
			Data:    newData,
		}

		// Update on disk (insert new version)
		pageID, slotNum, err := heap.Insert(newTuple)
		if err != nil {
			if autoCommit {
				e.txnManager.Rollback(txn)
			}
			return &Result{Error: fmt.Errorf("update failed: %w", err)}
		}

		newTuple.RowID = uint64(pageID)<<16 | uint64(slotNum)

		// Track in MVCC store
		e.mvccStore.RestoreTuple(newTuple)

		// Log to WAL
		if e.walWriter != nil {
			e.walWriter.LogUpdate(txn.ID, tableID, t.Tuple.RowID, oldTupleData, newTuple.Serialize())
		}

		updated++
	}

	if autoCommit {
		if e.bufferPool != nil {
			e.bufferPool.FlushAllPages()
		}
		e.txnManager.Commit(txn)
	}

	return &Result{Message: fmt.Sprintf("UPDATE %d", updated)}
}

func (e *Executor) executeDelete(stmt *DeleteStmt) *Result {
	if e.catalog == nil {
		return &Result{Error: fmt.Errorf("storage not initialized")}
	}

	schema := e.catalog.GetSchema(stmt.TableName)
	if schema == nil {
		return &Result{Error: fmt.Errorf("table %s does not exist", stmt.TableName)}
	}

	tableID, _ := e.catalog.GetTableID(stmt.TableName)
	heap := e.catalog.GetTableHeap(tableID)

	// Get or create transaction
	txn, autoCommit := e.getTransaction()

	// Scan heap
	tuples, err := heap.Scan()
	if err != nil {
		return &Result{Error: fmt.Errorf("scan failed: %w", err)}
	}

	deleted := 0
	for _, t := range tuples {
		// Check MVCC visibility
		if !txn.Snapshot.IsVisible(t.Tuple) {
			continue
		}

		var rowData map[string]types.Value
		json.Unmarshal(t.Tuple.Data, &rowData)

		// Apply WHERE filter
		if stmt.Where != nil {
			if !e.evaluateCondition(stmt.Where, rowData) {
				continue
			}
		}

		// Save old tuple for WAL
		oldTupleData := t.Tuple.Serialize()

		// Mark as deleted (MVCC style)
		t.Tuple.XMax = txn.ID

		// Update on disk
		heap.Update(t.PageID, t.SlotNum, t.Tuple)

		// Log to WAL
		if e.walWriter != nil {
			e.walWriter.LogDelete(txn.ID, tableID, t.Tuple.RowID, oldTupleData)
		}

		deleted++
	}

	if autoCommit {
		if e.bufferPool != nil {
			e.bufferPool.FlushAllPages()
		}
		e.txnManager.Commit(txn)
	}

	return &Result{Message: fmt.Sprintf("DELETE %d", deleted)}
}

func (e *Executor) getTransaction() (*txn.Transaction, bool) {
	if e.currentTxn != nil {
		return e.currentTxn, false
	}
	return e.txnManager.Begin(), true
}

func (e *Executor) evaluateExpr(expr Expr, rowData map[string]types.Value) types.Value {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value
	case *ColumnExpr:
		if rowData != nil {
			if val, ok := rowData[ex.Name]; ok {
				return val
			}
		}
		return types.Value{IsNull: true}
	default:
		return types.Value{IsNull: true}
	}
}

func (e *Executor) evaluateCondition(expr Expr, rowData map[string]types.Value) bool {
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case TokenAnd:
			return e.evaluateCondition(ex.Left, rowData) && e.evaluateCondition(ex.Right, rowData)
		case TokenOr:
			return e.evaluateCondition(ex.Left, rowData) || e.evaluateCondition(ex.Right, rowData)
		default:
			left := e.evaluateExpr(ex.Left, rowData)
			right := e.evaluateExpr(ex.Right, rowData)
			return e.compare(left, right, ex.Op)
		}
	case *LiteralExpr:
		return ex.Value.BoolVal
	default:
		return false
	}
}

func (e *Executor) compare(left, right types.Value, op TokenType) bool {
	if left.IsNull || right.IsNull {
		return false
	}

	switch op {
	case TokenEq:
		return e.valuesEqual(left, right)
	case TokenNe:
		return !e.valuesEqual(left, right)
	case TokenLt:
		return e.compareLess(left, right)
	case TokenLe:
		return e.compareLess(left, right) || e.valuesEqual(left, right)
	case TokenGt:
		return !e.compareLess(left, right) && !e.valuesEqual(left, right)
	case TokenGe:
		return !e.compareLess(left, right) || e.valuesEqual(left, right)
	default:
		return false
	}
}

func (e *Executor) valuesEqual(left, right types.Value) bool {
	if left.Type != right.Type {
		return false
	}
	switch left.Type {
	case types.ValueTypeInt:
		return left.IntVal == right.IntVal
	case types.ValueTypeString:
		return left.StrVal == right.StrVal
	case types.ValueTypeBool:
		return left.BoolVal == right.BoolVal
	default:
		return false
	}
}

func (e *Executor) compareLess(left, right types.Value) bool {
	if left.Type != right.Type {
		return false
	}
	switch left.Type {
	case types.ValueTypeInt:
		return left.IntVal < right.IntVal
	case types.ValueTypeString:
		return left.StrVal < right.StrVal
	default:
		return false
	}
}

// HasTransaction returns true if there's an active transaction.
func (e *Executor) HasTransaction() bool {
	return e.currentTxn != nil
}

// CurrentTxnID returns the current transaction ID.
func (e *Executor) CurrentTxnID() types.TxnID {
	if e.currentTxn != nil {
		return e.currentTxn.ID
	}
	return types.InvalidTxnID
}
