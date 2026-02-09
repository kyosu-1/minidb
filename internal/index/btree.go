// Package index implements B-Tree index for fast lookups.
package index

import (
	"bytes"
	"encoding/binary"
	"minidb/internal/storage"
	"minidb/pkg/types"
)

// EncodeKey encodes a Value into a byte slice that preserves sort order under bytes.Compare.
// INT: sign-bit flip + big-endian so that -1 < 0 < 1 in byte order.
// TEXT: raw bytes, zero-padded to keySize.
// BOOL: single byte 0x00/0x01.
func EncodeKey(val types.Value, keySize int) []byte {
	key := make([]byte, keySize)
	switch val.Type {
	case types.ValueTypeInt:
		// XOR sign bit so negative values sort before positive
		u := uint64(val.IntVal) ^ (1 << 63)
		binary.BigEndian.PutUint64(key[0:8], u)
	case types.ValueTypeString:
		copy(key, []byte(val.StrVal))
	case types.ValueTypeBool:
		if val.BoolVal {
			key[0] = 0x01
		}
	}
	return key
}

const (
	// B-Tree node layout:
	// Header: IsLeaf(1) + KeyCount(2) + Reserved(1) = 4 bytes
	// For leaf nodes: [Key1][RID1][Key2][RID2]...
	// For internal nodes: [Child0][Key1][Child1][Key2][Child2]...
	
	btreeHeaderSize = 4
	maxKeySize      = 64  // Maximum key size
	ridSize         = 12  // PageID(4) + SlotNum(4) + TableID(4)
	pageIDSize      = 4
	
	// Calculate order based on page size
	// Leaf: (PageSize - Header) / (KeySize + RID)
	// Internal: (PageSize - Header - PageID) / (KeySize + PageID)
)

// RID represents a row identifier (page + slot).
type RID struct {
	PageID  types.PageID
	SlotNum uint16
	TableID uint32
}

func (r RID) Serialize() []byte {
	buf := make([]byte, ridSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(r.PageID))
	binary.LittleEndian.PutUint16(buf[4:6], r.SlotNum)
	binary.LittleEndian.PutUint16(buf[6:8], 0) // padding
	binary.LittleEndian.PutUint32(buf[8:12], r.TableID)
	return buf
}

func DeserializeRID(buf []byte) RID {
	return RID{
		PageID:  types.PageID(binary.LittleEndian.Uint32(buf[0:4])),
		SlotNum: binary.LittleEndian.Uint16(buf[4:6]),
		TableID: binary.LittleEndian.Uint32(buf[8:12]),
	}
}

// BTree represents a B-Tree index.
type BTree struct {
	bufferPool *storage.BufferPool
	rootPageID types.PageID
	keySize    int
	order      int // Maximum number of children
}

// BTreeNode represents a node in the B-Tree.
type BTreeNode struct {
	page     *storage.Page
	isLeaf   bool
	keyCount int
	keys     [][]byte
	children []types.PageID // For internal nodes
	values   []RID          // For leaf nodes
}

// NewBTree creates a new B-Tree index.
func NewBTree(bufferPool *storage.BufferPool, keySize int) (*BTree, error) {
	bt := &BTree{
		bufferPool: bufferPool,
		keySize:    keySize,
	}
	
	// Calculate order
	usableSpace := storage.PageSize - storage.PageHeaderSize - btreeHeaderSize
	leafEntrySize := keySize + ridSize
	bt.order = usableSpace / leafEntrySize
	if bt.order < 3 {
		bt.order = 3
	}

	// Create root page (initially a leaf)
	rootPage, err := bufferPool.NewPage(storage.PageTypeBTree)
	if err != nil {
		return nil, err
	}
	
	bt.rootPageID = rootPage.ID
	
	// Initialize as empty leaf
	node := &BTreeNode{
		page:   rootPage,
		isLeaf: true,
	}
	node.serialize()
	bufferPool.UnpinPage(rootPage.ID, true)
	
	return bt, nil
}

// LoadBTree loads an existing B-Tree.
func LoadBTree(bufferPool *storage.BufferPool, rootPageID types.PageID, keySize int) *BTree {
	bt := &BTree{
		bufferPool: bufferPool,
		rootPageID: rootPageID,
		keySize:    keySize,
	}
	
	usableSpace := storage.PageSize - storage.PageHeaderSize - btreeHeaderSize
	leafEntrySize := keySize + ridSize
	bt.order = usableSpace / leafEntrySize
	if bt.order < 3 {
		bt.order = 3
	}

	return bt
}

// Insert inserts a key-value pair into the B-Tree.
func (bt *BTree) Insert(key []byte, rid RID) error {
	// Pad or truncate key to fixed size
	k := bt.normalizeKey(key)
	
	// Find leaf node
	leafNode, path, err := bt.findLeaf(k)
	if err != nil {
		return err
	}
	
	// Insert into leaf
	inserted := bt.insertIntoLeaf(leafNode, k, rid)
	
	// Check if split is needed
	if leafNode.keyCount > bt.order-1 {
		bt.splitLeaf(leafNode, path)
	}
	
	// Unpin all pages
	for _, pageID := range path {
		bt.bufferPool.UnpinPage(pageID, inserted)
	}
	bt.bufferPool.UnpinPage(leafNode.page.ID, true)
	
	return nil
}

// Search finds the RID for a key.
func (bt *BTree) Search(key []byte) (RID, bool) {
	k := bt.normalizeKey(key)
	
	leafNode, path, err := bt.findLeaf(k)
	if err != nil {
		return RID{}, false
	}
	
	// Search in leaf
	for i := 0; i < leafNode.keyCount; i++ {
		if bytes.Equal(leafNode.keys[i], k) {
			rid := leafNode.values[i]
			
			// Unpin pages
			for _, pageID := range path {
				bt.bufferPool.UnpinPage(pageID, false)
			}
			bt.bufferPool.UnpinPage(leafNode.page.ID, false)
			
			return rid, true
		}
	}
	
	// Unpin pages
	for _, pageID := range path {
		bt.bufferPool.UnpinPage(pageID, false)
	}
	bt.bufferPool.UnpinPage(leafNode.page.ID, false)
	
	return RID{}, false
}

// Delete removes a key from the B-Tree.
func (bt *BTree) Delete(key []byte) bool {
	k := bt.normalizeKey(key)
	
	leafNode, path, err := bt.findLeaf(k)
	if err != nil {
		return false
	}
	
	// Find and remove key
	found := false
	for i := 0; i < leafNode.keyCount; i++ {
		if bytes.Equal(leafNode.keys[i], k) {
			// Remove by shifting
			copy(leafNode.keys[i:], leafNode.keys[i+1:])
			copy(leafNode.values[i:], leafNode.values[i+1:])
			leafNode.keyCount--
			found = true
			break
		}
	}
	
	if found {
		leafNode.serialize()
	}
	
	// Unpin pages
	for _, pageID := range path {
		bt.bufferPool.UnpinPage(pageID, found)
	}
	bt.bufferPool.UnpinPage(leafNode.page.ID, found)
	
	return found
}

// RangeScan returns all RIDs in the given key range.
func (bt *BTree) RangeScan(startKey, endKey []byte) []RID {
	start := bt.normalizeKey(startKey)
	end := bt.normalizeKey(endKey)
	
	var results []RID
	
	leafNode, path, err := bt.findLeaf(start)
	if err != nil {
		return results
	}
	
	// Scan through leaf nodes
	for {
		for i := 0; i < leafNode.keyCount; i++ {
			if bytes.Compare(leafNode.keys[i], start) >= 0 &&
				bytes.Compare(leafNode.keys[i], end) <= 0 {
				results = append(results, leafNode.values[i])
			}
		}
		
		// TODO: Follow sibling pointers for full range scan
		break
	}
	
	// Unpin pages
	for _, pageID := range path {
		bt.bufferPool.UnpinPage(pageID, false)
	}
	bt.bufferPool.UnpinPage(leafNode.page.ID, false)
	
	return results
}

// ScanAll returns all RIDs in the index.
func (bt *BTree) ScanAll() []RID {
	var results []RID
	bt.scanNode(bt.rootPageID, &results)
	return results
}

func (bt *BTree) scanNode(pageID types.PageID, results *[]RID) {
	page, err := bt.bufferPool.FetchPage(pageID)
	if err != nil {
		return
	}
	defer bt.bufferPool.UnpinPage(pageID, false)
	
	node := bt.deserializeNode(page)
	
	if node.isLeaf {
		for i := 0; i < node.keyCount; i++ {
			*results = append(*results, node.values[i])
		}
	} else {
		for i := 0; i <= node.keyCount; i++ {
			if i < len(node.children) {
				bt.scanNode(node.children[i], results)
			}
		}
	}
}

// findLeaf finds the leaf node for a key, returning the path taken.
func (bt *BTree) findLeaf(key []byte) (*BTreeNode, []types.PageID, error) {
	var path []types.PageID
	
	page, err := bt.bufferPool.FetchPage(bt.rootPageID)
	if err != nil {
		return nil, nil, err
	}
	
	node := bt.deserializeNode(page)
	
	for !node.isLeaf {
		path = append(path, node.page.ID)
		
		// Find child to follow
		childIdx := 0
		for i := 0; i < node.keyCount; i++ {
			if bytes.Compare(key, node.keys[i]) >= 0 {
				childIdx = i + 1
			} else {
				break
			}
		}
		
		if childIdx >= len(node.children) {
			childIdx = len(node.children) - 1
		}
		
		childPageID := node.children[childIdx]
		
		page, err = bt.bufferPool.FetchPage(childPageID)
		if err != nil {
			return nil, path, err
		}
		
		node = bt.deserializeNode(page)
	}
	
	return node, path, nil
}

// insertIntoLeaf inserts a key-value pair into a leaf node.
func (bt *BTree) insertIntoLeaf(node *BTreeNode, key []byte, rid RID) bool {
	// Find insertion point
	insertIdx := 0
	for i := 0; i < node.keyCount; i++ {
		cmp := bytes.Compare(key, node.keys[i])
		if cmp == 0 {
			// Key exists, update value
			node.values[i] = rid
			node.serialize()
			return true
		}
		if cmp > 0 {
			insertIdx = i + 1
		}
	}
	
	// Shift and insert
	node.keys = append(node.keys, nil)
	node.values = append(node.values, RID{})
	
	copy(node.keys[insertIdx+1:], node.keys[insertIdx:])
	copy(node.values[insertIdx+1:], node.values[insertIdx:])
	
	node.keys[insertIdx] = key
	node.values[insertIdx] = rid
	node.keyCount++
	
	node.serialize()
	return true
}

// splitLeaf splits a full leaf node.
func (bt *BTree) splitLeaf(node *BTreeNode, path []types.PageID) {
	// Create new leaf
	newPage, err := bt.bufferPool.NewPage(storage.PageTypeBTree)
	if err != nil {
		return
	}
	
	newNode := &BTreeNode{
		page:   newPage,
		isLeaf: true,
	}
	
	// Split keys
	mid := node.keyCount / 2
	newNode.keys = make([][]byte, node.keyCount-mid)
	newNode.values = make([]RID, node.keyCount-mid)
	
	copy(newNode.keys, node.keys[mid:])
	copy(newNode.values, node.values[mid:])
	newNode.keyCount = node.keyCount - mid
	
	node.keys = node.keys[:mid]
	node.values = node.values[:mid]
	node.keyCount = mid
	
	// Serialize both
	node.serialize()
	newNode.serialize()
	
	// Insert into parent
	splitKey := newNode.keys[0]
	bt.insertIntoParent(path, node.page.ID, splitKey, newPage.ID)
	
	bt.bufferPool.UnpinPage(newPage.ID, true)
}

// insertIntoParent inserts a new key and child into parent node.
func (bt *BTree) insertIntoParent(path []types.PageID, leftChild types.PageID, key []byte, rightChild types.PageID) {
	if len(path) == 0 {
		// Create new root
		newRoot, err := bt.bufferPool.NewPage(storage.PageTypeBTree)
		if err != nil {
			return
		}
		
		rootNode := &BTreeNode{
			page:     newRoot,
			isLeaf:   false,
			keyCount: 1,
			keys:     [][]byte{key},
			children: []types.PageID{leftChild, rightChild},
		}
		rootNode.serialize()
		
		bt.rootPageID = newRoot.ID
		bt.bufferPool.UnpinPage(newRoot.ID, true)
		return
	}
	
	// Get parent
	parentPageID := path[len(path)-1]
	parentPage, err := bt.bufferPool.FetchPage(parentPageID)
	if err != nil {
		return
	}
	
	parentNode := bt.deserializeNode(parentPage)
	
	// Find insertion point
	insertIdx := 0
	for i := 0; i < parentNode.keyCount; i++ {
		if bytes.Compare(key, parentNode.keys[i]) > 0 {
			insertIdx = i + 1
		}
	}
	
	// Insert key and child
	parentNode.keys = append(parentNode.keys, nil)
	parentNode.children = append(parentNode.children, 0)
	
	copy(parentNode.keys[insertIdx+1:], parentNode.keys[insertIdx:])
	copy(parentNode.children[insertIdx+2:], parentNode.children[insertIdx+1:])
	
	parentNode.keys[insertIdx] = key
	parentNode.children[insertIdx+1] = rightChild
	parentNode.keyCount++
	
	parentNode.serialize()
	
	// Check if parent needs split
	if parentNode.keyCount > bt.order-1 {
		bt.splitInternal(parentNode, path[:len(path)-1])
	}
	
	bt.bufferPool.UnpinPage(parentPageID, true)
}

// splitInternal splits an internal node.
func (bt *BTree) splitInternal(node *BTreeNode, path []types.PageID) {
	// Create new internal node
	newPage, err := bt.bufferPool.NewPage(storage.PageTypeBTree)
	if err != nil {
		return
	}
	
	newNode := &BTreeNode{
		page:   newPage,
		isLeaf: false,
	}
	
	// Split
	mid := node.keyCount / 2
	promoteKey := node.keys[mid]
	
	newNode.keys = make([][]byte, node.keyCount-mid-1)
	newNode.children = make([]types.PageID, node.keyCount-mid)
	
	copy(newNode.keys, node.keys[mid+1:])
	copy(newNode.children, node.children[mid+1:])
	newNode.keyCount = node.keyCount - mid - 1
	
	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]
	node.keyCount = mid
	
	node.serialize()
	newNode.serialize()
	
	bt.insertIntoParent(path, node.page.ID, promoteKey, newPage.ID)
	
	bt.bufferPool.UnpinPage(newPage.ID, true)
}

// normalizeKey pads or truncates key to fixed size.
func (bt *BTree) normalizeKey(key []byte) []byte {
	k := make([]byte, bt.keySize)
	if len(key) > bt.keySize {
		copy(k, key[:bt.keySize])
	} else {
		copy(k, key)
	}
	return k
}

// deserializeNode reads a B-Tree node from a page.
func (bt *BTree) deserializeNode(page *storage.Page) *BTreeNode {
	node := &BTreeNode{page: page}

	node.isLeaf = page.Data[storage.PageHeaderSize] == 1
	node.keyCount = int(binary.LittleEndian.Uint16(page.Data[storage.PageHeaderSize+1 : storage.PageHeaderSize+3]))

	offset := storage.PageHeaderSize + btreeHeaderSize
	
	if node.isLeaf {
		node.keys = make([][]byte, node.keyCount)
		node.values = make([]RID, node.keyCount)
		
		for i := 0; i < node.keyCount; i++ {
			node.keys[i] = make([]byte, bt.keySize)
			copy(node.keys[i], page.Data[offset:offset+bt.keySize])
			offset += bt.keySize
			
			node.values[i] = DeserializeRID(page.Data[offset : offset+ridSize])
			offset += ridSize
		}
	} else {
		node.keys = make([][]byte, node.keyCount)
		node.children = make([]types.PageID, node.keyCount+1)
		
		// First child
		node.children[0] = types.PageID(binary.LittleEndian.Uint32(page.Data[offset:]))
		offset += pageIDSize
		
		for i := 0; i < node.keyCount; i++ {
			node.keys[i] = make([]byte, bt.keySize)
			copy(node.keys[i], page.Data[offset:offset+bt.keySize])
			offset += bt.keySize
			
			node.children[i+1] = types.PageID(binary.LittleEndian.Uint32(page.Data[offset:]))
			offset += pageIDSize
		}
	}
	
	return node
}

// serialize writes a B-Tree node to its page.
func (node *BTreeNode) serialize() {
	page := node.page

	// Header (after page header)
	if node.isLeaf {
		page.Data[storage.PageHeaderSize] = 1
	} else {
		page.Data[storage.PageHeaderSize] = 0
	}
	binary.LittleEndian.PutUint16(page.Data[storage.PageHeaderSize+1:storage.PageHeaderSize+3], uint16(node.keyCount))

	offset := storage.PageHeaderSize + btreeHeaderSize
	
	if node.isLeaf {
		for i := 0; i < node.keyCount; i++ {
			copy(page.Data[offset:], node.keys[i])
			offset += len(node.keys[i])
			
			copy(page.Data[offset:], node.values[i].Serialize())
			offset += ridSize
		}
	} else {
		// First child
		if len(node.children) > 0 {
			binary.LittleEndian.PutUint32(page.Data[offset:], uint32(node.children[0]))
		}
		offset += pageIDSize
		
		for i := 0; i < node.keyCount; i++ {
			copy(page.Data[offset:], node.keys[i])
			offset += len(node.keys[i])
			
			if i+1 < len(node.children) {
				binary.LittleEndian.PutUint32(page.Data[offset:], uint32(node.children[i+1]))
			}
			offset += pageIDSize
		}
	}
	
	page.IsDirty = true
}

// GetRootPageID returns the root page ID.
func (bt *BTree) GetRootPageID() types.PageID {
	return bt.rootPageID
}
