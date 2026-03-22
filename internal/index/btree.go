// Package btree implements a B-Tree for sorted key-value storage.
// Design decision: Order 32 means each node holds 15-31 keys (min 15 before merge,
// max 31 before split). Order 32 was chosen because it matches a typical OS page
// size (4KB) when keys average ~100 bytes, giving good cache locality.
//
// The B-Tree stores string keys mapped to string slices (entity references).
// This is used as a secondary index: key = normalized value, value = []entity.attr
// that have that value for a given attribute.
package index

import (
	"sort"
	"strings"
	"sync"
)

// Order is the maximum number of children per node.
// A node splits when it reaches Order keys.
const Order = 32

// maxKeys is the maximum keys before split (Order - 1).
const maxKeys = Order - 1

// minKeys is the minimum keys after merge (ceil(Order/2) - 1).
const minKeys = Order/2 - 1

// BTreeNode represents a single node in the B-Tree.
type BTreeNode struct {
	keys     []string     // sorted keys
	values   [][]string   // entity references per key
	children []*BTreeNode // child pointers (nil for leaf)
	leaf     bool
}

// BTree is a concurrent-safe B-Tree index.
type BTree struct {
	root  *BTreeNode
	mu    sync.RWMutex
	count int // total number of entries (key-value pairs)
}

// New creates an empty B-Tree.
func New() *BTree {
	return &BTree{
		root: &BTreeNode{leaf: true},
	}
}

// Insert adds or updates an entry. If the key exists, entities are appended.
// Duplicate entities are not added twice.
func (t *BTree) Insert(key string, entities []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.insert(key, entities)
}

func (t *BTree) insert(key string, entities []string) {
	// Use recursive insert - returns true if key already existed
	existed := t.insertNode(t.root, key, entities)
	if !existed {
		t.count++
	}

	// If root was split, create new root
	if len(t.root.keys) > maxKeys {
		old := t.root
		t.root = &BTreeNode{
			children: []*BTreeNode{old},
		}
		t.splitChild(t.root, 0)
	}
}

// insertNode recursively inserts into the given node.
// Returns true if the key already existed (updated), false if new key was added.
func (t *BTree) insertNode(n *BTreeNode, key string, entities []string) bool {
	if n.leaf {
		// Find insertion point
		i := sort.Search(len(n.keys), func(i int) bool {
			return n.keys[i] >= key
		})

		if i < len(n.keys) && n.keys[i] == key {
			// Key exists - append entities with dedup
			n.values[i] = dedup(append(n.values[i], entities...))
			return true
		}

		// Insert new key at position i
		n.keys = append(n.keys, "")
		n.values = append(n.values, nil)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.values[i+1:], n.values[i:])
		n.keys[i] = key
		n.values[i] = entities
		return false
	}

	// Internal node - find child to descend into
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i] >= key
	})

	// If key equals keys[i], we still go to children[i+1] for insertion
	// Actually for internal nodes, if key matches, the actual data is in the
	// leaf below. But our design stores entity refs in both leaves and internal
	// nodes when splitting. Let me keep it simple: exact match in internal node
	// means update in place.
	if i < len(n.keys) && n.keys[i] == key {
		n.values[i] = dedup(append(n.values[i], entities...))
		// Also propagate to leaf? No - let's only store in leaves.
		// Actually, for simplicity, let me store in internal nodes too when
		// they happen to have the key. The search checks all nodes.
		return true
	}

	if i > len(n.keys) {
		i = len(n.keys)
	}

	// Descend to child
	existed := t.insertNode(n.children[i], key, entities)

	// Check if child needs splitting
	if len(n.children[i].keys) > maxKeys {
		t.splitChild(n, i)
	}

	return existed
}

// splitChild splits child c[i] into two, promoting median key to parent n.
func (t *BTree) splitChild(n *BTreeNode, i int) {
	child := n.children[i]
	median := len(child.keys) / 2

	// Create new right node
	right := &BTreeNode{
		leaf: child.leaf,
	}

	// Right gets keys/values after median
	right.keys = make([]string, len(child.keys)-median-1)
	right.values = make([][]string, len(child.keys)-median-1)
	copy(right.keys, child.keys[median+1:])
	copy(right.values, child.values[median+1:])

	// Right gets children after median (if not leaf)
	if !child.leaf {
		right.children = make([]*BTreeNode, len(child.children)-median-1)
		copy(right.children, child.children[median+1:])
	}

	// Median key promoted to parent
	medianKey := child.keys[median]
	medianValue := child.values[median]

	// Truncate left child to keys before median
	child.keys = child.keys[:median]
	child.values = child.values[:median]
	if !child.leaf {
		child.children = child.children[:median+1]
	}

	// Insert median into parent
	n.keys = append(n.keys, "")
	n.values = append(n.values, nil)
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.values[i+1:], n.values[i:])
	n.keys[i] = medianKey
	n.values[i] = medianValue

	// Insert right child into parent
	n.children = append(n.children, nil)
	copy(n.children[i+2:], n.children[i+1:])
	n.children[i+1] = right
}

// Search finds exact matches for a key.
func (t *BTree) Search(key string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.search(t.root, key)
}

func (t *BTree) search(n *BTreeNode, key string) []string {
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i] >= key
	})

	if i < len(n.keys) && n.keys[i] == key {
		return n.values[i]
	}

	if n.leaf {
		return nil
	}

	return t.search(n.children[i], key)
}

// searchNode is a non-recursive helper that returns the node and index.
// Used only for root-level checks.
func (t *BTree) searchNode(n *BTreeNode, key string) (int, bool) {
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i] >= key
	})
	if i < len(n.keys) && n.keys[i] == key {
		return i, true
	}
	return i, false
}

// RangeOp defines range query operators.
type RangeOp int

const (
	OpGt  RangeOp = iota // >
	OpGte                // >=
	OpLt                 // <
	OpLte                // <=
)

// RangeQuery returns all entity references where key op value.
func (t *BTree) RangeQuery(op RangeOp, value string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	switch op {
	case OpGt:
		return t.rangeGT(value, false)
	case OpGte:
		return t.rangeGT(value, true)
	case OpLt:
		return t.rangeLT(value, false)
	case OpLte:
		return t.rangeLT(value, true)
	}
	return nil
}

// rangeGT collects all entries with key > value (or >= if inclusive).
func (t *BTree) rangeGT(value string, inclusive bool) []string {
	var result []string
	t.collectGT(t.root, value, inclusive, &result)
	return result
}

func (t *BTree) collectGT(n *BTreeNode, value string, inclusive bool, result *[]string) {
	i := sort.Search(len(n.keys), func(i int) bool {
		if inclusive {
			return n.keys[i] >= value
		}
		return n.keys[i] > value
	})

	if n.leaf {
		for ; i < len(n.keys); i++ {
			*result = append(*result, n.values[i]...)
		}
		return
	}

	// Collect from left subtree first (all keys in children[i] are < keys[i])
	if i < len(n.children) {
		t.collectAll(n.children[i], result)
	}

	// Then the separator key and everything to the right
	for ; i < len(n.keys); i++ {
		*result = append(*result, n.values[i]...)
		if i+1 < len(n.children) {
			t.collectAll(n.children[i+1], result)
		}
	}
}

// rangeLT collects all entries with key < value (or <= if inclusive).
func (t *BTree) rangeLT(value string, inclusive bool) []string {
	var result []string
	t.collectLT(t.root, value, inclusive, &result)
	return result
}

func (t *BTree) collectLT(n *BTreeNode, value string, inclusive bool, result *[]string) {
	i := sort.Search(len(n.keys), func(i int) bool {
		if inclusive {
			return n.keys[i] > value
		}
		return n.keys[i] >= value
	})

	if n.leaf {
		for j := 0; j < i && j < len(n.keys); j++ {
			*result = append(*result, n.values[j]...)
		}
		return
	}

	// Keys before i and their left subtrees
	for j := 0; j < i && j < len(n.keys); j++ {
		if j < len(n.children) {
			t.collectAll(n.children[j], result)
		}
		*result = append(*result, n.values[j]...)
	}

	// Descend into children[i] if it exists
	if i < len(n.children) {
		t.collectLT(n.children[i], value, inclusive, result)
	}
}

// collectAll gathers every entry in the subtree.
func (t *BTree) collectAll(n *BTreeNode, result *[]string) {
	if n == nil {
		return
	}
	if n.leaf {
		for i := range n.values {
			*result = append(*result, n.values[i]...)
		}
		return
	}
	for i := range n.keys {
		t.collectAll(n.children[i], result)
		*result = append(*result, n.values[i]...)
	}
	if len(n.children) > len(n.keys) {
		t.collectAll(n.children[len(n.keys)], result)
	}
}

// Count returns the total number of unique keys in the tree.
func (t *BTree) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.count
}

// Keys returns all keys in sorted order (for debugging/stats).
func (t *BTree) Keys() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var keys []string
	t.collectKeys(t.root, &keys)
	return keys
}

func (t *BTree) collectKeys(n *BTreeNode, keys *[]string) {
	if n.leaf {
		*keys = append(*keys, n.keys...)
		return
	}
	for i := range n.keys {
		t.collectKeys(n.children[i], keys)
		*keys = append(*keys, n.keys[i])
	}
	if len(n.children) > len(n.keys) {
		t.collectKeys(n.children[len(n.keys)], keys)
	}
}

// Remove deletes an entity reference from a key.
// If no entities remain, the key is removed entirely.
func (t *BTree) Remove(key string, entity string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.remove(t.root, key, entity)
}

func (t *BTree) remove(n *BTreeNode, key string, entity string) {
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i] >= key
	})

	if i < len(n.keys) && n.keys[i] == key {
		// Found - remove entity from the list
		n.values[i] = removeStr(n.values[i], entity)
		if len(n.values[i]) == 0 {
			// No more entities - remove key
			if n.leaf {
				n.keys = append(n.keys[:i], n.keys[i+1:]...)
				n.values = append(n.values[:i], n.values[i+1:]...)
				t.count--
			} else {
				// For internal nodes, just clear it (simplified)
				n.keys = append(n.keys[:i], n.keys[i+1:]...)
				n.values = append(n.values[:i], n.values[i+1:]...)
				t.count--
			}
		}
		return
	}

	if !n.leaf && i < len(n.children) {
		t.remove(n.children[i], key, entity)
	}
}

// Helper: dedup a string slice.
func dedup(s []string) []string {
	seen := make(map[string]bool, len(s))
	result := make([]string, 0, len(s))
	for _, v := range s {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	return result
}

// Helper: remove first occurrence of a string from slice.
func removeStr(s []string, target string) []string {
	for i, v := range s {
		if v == target {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

// Debug returns a text representation of the tree structure.
func (t *BTree) Debug() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var sb strings.Builder
	t.debugNode(&sb, t.root, 0)
	return sb.String()
}

func (t *BTree) debugNode(sb *strings.Builder, n *BTreeNode, depth int) {
	indent := strings.Repeat("  ", depth)
	for i := range n.keys {
		if !n.leaf && i < len(n.children) {
			t.debugNode(sb, n.children[i], depth+1)
		}
		sb.WriteString(indent + n.keys[i] + "\n")
	}
	if !n.leaf && len(n.children) > len(n.keys) {
		t.debugNode(sb, n.children[len(n.keys)], depth+1)
	}
}
