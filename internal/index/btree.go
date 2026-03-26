package index

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

const Order = 32
const maxKeys = Order - 1
const minKeys = Order/2 - 1

// MaxBTreeKeys limits unique keys per B-Tree to prevent memory exhaustion.
const MaxBTreeKeys = 100000

type BTreeNode struct {
	keys     []string
	values   [][]string
	valSets  []map[string]struct{} // parallel dedup set: valSets[i] tracks entities in values[i]
	children []*BTreeNode
	leaf     bool
}

type BTree struct {
	root  *BTreeNode
	mu    sync.RWMutex
	count int
}

func New() *BTree {
	return &BTree{
		root: &BTreeNode{leaf: true},
	}
}

func (t *BTree) Insert(key string, entities []string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.insert(key, entities)
}

func (t *BTree) insert(key string, entities []string) error {
	existed := t.keyExists(t.root, key)
	if !existed && t.count >= MaxBTreeKeys {
		return fmt.Errorf("B-Tree key limit (%d) exceeded", MaxBTreeKeys)
	}

	t.insertNode(t.root, key, entities)
	if !existed {
		t.count++
	}

	if len(t.root.keys) > maxKeys {
		old := t.root
		t.root = &BTreeNode{
			children: []*BTreeNode{old},
		}
		t.splitChild(t.root, 0)
	}
	return nil
}

func (t *BTree) keyExists(n *BTreeNode, key string) bool {
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i] >= key
	})
	if i < len(n.keys) && n.keys[i] == key {
		return true
	}
	if n.leaf {
		return false
	}
	return t.keyExists(n.children[i], key)
}

func (t *BTree) insertNode(n *BTreeNode, key string, entities []string) bool {
	if n.leaf {
		i := sort.Search(len(n.keys), func(i int) bool {
			return n.keys[i] >= key
		})

		if i < len(n.keys) && n.keys[i] == key {
			// O(1) dedup per entity using valSets instead of O(n) dedup()
			for _, e := range entities {
				if _, exists := n.valSets[i][e]; !exists {
					n.valSets[i][e] = struct{}{}
					n.values[i] = append(n.values[i], e)
				}
			}
			return true
		}

		// Insert new key at position i
		n.keys = append(n.keys, "")
		n.values = append(n.values, nil)
		n.valSets = append(n.valSets, nil)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.values[i+1:], n.values[i:])
		copy(n.valSets[i+1:], n.valSets[i:])
		n.keys[i] = key
		n.values[i] = entities
		set := make(map[string]struct{}, len(entities))
		for _, e := range entities {
			set[e] = struct{}{}
		}
		n.valSets[i] = set
		return false
	}

	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i] >= key
	})

	if i < len(n.keys) && n.keys[i] == key {
		for _, e := range entities {
			if _, exists := n.valSets[i][e]; !exists {
				n.valSets[i][e] = struct{}{}
				n.values[i] = append(n.values[i], e)
			}
		}
		return true
	}

	if i > len(n.keys) {
		i = len(n.keys)
	}

	existed := t.insertNode(n.children[i], key, entities)

	if len(n.children[i].keys) > maxKeys {
		t.splitChild(n, i)
	}

	return existed
}

func (t *BTree) splitChild(n *BTreeNode, i int) {
	child := n.children[i]
	median := len(child.keys) / 2

	right := &BTreeNode{
		leaf: child.leaf,
	}

	right.keys = make([]string, len(child.keys)-median-1)
	right.values = make([][]string, len(child.keys)-median-1)
	right.valSets = make([]map[string]struct{}, len(child.keys)-median-1)
	copy(right.keys, child.keys[median+1:])
	copy(right.values, child.values[median+1:])
	copy(right.valSets, child.valSets[median+1:])

	if !child.leaf {
		right.children = make([]*BTreeNode, len(child.children)-median-1)
		copy(right.children, child.children[median+1:])
	}

	medianKey := child.keys[median]
	medianValue := child.values[median]
	medianSet := child.valSets[median]

	child.keys = child.keys[:median]
	child.values = child.values[:median]
	child.valSets = child.valSets[:median]
	if !child.leaf {
		child.children = child.children[:median+1]
	}

	n.keys = append(n.keys, "")
	n.values = append(n.values, nil)
	n.valSets = append(n.valSets, nil)
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.values[i+1:], n.values[i:])
	copy(n.valSets[i+1:], n.valSets[i:])
	n.keys[i] = medianKey
	n.values[i] = medianValue
	n.valSets[i] = medianSet

	n.children = append(n.children, nil)
	copy(n.children[i+2:], n.children[i+1:])
	n.children[i+1] = right
}

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

type RangeOp int

const (
	OpGt RangeOp = iota
	OpGte
	OpLt
	OpLte
)

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

func (t *BTree) rangeGT(value string, inclusive bool) []string {
	// Start with small capacity - grow dynamically to avoid over-allocation
	result := make([]string, 0, 64)
	t.collectGT(t.root, value, inclusive, &result)
	return result
}

// collectGT gathers all entries with key > value (or >= if inclusive).
// On internal nodes, recurse into children[i] with the same filter
// instead of calling collectAll — children[i] may contain keys <= value.
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

	// Recurse into children[i] — it may contain keys both above and below value
	if i < len(n.children) {
		t.collectGT(n.children[i], value, inclusive, result)
	}

	// Then separator keys[i..] and their right subtrees
	for ; i < len(n.keys); i++ {
		*result = append(*result, n.values[i]...)
		if i+1 < len(n.children) {
			t.collectAll(n.children[i+1], result)
		}
	}
}

func (t *BTree) rangeLT(value string, inclusive bool) []string {
	// Start with small capacity - grow dynamically to avoid over-allocation
	result := make([]string, 0, 64)
	t.collectLT(t.root, value, inclusive, &result)
	return result
}

// collectLT gathers all entries with key < value (or <= if inclusive).
// Recurse into children[i] instead of collectAll on children[j].
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

	// Recurse into children before i — they may contain keys both above and below value
	for j := 0; j < i && j < len(n.keys); j++ {
		if j < len(n.children) {
			t.collectLT(n.children[j], value, inclusive, result)
		}
		*result = append(*result, n.values[j]...)
	}

	// Recurse into children[i] — may contain keys both above and below value
	if i < len(n.children) {
		t.collectLT(n.children[i], value, inclusive, result)
	}
}

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

func (t *BTree) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.count
}

// CountSearch returns the number of entity entries for an exact key match.
// O(log n) — avoids materializing the result slice.
func (t *BTree) CountSearch(key string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.countSearch(t.root, key)
}

func (t *BTree) countSearch(n *BTreeNode, key string) int {
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i] >= key
	})

	if i < len(n.keys) && n.keys[i] == key {
		return len(n.values[i])
	}

	if n.leaf {
		return 0
	}

	return t.countSearch(n.children[i], key)
}

// CountRange returns the count of entity entries matching a range query.
// O(log n + k) where k is the number of matching entries — avoids materializing the result slice.
func (t *BTree) CountRange(op RangeOp, value string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var count int
	switch op {
	case OpGt:
		t.countGT(t.root, value, false, &count)
	case OpGte:
		t.countGT(t.root, value, true, &count)
	case OpLt:
		t.countLT(t.root, value, false, &count)
	case OpLte:
		t.countLT(t.root, value, true, &count)
	}
	return count
}

func (t *BTree) countGT(n *BTreeNode, value string, inclusive bool, count *int) {
	i := sort.Search(len(n.keys), func(i int) bool {
		if inclusive {
			return n.keys[i] >= value
		}
		return n.keys[i] > value
	})

	if n.leaf {
		for ; i < len(n.keys); i++ {
			*count += len(n.values[i])
		}
		return
	}

	if i < len(n.children) {
		t.countGT(n.children[i], value, inclusive, count)
	}

	for ; i < len(n.keys); i++ {
		*count += len(n.values[i])
		if i+1 < len(n.children) {
			t.countAll(n.children[i+1], count)
		}
	}
}

func (t *BTree) countLT(n *BTreeNode, value string, inclusive bool, count *int) {
	i := sort.Search(len(n.keys), func(i int) bool {
		if inclusive {
			return n.keys[i] > value
		}
		return n.keys[i] >= value
	})

	if n.leaf {
		for j := 0; j < i && j < len(n.keys); j++ {
			*count += len(n.values[j])
		}
		return
	}

	for j := 0; j < i && j < len(n.keys); j++ {
		if j < len(n.children) {
			t.countLT(n.children[j], value, inclusive, count)
		}
		*count += len(n.values[j])
	}

	if i < len(n.children) {
		t.countLT(n.children[i], value, inclusive, count)
	}
}

func (t *BTree) countAll(n *BTreeNode, count *int) {
	if n == nil {
		return
	}
	if n.leaf {
		for i := range n.values {
			*count += len(n.values[i])
		}
		return
	}
	for i := range n.keys {
		t.countAll(n.children[i], count)
		*count += len(n.values[i])
	}
	if len(n.children) > len(n.keys) {
		t.countAll(n.children[len(n.keys)], count)
	}
}

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

func (t *BTree) Remove(key string, entity string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.remove(t.root, key, entity)

	// Shrink tree if root is now empty internal node
	if !t.root.leaf && len(t.root.keys) == 0 {
		t.root = t.root.children[0]
	}
}

func (t *BTree) remove(n *BTreeNode, key string, entity string) {
	i := sort.Search(len(n.keys), func(i int) bool {
		return n.keys[i] >= key
	})

	found := i < len(n.keys) && n.keys[i] == key

	if found {
		// Remove from dedup set
		if n.valSets[i] != nil {
			delete(n.valSets[i], entity)
		}
		n.values[i] = removeStr(n.values[i], entity)
		if len(n.values[i]) == 0 {
			t.count--
			if n.leaf {
				n.keys = append(n.keys[:i], n.keys[i+1:]...)
				n.values = append(n.values[:i], n.values[i+1:]...)
				n.valSets = append(n.valSets[:i], n.valSets[i+1:]...)
			} else {
				// Replace with successor from right subtree
				succ := t.findMin(n.children[i+1])
				n.keys[i] = succ.keys[0]
				n.values[i] = succ.values[0]
				n.valSets[i] = succ.valSets[0]
				t.remove(n.children[i+1], succ.keys[0], succ.values[0][0])
				t.rebalance(n, i+1)
			}
		}
		return
	}

	if n.leaf {
		return
	}

	if i >= len(n.children) {
		return
	}

	t.remove(n.children[i], key, entity)
	t.rebalance(n, i)
}

// rebalance ensures n.children[childIdx] has at least minKeys+1 keys.
// Called after a deletion that may have caused underflow in the child.
func (t *BTree) rebalance(parent *BTreeNode, childIdx int) {
	child := parent.children[childIdx]
	if len(child.keys) >= minKeys+1 {
		return // Child is fine
	}

	// Try to borrow from left sibling
	if childIdx > 0 {
		left := parent.children[childIdx-1]
		if len(left.keys) > minKeys+1 {
			t.borrowFromLeft(parent, childIdx)
			return
		}
	}

	// Try to borrow from right sibling
	if childIdx < len(parent.children)-1 {
		right := parent.children[childIdx+1]
		if len(right.keys) > minKeys+1 {
			t.borrowFromRight(parent, childIdx)
			return
		}
	}

	// Must merge - pick a direction
	if childIdx > 0 {
		t.mergeWithLeft(parent, childIdx)
	} else {
		t.mergeWithRight(parent, childIdx)
	}
}

// borrowFromLeft takes the largest key from left sibling through parent.
func (t *BTree) borrowFromLeft(parent *BTreeNode, childIdx int) {
	left := parent.children[childIdx-1]
	child := parent.children[childIdx]

	// Rotate parent separator down into child
	child.keys = append([]string{parent.keys[childIdx-1]}, child.keys...)
	child.values = append([][]string{parent.values[childIdx-1]}, child.values...)
	child.valSets = append([]map[string]struct{}{parent.valSets[childIdx-1]}, child.valSets...)

	// Move left sibling's last child to child's first position
	if !left.leaf {
		lastChild := left.children[len(left.children)-1]
		left.children = left.children[:len(left.children)-1]
		child.children = append([]*BTreeNode{lastChild}, child.children...)
	}

	// Move left sibling's last key up to parent
	parent.keys[childIdx-1] = left.keys[len(left.keys)-1]
	parent.values[childIdx-1] = left.values[len(left.values)-1]
	parent.valSets[childIdx-1] = left.valSets[len(left.valSets)-1]
	left.keys = left.keys[:len(left.keys)-1]
	left.values = left.values[:len(left.values)-1]
	left.valSets = left.valSets[:len(left.valSets)-1]
}

// borrowFromRight takes the smallest key from right sibling through parent.
func (t *BTree) borrowFromRight(parent *BTreeNode, childIdx int) {
	right := parent.children[childIdx+1]
	child := parent.children[childIdx]

	// Rotate parent separator down into child
	child.keys = append(child.keys, parent.keys[childIdx])
	child.values = append(child.values, parent.values[childIdx])
	child.valSets = append(child.valSets, parent.valSets[childIdx])

	// Move right sibling's first child to child's last position
	if !right.leaf {
		firstChild := right.children[0]
		right.children = right.children[1:]
		child.children = append(child.children, firstChild)
	}

	// Move right sibling's first key up to parent
	parent.keys[childIdx] = right.keys[0]
	parent.values[childIdx] = right.values[0]
	parent.valSets[childIdx] = right.valSets[0]
	right.keys = right.keys[1:]
	right.values = right.values[1:]
	right.valSets = right.valSets[1:]
}

// mergeWithLeft merges child into left sibling with parent separator.
func (t *BTree) mergeWithLeft(parent *BTreeNode, childIdx int) {
	left := parent.children[childIdx-1]
	child := parent.children[childIdx]

	// Pull parent separator into left
	left.keys = append(left.keys, parent.keys[childIdx-1])
	left.values = append(left.values, parent.values[childIdx-1])
	left.valSets = append(left.valSets, parent.valSets[childIdx-1])

	// Append child's keys/values/valSets
	left.keys = append(left.keys, child.keys...)
	left.values = append(left.values, child.values...)
	left.valSets = append(left.valSets, child.valSets...)

	// Append child's children
	if !child.leaf {
		left.children = append(left.children, child.children...)
	}

	// Remove separator from parent
	parent.keys = append(parent.keys[:childIdx-1], parent.keys[childIdx:]...)
	parent.values = append(parent.values[:childIdx-1], parent.values[childIdx:]...)
	parent.valSets = append(parent.valSets[:childIdx-1], parent.valSets[childIdx:]...)
	parent.children = append(parent.children[:childIdx], parent.children[childIdx+1:]...)
}

// mergeWithRight merges child into itself with right sibling and parent separator.
func (t *BTree) mergeWithRight(parent *BTreeNode, childIdx int) {
	child := parent.children[childIdx]
	right := parent.children[childIdx+1]

	// Pull parent separator into child
	child.keys = append(child.keys, parent.keys[childIdx])
	child.values = append(child.values, parent.values[childIdx])
	child.valSets = append(child.valSets, parent.valSets[childIdx])

	// Append right's keys/values/valSets
	child.keys = append(child.keys, right.keys...)
	child.values = append(child.values, right.values...)
	child.valSets = append(child.valSets, right.valSets...)

	// Append right's children
	if !right.leaf {
		child.children = append(child.children, right.children...)
	}

	// Remove separator from parent
	parent.keys = append(parent.keys[:childIdx], parent.keys[childIdx+1:]...)
	parent.values = append(parent.values[:childIdx], parent.values[childIdx+1:]...)
	parent.valSets = append(parent.valSets[:childIdx], parent.valSets[childIdx+1:]...)
	parent.children = append(parent.children[:childIdx+1], parent.children[childIdx+2:]...)
}

func (t *BTree) findMin(n *BTreeNode) *BTreeNode {
	for !n.leaf {
		n = n.children[0]
	}
	return n
}

func removeStr(s []string, target string) []string {
	for i, v := range s {
		if v == target {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

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
