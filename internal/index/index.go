// Package index provides a higher-level index manager on top of B-Trees.
// It maintains per-attribute B-Tree indexes plus a full-text inverted index.
package index

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/user/atomdb/internal/atom"
)

// IndexManager maintains secondary indexes for fast attribute-based queries.
// Each attribute gets its own B-Tree mapping normalized values to entity references.
// A full-text inverted index is maintained for text/string attributes.
type IndexManager struct {
	indexes map[string]*BTree         // attribute name -> BTree
	textIdx map[string]*InvertedIndex // attribute name -> inverted index (text search)
	mu      sync.RWMutex
}

// InvertedIndex is a simple inverted index for full-text search.
// Maps tokens (lowercased words) to the set of entities containing them.
type InvertedIndex struct {
	index map[string]map[string]bool // token -> set of entities
	mu    sync.RWMutex
}

// StopWords is a set of common English words excluded from the inverted index.
var StopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "but": true, "by": true, "for": true, "if": true, "in": true,
	"into": true, "is": true, "it": true, "no": true, "not": true, "of": true,
	"on": true, "or": true, "such": true, "that": true, "the": true,
	"their": true, "then": true, "there": true, "these": true, "they": true,
	"this": true, "to": true, "was": true, "will": true, "with": true,
}

// NewIndexManager creates an empty index manager.
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*BTree),
		textIdx: make(map[string]*InvertedIndex),
	}
}

// newInvertedIndex creates an empty inverted index.
func newInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		index: make(map[string]map[string]bool),
	}
}

// IndexAtom adds an atom to all relevant indexes.
func (im *IndexManager) IndexAtom(a *atom.Atom) {
	if a.Type == "deleted" {
		return
	}

	key := normalizeValue(a.Value)
	entityKey := fmt.Sprintf("%s.%s", a.Entity, a.Attribute)

	im.mu.Lock()
	bt, ok := im.indexes[a.Attribute]
	if !ok {
		bt = New()
		im.indexes[a.Attribute] = bt
	}
	im.mu.Unlock()

	bt.Insert(key, []string{entityKey})

	// Text index for string values
	if a.Type == "string" {
		if s, ok := a.Value.(string); ok {
			im.indexText(a.Attribute, a.Entity, s)
		}
	}
}

// RemoveAtom removes an atom from all indexes.
func (im *IndexManager) RemoveAtom(a *atom.Atom) {
	key := normalizeValue(a.Value)
	entityKey := fmt.Sprintf("%s.%s", a.Entity, a.Attribute)

	im.mu.RLock()
	bt, ok := im.indexes[a.Attribute]
	im.mu.RUnlock()

	if ok {
		bt.Remove(key, entityKey)
	}

	// Remove from text index
	if a.Type == "string" {
		if s, ok := a.Value.(string); ok {
			im.removeText(a.Attribute, a.Entity, s)
		}
	}
}

// RebuildFromAtoms rebuilds all indexes from a set of atoms.
// Called on startup after loading from disk.
func (im *IndexManager) RebuildFromAtoms(atoms map[string]map[string]*atom.Atom) {
	im.mu.Lock()
	im.indexes = make(map[string]*BTree)
	im.textIdx = make(map[string]*InvertedIndex)
	im.mu.Unlock()

	for _, attrs := range atoms {
		for _, a := range attrs {
			im.IndexAtom(a)
		}
	}
}

// Search performs an exact match query using the index.
// Returns entity.attribute keys, or nil if no index exists for the attribute.
func (im *IndexManager) Search(attribute, value string) []string {
	im.mu.RLock()
	bt, ok := im.indexes[attribute]
	im.mu.RUnlock()

	if !ok {
		return nil
	}

	return bt.Search(normalizeValue(value))
}

// RangeSearch performs a range query using the index.
func (im *IndexManager) RangeSearch(attribute string, op RangeOp, value string) []string {
	im.mu.RLock()
	bt, ok := im.indexes[attribute]
	im.mu.RUnlock()

	if !ok {
		return nil
	}

	return bt.RangeQuery(op, normalizeValue(value))
}

// HasIndex returns true if an index exists for the given attribute.
func (im *IndexManager) HasIndex(attribute string) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	_, ok := im.indexes[attribute]
	return ok
}

// IndexedAttributes returns the list of attributes that have indexes.
func (im *IndexManager) IndexedAttributes() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()
	attrs := make([]string, 0, len(im.indexes))
	for a := range im.indexes {
		attrs = append(attrs, a)
	}
	sort.Strings(attrs)
	return attrs
}

// IndexStats returns stats for a given attribute's index.
func (im *IndexManager) IndexStats(attribute string) (keyCount int, hasTextIndex bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if bt, ok := im.indexes[attribute]; ok {
		keyCount = bt.Count()
	}
	_, hasTextIndex = im.textIdx[attribute]
	return
}

// TotalKeys returns the total number of index entries across all attributes.
func (im *IndexManager) TotalKeys() int {
	im.mu.RLock()
	defer im.mu.RUnlock()
	total := 0
	for _, bt := range im.indexes {
		total += bt.Count()
	}
	return total
}

// indexText tokenizes a string value and adds tokens to the inverted index.
func (im *IndexManager) indexText(attribute, entity, text string) {
	im.mu.Lock()
	ii, ok := im.textIdx[attribute]
	if !ok {
		ii = newInvertedIndex()
		im.textIdx[attribute] = ii
	}
	im.mu.Unlock()

	entityKey := fmt.Sprintf("%s.%s", entity, attribute)
	tokens := tokenize(text)
	ii.Add(entityKey, tokens)
}

// removeText removes entity from inverted index for given text tokens.
func (im *IndexManager) removeText(attribute, entity, text string) {
	im.mu.RLock()
	ii, ok := im.textIdx[attribute]
	im.mu.RUnlock()

	if !ok {
		return
	}

	entityKey := fmt.Sprintf("%s.%s", entity, attribute)
	tokens := tokenize(text)
	ii.Remove(entityKey, tokens)
}

// FullTextSearch searches for entities containing a word in a text attribute.
func (im *IndexManager) FullTextSearch(attribute, word string) []string {
	im.mu.RLock()
	ii, ok := im.textIdx[attribute]
	im.mu.RUnlock()

	if !ok {
		return nil
	}

	token := strings.ToLower(word)
	return ii.Search(token)
}

// Add adds entity to the inverted index for the given tokens.
func (ii *InvertedIndex) Add(entity string, tokens []string) {
	ii.mu.Lock()
	defer ii.mu.Unlock()

	for _, token := range tokens {
		if ii.index[token] == nil {
			ii.index[token] = make(map[string]bool)
		}
		ii.index[token][entity] = true
	}
}

// Remove removes entity from the inverted index for the given tokens.
func (ii *InvertedIndex) Remove(entity string, tokens []string) {
	ii.mu.Lock()
	defer ii.mu.Unlock()

	for _, token := range tokens {
		if entities, ok := ii.index[token]; ok {
			delete(entities, entity)
			if len(entities) == 0 {
				delete(ii.index, token)
			}
		}
	}
}

// Search returns all entities that contain the given token.
func (ii *InvertedIndex) Search(token string) []string {
	ii.mu.RLock()
	defer ii.mu.RUnlock()

	entities, ok := ii.index[token]
	if !ok {
		return nil
	}

	result := make([]string, 0, len(entities))
	for e := range entities {
		result = append(result, e)
	}
	return result
}

// TokenCount returns the number of unique tokens in the index.
func (ii *InvertedIndex) TokenCount() int {
	ii.mu.RLock()
	defer ii.mu.RUnlock()
	return len(ii.index)
}

// tokenize splits text into lowercase tokens, excluding stop words.
func tokenize(text string) []string {
	// Split on whitespace and common delimiters
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == ',' || r == '.' ||
			r == '!' || r == '?' || r == ';' || r == ':' || r == '"' || r == '\''
	})

	var tokens []string
	seen := make(map[string]bool)
	for _, f := range fields {
		token := strings.ToLower(strings.TrimSpace(f))
		if len(token) < 2 {
			continue
		}
		if StopWords[token] {
			continue
		}
		if !seen[token] {
			seen[token] = true
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// normalizeValue converts a Go value to a normalized string key for indexing.
// Numbers are normalized to a consistent format. Strings are stored as-is.
// This ensures that 30 (int) and 30.0 (float64) produce the same key.
func normalizeValue(v interface{}) string {
	switch val := v.(type) {
	case float64:
		// Normalize: 30.0 -> "30", 30.5 -> "30.5"
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case float32:
		return normalizeValue(float64(val))
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case string:
		return val
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}
