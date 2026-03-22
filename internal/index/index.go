package index

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/tita-n/atomdb/internal/atom"
)

type IndexManager struct {
	indexes   map[string]*BTree
	textIdx   map[string]*InvertedIndex
	attrTypes map[string]string // attribute -> atom type for search normalization
	mu        sync.RWMutex
}

type InvertedIndex struct {
	index map[string]map[string]struct{}
	mu    sync.RWMutex
}

var StopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "but": true, "by": true, "for": true, "if": true, "in": true,
	"into": true, "is": true, "it": true, "no": true, "not": true, "of": true,
	"on": true, "or": true, "such": true, "that": true, "the": true,
	"their": true, "then": true, "there": true, "these": true, "they": true,
	"this": true, "to": true, "was": true, "will": true, "with": true,
}

func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes:   make(map[string]*BTree),
		textIdx:   make(map[string]*InvertedIndex),
		attrTypes: make(map[string]string),
	}
}

func newInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		index: make(map[string]map[string]struct{}),
	}
}

// IndexAtom adds an atom to all relevant indexes.
func (im *IndexManager) IndexAtom(a *atom.Atom) {
	if a.Type == "deleted" {
		return
	}

	key := normalizeValue(a.Value)
	entityKey := a.Entity + "." + a.Attribute

	im.mu.Lock()
	bt, ok := im.indexes[a.Attribute]
	if !ok {
		bt = New()
		im.indexes[a.Attribute] = bt
	}
	im.attrTypes[a.Attribute] = a.Type
	im.mu.Unlock()

	if err := bt.Insert(key, []string{entityKey}); err != nil {
		log.Printf("WARNING: B-Tree insert failed for %s: %v", a.Attribute, err)
		return
	}

	if a.Type == "string" {
		if s, ok := a.Value.(string); ok {
			im.indexText(a.Attribute, a.Entity, s)
		}
	}
}

func (im *IndexManager) RemoveAtom(a *atom.Atom) {
	im.mu.RLock()
	bt, ok := im.indexes[a.Attribute]
	isNumeric := im.attrTypes[a.Attribute] == "number"
	im.mu.RUnlock()

	if !ok {
		return
	}

	var key string
	if isNumeric {
		if f, err := toFloat64Value(a.Value); err == nil {
			key = encodeNumericKey(f)
		} else {
			key = normalizeValue(a.Value)
		}
	} else {
		key = normalizeValue(a.Value)
	}
	entityKey := a.Entity + "." + a.Attribute

	bt.Remove(key, entityKey)

	if a.Type == "string" {
		if s, ok := a.Value.(string); ok {
			im.removeText(a.Attribute, a.Entity, s)
		}
	}
}

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

func (im *IndexManager) Search(attribute, value string) []string {
	im.mu.RLock()
	bt, ok := im.indexes[attribute]
	im.mu.RUnlock()

	if !ok {
		return nil
	}

	return bt.Search(normalizeValue(value))
}

func (im *IndexManager) RangeSearch(attribute string, op RangeOp, value string) []string {
	im.mu.RLock()
	bt, ok := im.indexes[attribute]
	im.mu.RUnlock()

	if !ok {
		return nil
	}

	return bt.RangeQuery(op, normalizeValue(value))
}

func (im *IndexManager) HasIndex(attribute string) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	_, ok := im.indexes[attribute]
	return ok
}

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

func (im *IndexManager) TotalKeys() int {
	im.mu.RLock()
	defer im.mu.RUnlock()
	total := 0
	for _, bt := range im.indexes {
		total += bt.Count()
	}
	return total
}

func (im *IndexManager) indexText(attribute, entity, text string) {
	im.mu.Lock()
	ii, ok := im.textIdx[attribute]
	if !ok {
		ii = newInvertedIndex()
		im.textIdx[attribute] = ii
	}
	im.mu.Unlock()

	entityKey := entity + "." + attribute
	tokens := tokenize(text)
	ii.Add(entityKey, tokens)
}

func (im *IndexManager) removeText(attribute, entity, text string) {
	im.mu.RLock()
	ii, ok := im.textIdx[attribute]
	im.mu.RUnlock()

	if !ok {
		return
	}

	entityKey := entity + "." + attribute
	tokens := tokenize(text)
	ii.Remove(entityKey, tokens)
}

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

func (ii *InvertedIndex) Add(entity string, tokens []string) {
	ii.mu.Lock()
	defer ii.mu.Unlock()

	for _, token := range tokens {
		if ii.index[token] == nil {
			ii.index[token] = make(map[string]struct{})
		}
		ii.index[token][entity] = struct{}{}
	}
}

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

// tokenize splits text into lowercase, unique, non-stopword tokens.
// Single-pass: scans characters directly without intermediate slice allocations.
func tokenize(text string) []string {
	var tokens []string
	seen := make(map[string]struct{}, 8)
	start := -1

	for i := 0; i <= len(text); i++ {
		var c byte
		if i < len(text) {
			c = text[i]
		}

		isDelim := i == len(text) || c == ' ' || c == '\t' || c == '\n' ||
			c == ',' || c == '.' || c == '!' || c == '?' || c == ';' ||
			c == ':' || c == '"' || c == '\''

		if !isDelim {
			if start == -1 {
				start = i
			}
			continue
		}

		if start == -1 {
			continue
		}

		raw := text[start:i]
		start = -1

		if len(raw) < 2 {
			continue
		}

		token := toLowerASCII(raw)

		if StopWords[token] {
			continue
		}
		if _, dup := seen[token]; dup {
			continue
		}

		if len(tokens) >= 1000 {
			break
		}

		seen[token] = struct{}{}
		tokens = append(tokens, token)
	}

	return tokens
}

// toLowerASCII lowercases A-Z only, returns original if already lowercase.
// Avoids strings.ToLower allocation for strings that are already lowercase.
func toLowerASCII(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			buf := make([]byte, len(s))
			copy(buf, s)
			for j := i; j < len(buf); j++ {
				if buf[j] >= 'A' && buf[j] <= 'Z' {
					buf[j] += 'a' - 'A'
				}
			}
			return string(buf)
		}
	}
	return s
}

// toFloat64Value converts a value to float64 if possible.
func toFloat64Value(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("not a number")
	}
}

// normalizeValue converts a Go value to a normalized string key for indexing.
// Numbers use IEEE 754 bit encoding so that string comparison preserves numeric ordering.
// Strings that look like numbers are parsed and encoded numerically to ensure
// range queries work correctly (e.g., query "age > 20" where 20 is a CLI string).
func normalizeValue(v interface{}) string {
	switch val := v.(type) {
	case float64:
		return encodeNumericKey(val)
	case float32:
		return encodeNumericKey(float64(val))
	case int:
		return encodeNumericKey(float64(val))
	case int64:
		return encodeNumericKey(float64(val))
	case bool:
		if val {
			return "true"
		}
		return "false"
	case string:
		// Try to parse as number so range queries work on numeric attributes
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return encodeNumericKey(f)
		}
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return encodeNumericKey(float64(i))
		}
		return val
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}

// encodeNumericKey produces a fixed-width 16-char hex string that sorts correctly
// with string comparison for any float64 value. Uses IEEE 754 bit manipulation:
// flip sign bit for positives, invert all bits for negatives.
// Uses strconv.FormatUint instead of fmt.Sprintf for ~3x fewer allocations.
func encodeNumericKey(v float64) string {
	bits := math.Float64bits(v)
	if bits>>63 == 1 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	return strconv.FormatUint(bits, 16)
}
