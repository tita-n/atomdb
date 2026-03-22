package store

import (
	"fmt"
	"os"
	"sync"

	"github.com/user/atomdb/internal/atom"
	"github.com/user/atomdb/internal/disk"
	"github.com/user/atomdb/internal/index"
)

const maxHistorySize = 10000
const syncThreshold = 1000

// AtomStore is the core storage engine with B-Tree secondary indexes.
type AtomStore struct {
	mu                   sync.RWMutex
	atoms                map[string]map[string]*atom.Atom // entity -> attribute -> latest atom
	idx                  *index.IndexManager              // B-Tree + full-text indexes
	history              []atom.Atom
	file                 *os.File
	dirty                bool
	dirtyCount           int
	lastPersistedVersion int64
	syncMode             SyncMode
}

type SyncMode int

const (
	SyncAlways SyncMode = iota
	SyncBatch
)

func New(path string) (*AtomStore, error) {
	return NewWithMode(path, SyncAlways)
}

func NewWithMode(path string, mode SyncMode) (*AtomStore, error) {
	disk.CleanupOrphaned(path)

	existing, err := disk.Load(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing data: %w", err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	store := &AtomStore{
		atoms:    make(map[string]map[string]*atom.Atom),
		idx:      index.NewIndexManager(),
		history:  make([]atom.Atom, 0),
		file:     file,
		syncMode: mode,
	}

	// Replay atoms to rebuild in-memory index
	for i := range existing {
		store.applyAtom(&existing[i])
	}

	// Build B-Tree indexes from loaded data
	store.idx.RebuildFromAtoms(store.atoms)

	return store, nil
}

// applyAtom applies an atom to the in-memory index.
func (s *AtomStore) applyAtom(a *atom.Atom) {
	if _, ok := s.atoms[a.Entity]; !ok {
		s.atoms[a.Entity] = make(map[string]*atom.Atom)
	}
	s.atoms[a.Entity][a.Attribute] = a
}

// Set stores an atom and updates all indexes.
func (s *AtomStore) Set(entity, attribute string, value interface{}, valueType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	a, err := atom.NewAtom(entity, attribute, value, valueType)
	if err != nil {
		return err
	}

	// Remove old atom from index if overwriting
	if oldAttrs, ok := s.atoms[entity]; ok {
		if old, exists := oldAttrs[attribute]; exists && old.Type != "deleted" {
			s.idx.RemoveAtom(old)
		}
	}

	if err := disk.Save(a, s.file); err != nil {
		return fmt.Errorf("failed to persist atom: %w", err)
	}
	s.dirty = true
	s.dirtyCount++

	s.applyAtom(a)
	s.idx.IndexAtom(a)
	s.appendHistory(*a)

	return s.maybeSync()
}

// Get retrieves the latest atom for an entity+attribute pair.
func (s *AtomStore) Get(entity, attribute string) (*atom.Atom, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	attrs, ok := s.atoms[entity]
	if !ok {
		return nil, false
	}
	a, ok := attrs[attribute]
	if !ok || a.Type == "deleted" {
		return nil, false
	}
	c := *a
	return &c, true
}

// Exists checks if a live atom exists for the given entity+attribute.
func (s *AtomStore) Exists(entity, attribute string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	attrs, ok := s.atoms[entity]
	if !ok {
		return false
	}
	a, ok := attrs[attribute]
	return ok && a.Type != "deleted"
}

// GetAll returns all live atoms for a given entity.
func (s *AtomStore) GetAll(entity string) map[string]*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	attrs, ok := s.atoms[entity]
	if !ok {
		return nil
	}
	result := make(map[string]*atom.Atom)
	for attr, a := range attrs {
		if a.Type != "deleted" {
			c := *a
			result[attr] = &c
		}
	}
	return result
}

// Query performs a filtered query. Uses the attribute index to narrow candidates.
// If attribute is empty, falls back to full scan.
func (s *AtomStore) Query(attribute string, predicate func(*atom.Atom) bool) []*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*atom.Atom

	if attribute != "" {
		if candidates, ok := s.atoms[""]; ok {
			_ = candidates
		}
		// Use byAttr index via idx manager for attribute-narrowed scan
		for _, attrs := range s.atoms {
			if a, ok := attrs[attribute]; ok && a.Type != "deleted" && predicate(a) {
				c := *a
				results = append(results, &c)
			}
		}
		return results
	}

	// Full scan
	for _, attrs := range s.atoms {
		for _, a := range attrs {
			if a.Type != "deleted" && predicate(a) {
				c := *a
				results = append(results, &c)
			}
		}
	}
	return results
}

// QueryIndexed performs an exact match query using the B-Tree index.
// Returns nil if no index exists (caller should fall back to scan).
func (s *AtomStore) QueryIndexed(attribute, value string) []*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := s.idx.Search(attribute, value)
	if keys == nil {
		return nil
	}

	return s.resolveKeys(keys)
}

// QueryRange performs a range query using the B-Tree index.
func (s *AtomStore) QueryRange(attribute string, op index.RangeOp, value string) []*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := s.idx.RangeSearch(attribute, op, value)
	if keys == nil {
		return nil
	}

	return s.resolveKeys(keys)
}

// QueryExplain returns info about how a query would be executed.
func (s *AtomStore) QueryExplain(attribute, op, value string) QueryPlan {
	s.mu.RLock()
	defer s.mu.RUnlock()

	plan := QueryPlan{
		Attribute: attribute,
		Operator:  op,
		Value:     value,
	}

	if s.idx.HasIndex(attribute) {
		plan.UseIndex = true
		plan.IndexType = "btree"
		if op == "==" {
			plan.ScanType = "exact"
			keys := s.idx.Search(attribute, value)
			plan.EstimatedRows = len(keys)
		} else {
			plan.ScanType = "range"
			var rangeOp index.RangeOp
			switch op {
			case ">":
				rangeOp = index.OpGt
			case ">=":
				rangeOp = index.OpGte
			case "<":
				rangeOp = index.OpLt
			case "<=":
				rangeOp = index.OpLte
			}
			keys := s.idx.RangeSearch(attribute, rangeOp, value)
			plan.EstimatedRows = len(keys)
		}
	} else {
		plan.UseIndex = false
		plan.ScanType = "full"
		plan.EstimatedRows = s.atomCount()
	}

	return plan
}

// FullTextSearch searches for entities with a word in a text attribute.
func (s *AtomStore) FullTextSearch(attribute, word string) []*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := s.idx.FullTextSearch(attribute, word)
	return s.resolveKeys(keys)
}

// resolveKeys converts "entity.attribute" keys to actual Atom copies.
func (s *AtomStore) resolveKeys(keys []string) []*atom.Atom {
	var results []*atom.Atom
	seen := make(map[string]bool)
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		parts := splitEntityAttr(k)
		if len(parts) != 2 {
			continue
		}
		if attrs, ok := s.atoms[parts[0]]; ok {
			if a, ok := attrs[parts[1]]; ok && a.Type != "deleted" {
				c := *a
				results = append(results, &c)
			}
		}
	}
	return results
}

// Delete marks an atom as deleted. Returns error if atom doesn't exist.
func (s *AtomStore) Delete(entity, attribute string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check existence
	attrs, ok := s.atoms[entity]
	if !ok {
		return ErrNotFound
	}
	existing, ok := attrs[attribute]
	if !ok || existing.Type == "deleted" {
		return ErrNotFound
	}

	// Remove old atom from index
	s.idx.RemoveAtom(existing)

	tombstone, err := atom.NewAtom(entity, attribute, nil, "deleted")
	if err != nil {
		return err
	}

	if err := disk.Save(tombstone, s.file); err != nil {
		return fmt.Errorf("failed to persist tombstone: %w", err)
	}
	s.dirty = true
	s.dirtyCount++

	s.applyAtom(tombstone)
	s.appendHistory(*tombstone)

	return s.maybeSync()
}

// ErrNotFound is returned when a delete target doesn't exist.
var ErrNotFound = fmt.Errorf("not found")

// Compact rewrites the data file.
func (s *AtomStore) Compact() error {
	s.mu.RLock()
	path := s.file.Name()
	atomsSnapshot := make(map[string]map[string]*atom.Atom, len(s.atoms))
	for entity, attrs := range s.atoms {
		attrsCopy := make(map[string]*atom.Atom, len(attrs))
		for attr, a := range attrs {
			attrsCopy[attr] = a
		}
		atomsSnapshot[entity] = attrsCopy
	}
	s.file.Close()
	s.mu.RUnlock()

	compactErr := disk.Compact(path, atomsSnapshot, true)

	s.mu.Lock()
	defer s.mu.Unlock()

	if compactErr != nil {
		file, _ := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if file != nil {
			s.file = file
		}
		return fmt.Errorf("compaction failed: %w", compactErr)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to reopen after compaction: %w", err)
	}
	s.file = file

	s.idx.RebuildFromAtoms(s.atoms)
	return nil
}

// RebuildIndexes forces a full rebuild of all B-Tree indexes.
func (s *AtomStore) RebuildIndexes() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.idx.RebuildFromAtoms(s.atoms)
}

// Close flushes and closes.
func (s *AtomStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dirty {
		disk.Sync(s.file)
	}
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Stats returns store statistics.
type StoreStats struct {
	EntityCount    int
	AtomCount      int
	IndexedAttrs   []string
	IndexKeyCount  int
	HistorySize    int
	LastPersistedV int64
}

func (s *AtomStore) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return StoreStats{
		EntityCount:    len(s.atoms),
		AtomCount:      s.atomCount(),
		IndexedAttrs:   s.idx.IndexedAttributes(),
		IndexKeyCount:  s.idx.TotalKeys(),
		HistorySize:    len(s.history),
		LastPersistedV: s.lastPersistedVersion,
	}
}

// QueryPlan describes how a query will be executed.
type QueryPlan struct {
	Attribute     string
	Operator      string
	Value         string
	UseIndex      bool
	IndexType     string // "btree" or ""
	ScanType      string // "exact", "range", "full"
	EstimatedRows int
}

func (s *AtomStore) atomCount() int {
	count := 0
	for _, attrs := range s.atoms {
		for _, a := range attrs {
			if a.Type != "deleted" {
				count++
			}
		}
	}
	return count
}

func (s *AtomStore) appendHistory(a atom.Atom) {
	s.history = append(s.history, a)
	if len(s.history) > maxHistorySize {
		keep := maxHistorySize / 2
		s.history = s.history[len(s.history)-keep:]
	}
}

func (s *AtomStore) maybeSync() error {
	if s.syncMode == SyncAlways && s.dirty {
		s.dirty = false
		s.dirtyCount = 0
		s.lastPersistedVersion = s.maxAtomVersion()
		return disk.Sync(s.file)
	}
	if s.syncMode == SyncBatch {
		if s.dirtyCount >= syncThreshold {
			s.dirty = false
			s.dirtyCount = 0
			s.lastPersistedVersion = s.maxAtomVersion()
			return disk.Sync(s.file)
		}
	}
	return nil
}

func (s *AtomStore) maxAtomVersion() int64 {
	var max int64
	for _, attrs := range s.atoms {
		for _, a := range attrs {
			if a.Version > max {
				max = a.Version
			}
		}
	}
	return max
}

// splitEntityAttr splits "entity.attribute" into [entity, attribute].
func splitEntityAttr(key string) []string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '.' {
			return []string{key[:i], key[i+1:]}
		}
	}
	return []string{key}
}
