package store

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/tita-n/atomdb/internal/atom"
	"github.com/tita-n/atomdb/internal/disk"
	"github.com/tita-n/atomdb/internal/index"
)

const maxHistorySize = 10000
const syncThreshold = 1000

const (
	DefaultMaxEntities = 100000
	DefaultMaxAtoms    = 1000000
	DefaultMaxAttrs    = 1000
	DefaultMaxIndexes  = 100
)

type StoreLimits struct {
	MaxEntities int
	MaxAtoms    int
	MaxAttrs    int
	MaxIndexes  int
}

func DefaultLimits() StoreLimits {
	return StoreLimits{
		MaxEntities: DefaultMaxEntities,
		MaxAtoms:    DefaultMaxAtoms,
		MaxAttrs:    DefaultMaxAttrs,
		MaxIndexes:  DefaultMaxIndexes,
	}
}

type AtomStore struct {
	mu                   sync.RWMutex
	atoms                map[string]map[string]*atom.Atom
	idx                  *index.IndexManager
	constraints          *ConstraintManager
	history              []atom.Atom
	historyWriteIdx      int
	file                 *os.File
	dirty                bool
	dirtyCount           int
	lastPersistedVersion int64
	syncMode             SyncMode
	limits               StoreLimits
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
	return NewWithModeAndLimits(path, mode, DefaultLimits())
}

func NewWithModeAndLimits(path string, mode SyncMode, limits StoreLimits) (*AtomStore, error) {
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
		atoms:       make(map[string]map[string]*atom.Atom),
		idx:         index.NewIndexManager(),
		constraints: NewConstraintManager(),
		history:     make([]atom.Atom, 0),
		file:        file,
		syncMode:    mode,
		limits:      limits,
	}

	// Replay atoms to rebuild in-memory index and populate history
	for i := range existing {
		store.applyAtom(&existing[i])
		store.appendHistory(existing[i])
	}

	// Build B-Tree indexes from loaded data
	store.idx.RebuildFromAtoms(store.atoms)

	return store, nil
}

func (s *AtomStore) applyAtom(a *atom.Atom) {
	if _, ok := s.atoms[a.Entity]; !ok {
		s.atoms[a.Entity] = make(map[string]*atom.Atom)
	}
	s.atoms[a.Entity][a.Attribute] = a
}

func (s *AtomStore) Set(entity, attribute string, value interface{}, valueType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check entity limit
	if _, exists := s.atoms[entity]; !exists {
		if len(s.atoms) >= s.limits.MaxEntities {
			return fmt.Errorf("maximum entity count (%d) exceeded", s.limits.MaxEntities)
		}
	}

	// Check attribute limit
	if attrs, exists := s.atoms[entity]; exists {
		if _, attrExists := attrs[attribute]; !attrExists {
			if len(attrs) >= s.limits.MaxAttrs {
				return fmt.Errorf("maximum attributes per entity (%d) exceeded for entity %q", s.limits.MaxAttrs, entity)
			}
		}
	}

	// Check total atom limit (only for new atoms, not updates)
	isNewAtom := true
	if attrs, ok := s.atoms[entity]; ok {
		if _, exists := attrs[attribute]; exists {
			isNewAtom = false
		}
	}
	if isNewAtom && s.atomCount() >= s.limits.MaxAtoms {
		return fmt.Errorf("maximum atom count (%d) exceeded", s.limits.MaxAtoms)
	}

	// Validate constraints
	typeName := entity
	if idx := strings.Index(entity, ":"); idx > 0 {
		typeName = entity[:idx]
	}
	if err := s.constraints.Validate(typeName, attribute, value, entity); err != nil {
		return err
	}

	a, err := atom.NewAtom(entity, attribute, value, valueType)
	if err != nil {
		return err
	}

	// Remove old atom from index if overwriting
	if oldAttrs, ok := s.atoms[entity]; ok {
		if old, exists := oldAttrs[attribute]; exists && old.Type != "deleted" {
			s.idx.RemoveAtom(old)
			// Remove old unique tracking
			valStr := fmt.Sprintf("%v", old.Value)
			s.constraints.RemoveUnique(typeName, attribute, valStr)
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

	// Track unique value
	valStr := fmt.Sprintf("%v", value)
	s.constraints.TrackUnique(typeName, attribute, valStr, entity)

	return s.maybeSync()
}

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

// EntityExists checks if any live attribute exists for the given entity.
func (s *AtomStore) EntityExists(entity string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	attrs, ok := s.atoms[entity]
	if !ok {
		return false
	}
	for _, a := range attrs {
		if a.Type != "deleted" {
			return true
		}
	}
	return false
}

// InsertIfNotExists atomically inserts all fields for a new entity.
// Returns error if entity already exists (race-safe duplicate detection).
func (s *AtomStore) InsertIfNotExists(entity string, fields map[string]interface{}, typeFn func(v interface{}) string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if entity already exists (any live attribute)
	if attrs, ok := s.atoms[entity]; ok {
		for _, a := range attrs {
			if a.Type != "deleted" {
				return fmt.Errorf("duplicate entity: %q already exists", entity)
			}
		}
	}

	// Check entity limit
	if len(s.atoms) >= s.limits.MaxEntities {
		return fmt.Errorf("maximum entity count (%d) exceeded", s.limits.MaxEntities)
	}

	// Validate constraints and limits before writing
	typeName := entity
	if idx := strings.Index(entity, ":"); idx > 0 {
		typeName = entity[:idx]
	}

	if len(fields) > s.limits.MaxAttrs {
		return fmt.Errorf("too many attributes (%d) for entity %q (max %d)", len(fields), entity, s.limits.MaxAttrs)
	}

	for attr, val := range fields {
		if err := s.constraints.Validate(typeName, attr, val, entity); err != nil {
			return err
		}
	}

	// Check total atom limit
	newCount := len(fields)
	if existing, ok := s.atoms[entity]; ok {
		for _, a := range existing {
			if a.Type != "deleted" {
				newCount--
			}
		}
	}
	if s.atomCount()+newCount > s.limits.MaxAtoms {
		return fmt.Errorf("maximum atom count (%d) exceeded", s.limits.MaxAtoms)
	}

	// All checks passed — write atoms
	for attr, val := range fields {
		valType := typeFn(val)
		a, err := atom.NewAtom(entity, attr, val, valType)
		if err != nil {
			return err
		}
		if err := disk.Save(a, s.file); err != nil {
			return fmt.Errorf("failed to persist atom: %w", err)
		}
		s.applyAtom(a)
		s.idx.IndexAtom(a)
		s.appendHistory(*a)
		s.constraints.TrackUnique(typeName, attr, fmt.Sprintf("%v", val), entity)
	}

	s.dirty = true
	s.dirtyCount += len(fields)
	return s.maybeSync()
}

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

// Query performs a filtered query scanning all atoms matching the attribute.
func (s *AtomStore) Query(attribute string, predicate func(*atom.Atom) bool) []*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*atom.Atom

	if attribute != "" {
		for _, attrs := range s.atoms {
			if a, ok := attrs[attribute]; ok && a.Type != "deleted" && predicate(a) {
				c := *a
				results = append(results, &c)
			}
		}
		return results
	}

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

// HasIndex returns true if an index exists for the given attribute.
func (s *AtomStore) HasIndex(attribute string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idx.HasIndex(attribute)
}

// QueryEntities finds all entity IDs of a given type matching conditions.
// Uses B-Tree indexes when available — intersects results from multiple indexed
// conditions for optimal filtering, falls back to entity-grouped scan otherwise.
func (s *AtomStore) QueryEntities(typeName string, conditions []Condition) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := typeName + ":"

	if len(conditions) == 0 {
		return s.scanEntitiesByPrefix(prefix)
	}

	// Collect all indexed conditions and their results
	var indexedResults [][]string
	var unindexed []Condition

	for _, cond := range conditions {
		if !s.idx.HasIndex(cond.Field) {
			unindexed = append(unindexed, cond)
			continue
		}

		var keys []string
		switch cond.Operator {
		case "==":
			keys = s.idx.SearchByValue(cond.Field, cond.Value)
		case ">", ">=", "<", "<=":
			rangeOp := operatorToRangeOp(cond.Operator)
			keys = s.idx.RangeSearchByValue(cond.Field, rangeOp, cond.Value)
		default:
			unindexed = append(unindexed, cond)
			continue
		}

		if len(keys) == 0 {
			return nil // indexed lookup found nothing
		}
		indexedResults = append(indexedResults, keys)
	}

	// If we have indexed results, intersect them
	if len(indexedResults) > 0 {
		keys := intersectKeySets(indexedResults)
		if len(keys) == 0 {
			return nil
		}
		return s.resolveEntityKeysFiltered(keys, prefix, unindexed)
	}

	// No usable index — full scan fallback grouped by entity
	return s.scanEntitiesWithConditions(prefix, conditions)
}

// intersectKeySets returns entity keys present in ALL sets.
// Keys are in "entity.attribute" format — we extract just the entity part
// for comparison, then return the full keys from the first set.
func intersectKeySets(sets [][]string) []string {
	if len(sets) == 0 {
		return nil
	}
	if len(sets) == 1 {
		return sets[0]
	}

	// Use smallest set as base for intersection
	minIdx := 0
	for i := 1; i < len(sets); i++ {
		if len(sets[i]) < len(sets[minIdx]) {
			minIdx = i
		}
	}

	// Build entity-ID set from smallest
	entityToKey := make(map[string]string, len(sets[minIdx]))
	base := make(map[string]struct{}, len(sets[minIdx]))
	for _, k := range sets[minIdx] {
		parts := splitEntityAttr(k)
		entity := parts[0]
		entityToKey[entity] = k
		base[entity] = struct{}{}
	}

	// Intersect with each other set
	for i := 0; i < len(sets); i++ {
		if i == minIdx {
			continue
		}
		next := make(map[string]struct{}, len(sets[i]))
		for _, k := range sets[i] {
			parts := splitEntityAttr(k)
			next[parts[0]] = struct{}{}
		}
		for entity := range base {
			if _, ok := next[entity]; !ok {
				delete(base, entity)
			}
		}
	}

	result := make([]string, 0, len(base))
	for entity := range base {
		// Return the key from the first set (arbitrary but consistent)
		result = append(result, entityToKey[entity])
	}
	return result
}

// scanEntitiesByPrefix returns all live entity IDs with the given prefix.
func (s *AtomStore) scanEntitiesByPrefix(prefix string) []string {
	var results []string
	for entity, attrs := range s.atoms {
		if !strings.HasPrefix(entity, prefix) {
			continue
		}
		for _, a := range attrs {
			if a.Type != "deleted" {
				results = append(results, entity)
				break
			}
		}
	}
	return results
}

// resolveEntityKeysFiltered converts B-Tree keys ("entity.attr") to unique entity IDs,
// filtering by type prefix and applying remaining conditions.
func (s *AtomStore) resolveEntityKeysFiltered(keys []string, prefix string, remaining []Condition) []string {
	seen := make(map[string]struct{}, len(keys)/2)
	var results []string

	for _, k := range keys {
		parts := splitEntityAttr(k)
		if len(parts) != 2 {
			continue
		}
		entity := parts[0]
		if _, dup := seen[entity]; dup {
			continue
		}
		if !strings.HasPrefix(entity, prefix) {
			continue
		}
		seen[entity] = struct{}{}

		if len(remaining) == 0 {
			results = append(results, entity)
			continue
		}

		attrs := s.atoms[entity]
		if attrs == nil {
			continue
		}
		if matchConditionsLocal(attrs, remaining) {
			results = append(results, entity)
		}
	}
	return results
}

// scanEntitiesWithConditions groups atoms by entity and filters by conditions.
// Replaces the old pattern of scanning every atom then calling GetAll per entity.
func (s *AtomStore) scanEntitiesWithConditions(prefix string, conditions []Condition) []string {
	var results []string
	for entity, attrs := range s.atoms {
		if !strings.HasPrefix(entity, prefix) {
			continue
		}
		hasLive := false
		for _, a := range attrs {
			if a.Type != "deleted" {
				hasLive = true
				break
			}
		}
		if !hasLive {
			continue
		}
		if matchConditionsLocal(attrs, conditions) {
			results = append(results, entity)
		}
	}
	return results
}

// matchConditionsLocal checks if entity attributes satisfy all conditions.
func matchConditionsLocal(attrs map[string]*atom.Atom, conditions []Condition) bool {
	for _, cond := range conditions {
		a, ok := attrs[cond.Field]
		if !ok || a.Type == "deleted" {
			return false
		}
		if !compareAtomValueLocal(a.Value, cond.Operator, cond.Value) {
			return false
		}
	}
	return true
}

// valueKind returns a category for type-safe comparison.
func valueKind(v interface{}) string {
	if v == nil {
		return "nil"
	}
	switch v.(type) {
	case bool:
		return "bool"
	case float64, float32, int, int64:
		return "number"
	default:
		return "string"
	}
}

// compareAtomValueLocal compares atom values using the given operator.
func compareAtomValueLocal(atomVal interface{}, op string, condVal interface{}) bool {
	switch op {
	case "==":
		ak := valueKind(atomVal)
		bk := valueKind(condVal)
		if ak != bk {
			return false
		}
		return fmt.Sprintf("%v", atomVal) == fmt.Sprintf("%v", condVal)
	case "!=":
		ak := valueKind(atomVal)
		bk := valueKind(condVal)
		if ak != bk {
			return true
		}
		return fmt.Sprintf("%v", atomVal) != fmt.Sprintf("%v", condVal)
	default:
		af := toFloatLocal(atomVal)
		bf := toFloatLocal(condVal)
		switch op {
		case ">":
			return af > bf
		case "<":
			return af < bf
		case ">=":
			return af >= bf
		case "<=":
			return af <= bf
		}
	}
	return false
}

func toFloatLocal(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		var f float64
		if _, err := fmt.Sscanf(fmt.Sprintf("%v", v), "%f", &f); err == nil {
			return f
		}
		return 0
	}
}

// operatorToRangeOp converts a string operator to a RangeOp.
func operatorToRangeOp(op string) index.RangeOp {
	switch op {
	case ">":
		return index.OpGt
	case ">=":
		return index.OpGte
	case "<":
		return index.OpLt
	case "<=":
		return index.OpLte
	}
	return index.OpGt
}

// Condition represents a query condition, local to store to avoid circular import with query package.
type Condition struct {
	Field    string
	Operator string
	Value    interface{}
}

func (s *AtomStore) QueryIndexed(attribute, value string) []*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := s.idx.SearchByValue(attribute, value)
	if keys == nil {
		return nil
	}

	return s.resolveKeys(keys)
}

func (s *AtomStore) QueryRange(attribute string, op index.RangeOp, value string) []*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := s.idx.RangeSearchByValue(attribute, op, value)
	if keys == nil {
		return nil
	}

	return s.resolveKeys(keys)
}

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

func (s *AtomStore) FullTextSearch(attribute, word string) []*atom.Atom {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := s.idx.FullTextSearch(attribute, word)
	return s.resolveKeys(keys)
}

// resolveKeys converts "entity.attribute" keys to Atom pointers.
// Pre-allocates results slice and skips dedup since B-Tree already guarantees uniqueness.
func (s *AtomStore) resolveKeys(keys []string) []*atom.Atom {
	if len(keys) == 0 {
		return nil
	}
	results := make([]*atom.Atom, 0, len(keys))
	for _, k := range keys {
		parts := splitEntityAttr(k)
		if len(parts) != 2 {
			continue
		}
		if attrs, ok := s.atoms[parts[0]]; ok {
			if a, ok := attrs[parts[1]]; ok && a.Type != "deleted" {
				results = append(results, a)
			}
		}
	}
	return results
}

func (s *AtomStore) Delete(entity, attribute string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	attrs, ok := s.atoms[entity]
	if !ok {
		return ErrNotFound
	}
	existing, ok := attrs[attribute]
	if !ok || existing.Type == "deleted" {
		return ErrNotFound
	}

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

var ErrNotFound = fmt.Errorf("not found")

// AddConstraint registers a constraint on the store.
func (s *AtomStore) AddConstraint(c Constraint) {
	s.constraints.AddConstraint(c)
}

// ListConstraints returns all constraints for a type.
func (s *AtomStore) ListConstraints(typeName string) []Constraint {
	return s.constraints.ListConstraints(typeName)
}

// Compact rewrites the data file. Holds full Lock for the entire operation
// to prevent concurrent writes to the closed file handle.
func (s *AtomStore) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.file.Name()
	atomsSnapshot := make(map[string]map[string]*atom.Atom, len(s.atoms))
	for entity, attrs := range s.atoms {
		attrsCopy := make(map[string]*atom.Atom, len(attrs))
		for attr, a := range attrs {
			attrsCopy[attr] = a
		}
		atomsSnapshot[entity] = attrsCopy
	}

	// Close file while we hold the exclusive lock
	s.file.Close()

	compactErr := disk.Compact(path, atomsSnapshot, true)

	if compactErr != nil {
		// Try to reopen even on failure
		file, reopenErr := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if reopenErr != nil {
			return fmt.Errorf("compaction failed and could not reopen: %v (reopen: %v)", compactErr, reopenErr)
		}
		s.file = file
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idx.RebuildFromAtoms(s.atoms)
}

// CreateIndex creates a B-Tree index for the given type field.
// Indexes all existing data for that field.
func (s *AtomStore) CreateIndex(typeName, fieldName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limits.MaxIndexes > 0 && s.idx.IndexCount() >= s.limits.MaxIndexes {
		return fmt.Errorf("maximum index count (%d) exceeded", s.limits.MaxIndexes)
	}
	s.idx.CreateIndex(typeName, fieldName, s.atoms)
	return nil
}

// DropIndex drops the B-Tree index for the given field.
func (s *AtomStore) DropIndex(fieldName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idx.DropIndex(fieldName)
}

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

	histSize := len(s.history)
	if s.historyWriteIdx > histSize {
		histSize = maxHistorySize
	}

	return StoreStats{
		EntityCount:    len(s.atoms),
		AtomCount:      s.atomCount(),
		IndexedAttrs:   s.idx.IndexedAttributes(),
		IndexKeyCount:  s.idx.TotalKeys(),
		HistorySize:    histSize,
		LastPersistedV: s.lastPersistedVersion,
	}
}

type QueryPlan struct {
	Attribute     string
	Operator      string
	Value         string
	UseIndex      bool
	IndexType     string
	ScanType      string
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
	if len(s.history) < maxHistorySize {
		s.history = append(s.history, a)
		return
	}
	// Ring buffer: overwrite oldest entry
	idx := s.historyWriteIdx % maxHistorySize
	s.history[idx] = a
	s.historyWriteIdx++
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

func splitEntityAttr(key string) []string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '.' {
			return []string{key[:i], key[i+1:]}
		}
	}
	return []string{key}
}
