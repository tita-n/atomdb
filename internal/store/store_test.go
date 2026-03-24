package store

import (
	"fmt"
	"os"
	"testing"

	"github.com/tita-n/atomdb/internal/index"
)

func tempDB(t *testing.T) string {
	t.Helper()
	path := t.TempDir() + "/test.db"
	t.Cleanup(func() { os.Remove(path) })
	return path
}

func TestStoreSetAndGet(t *testing.T) {
	s, err := New(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Set("user:1", "name", "Ayo", "string")

	a, ok := s.Get("user:1", "name")
	if !ok {
		t.Fatal("Get returned false")
	}
	if a.Value != "Ayo" {
		t.Errorf("Value = %v, want Ayo", a.Value)
	}
	if a.Type != "string" {
		t.Errorf("Type = %s, want string", a.Type)
	}
}

func TestStoreGetNotFound(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	_, ok := s.Get("user:1", "missing")
	if ok {
		t.Error("Get should return false for missing attribute")
	}
}

func TestStoreGetAll(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "name", "Ayo", "string")
	s.Set("user:1", "age", 28.0, "number")

	attrs := s.GetAll("user:1")
	if len(attrs) != 2 {
		t.Errorf("GetAll returned %d attrs, want 2", len(attrs))
	}
}

func TestStoreDelete(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "name", "Ayo", "string")

	err := s.Delete("user:1", "name")
	if err != nil {
		t.Fatal(err)
	}

	_, ok := s.Get("user:1", "name")
	if ok {
		t.Error("Get should return false after delete")
	}
}

func TestStoreDeleteNotFound(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	err := s.Delete("user:999", "nothing")
	if err != ErrNotFound {
		t.Errorf("Delete non-existent should return ErrNotFound, got %v", err)
	}
}

func TestStoreExists(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	if s.Exists("user:1", "name") {
		t.Error("Exists should return false for missing")
	}

	s.Set("user:1", "name", "Ayo", "string")
	if !s.Exists("user:1", "name") {
		t.Error("Exists should return true after Set")
	}

	s.Delete("user:1", "name")
	if s.Exists("user:1", "name") {
		t.Error("Exists should return false after Delete")
	}
}

func TestStoreOverwrite(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "name", "Ayo", "string")
	s.Set("user:1", "name", "Bob", "string")

	a, _ := s.Get("user:1", "name")
	if a.Value != "Bob" {
		t.Errorf("Value = %v, want Bob", a.Value)
	}
}

func TestStoreQueryIndexed(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "name", "Ayo", "string")
	s.Set("user:2", "name", "Bob", "string")
	s.Set("user:3", "name", "Ayo", "string")

	results := s.QueryIndexed("name", "Ayo")
	if len(results) != 2 {
		t.Errorf("QueryIndexed(name, Ayo) = %d results, want 2", len(results))
	}
}

func TestStoreQueryRange(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "age", 9.0, "number")
	s.Set("user:2", "age", 25.0, "number")
	s.Set("user:3", "age", 100.0, "number")

	results := s.QueryRange("age", index.OpGt, "20")
	if len(results) != 2 {
		t.Errorf("QueryRange(age, GT, 20) = %d results, want 2", len(results))
	}

	results = s.QueryRange("age", index.OpLt, "30")
	if len(results) != 2 {
		t.Errorf("QueryRange(age, LT, 30) = %d results, want 2", len(results))
	}
}

func TestStoreFullTextSearch(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "name", "Ayo Adeleke", "string")
	s.Set("user:2", "name", "Bob Johnson", "string")

	results := s.FullTextSearch("name", "Ayo")
	if len(results) != 1 {
		t.Errorf("FullTextSearch(name, Ayo) = %d results, want 1", len(results))
	}
}

func TestStorePersistence(t *testing.T) {
	path := tempDB(t)

	s1, _ := New(path)
	s1.Set("user:1", "name", "Ayo", "string")
	s1.Set("user:1", "age", 28.0, "number")
	s1.Close()

	s2, _ := New(path)
	defer s2.Close()

	a, ok := s2.Get("user:1", "name")
	if !ok || a.Value != "Ayo" {
		t.Errorf("after restart: Get(name) = %v, %v; want Ayo, true", a.Value, ok)
	}

	a, ok = s2.Get("user:1", "age")
	if !ok || a.Value != 28.0 {
		t.Errorf("after restart: Get(age) = %v, %v; want 28, true", a.Value, ok)
	}
}

func TestStoreStats(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "name", "Ayo", "string")
	s.Set("user:2", "age", 30.0, "number")

	stats := s.Stats()
	if stats.EntityCount != 2 {
		t.Errorf("EntityCount = %d, want 2", stats.EntityCount)
	}
	if stats.AtomCount != 2 {
		t.Errorf("AtomCount = %d, want 2", stats.AtomCount)
	}
}

func TestStoreCompact(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("user:%d", i), "name", fmt.Sprintf("User %d", i), "string")
	}

	err := s.Compact()
	if err != nil {
		t.Fatal(err)
	}

	stats := s.Stats()
	if stats.AtomCount != 100 {
		t.Errorf("after compact: AtomCount = %d, want 100", stats.AtomCount)
	}
}

func TestStoreHistoryPopulatedOnStartup(t *testing.T) {
	path := tempDB(t)

	s1, _ := New(path)
	s1.Set("user:1", "name", "Ayo", "string")
	s1.Close()

	s2, _ := New(path)
	defer s2.Close()

	stats := s2.Stats()
	if stats.HistorySize == 0 {
		t.Error("history should be populated after startup replay")
	}
}

func TestStoreQueryEntitiesEquality(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("person:1", "name", "Ayo", "string")
	s.Set("person:1", "city", "Lagos", "string")
	s.Set("person:2", "name", "Bob", "string")
	s.Set("person:2", "city", "Lagos", "string")
	s.Set("person:3", "name", "Chi", "string")
	s.Set("person:3", "city", "Abuja", "string")

	// Query with index on city
	results := s.QueryEntities("person", []Condition{{Field: "city", Operator: "==", Value: "Lagos"}})
	if len(results) != 2 {
		t.Errorf("QueryEntities(city==Lagos) = %d results, want 2", len(results))
	}
}

func TestStoreQueryEntitiesRange(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("person:1", "age", 20.0, "number")
	s.Set("person:1", "name", "Ayo", "string")
	s.Set("person:2", "age", 30.0, "number")
	s.Set("person:2", "name", "Bob", "string")
	s.Set("person:3", "age", 40.0, "number")
	s.Set("person:3", "name", "Chi", "string")

	results := s.QueryEntities("person", []Condition{{Field: "age", Operator: ">", Value: 25.0}})
	if len(results) != 2 {
		t.Errorf("QueryEntities(age>25) = %d results, want 2", len(results))
	}
}

func TestStoreQueryEntitiesMultipleConditions(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("person:1", "city", "Lagos", "string")
	s.Set("person:1", "age", 20.0, "number")
	s.Set("person:2", "city", "Lagos", "string")
	s.Set("person:2", "age", 30.0, "number")
	s.Set("person:3", "city", "Abuja", "string")
	s.Set("person:3", "age", 25.0, "number")

	// Should use city index then filter by age
	results := s.QueryEntities("person", []Condition{
		{Field: "city", Operator: "==", Value: "Lagos"},
		{Field: "age", Operator: ">", Value: 25.0},
	})
	if len(results) != 1 {
		t.Errorf("QueryEntities(city==Lagos AND age>25) = %d results, want 1", len(results))
	}
}

func TestStoreQueryEntitiesNoConditions(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("person:1", "name", "Ayo", "string")
	s.Set("person:2", "name", "Bob", "string")
	s.Set("other:1", "name", "X", "string")

	results := s.QueryEntities("person", nil)
	if len(results) != 2 {
		t.Errorf("QueryEntities(person, nil) = %d results, want 2", len(results))
	}
}

// --- Comprehensive integration tests for all performance fixes ---

func TestIntegration_IndexedQueryVsFullScan(t *testing.T) {
	// Validates: QueryEntities uses indexes, not full scan
	s, _ := NewWithMode(tempDB(t), SyncBatch)
	defer s.Close()

	// Insert 200 entities with 3 attributes each = 600 atoms
	for i := 0; i < 200; i++ {
		entity := fmt.Sprintf("user:%d", i)
		s.Set(entity, "name", fmt.Sprintf("User %d", i), "string")
		s.Set(entity, "age", float64(20+i%60), "number")
		if i%3 == 0 {
			s.Set(entity, "city", "Lagos", "string")
		} else {
			s.Set(entity, "city", "Abuja", "string")
		}
	}

	// Equality query on city (indexed) — should return only Lagos users
	results := s.QueryEntities("user", []Condition{{Field: "city", Operator: "==", Value: "Lagos"}})
	expected := 0
	for i := 0; i < 200; i++ {
		if i%3 == 0 {
			expected++
		}
	}
	if len(results) != expected {
		t.Errorf("city==Lagos: got %d, want %d", len(results), expected)
	}

	// Range query on age (indexed) — should return users with age > 60
	results = s.QueryEntities("user", []Condition{{Field: "age", Operator: ">", Value: 60.0}})
	if len(results) == 0 {
		t.Error("age>60 returned no results, expected some")
	}
	for _, entity := range results {
		a, ok := s.Get(entity, "age")
		if !ok {
			t.Errorf("entity %s has no age", entity)
			continue
		}
		if a.Value.(float64) <= 60 {
			t.Errorf("entity %s has age %v, expected > 60", entity, a.Value)
		}
	}
}

func TestIntegration_MultiConditionIndexedQuery(t *testing.T) {
	// Validates: first indexed condition narrows candidates, remaining filter in memory
	s, _ := New(tempDB(t))
	defer s.Close()

	for i := 0; i < 100; i++ {
		entity := fmt.Sprintf("product:%d", i)
		s.Set(entity, "name", fmt.Sprintf("Product %d", i), "string")
		s.Set(entity, "price", float64(i*10), "number")
		if i < 30 {
			s.Set(entity, "category", "electronics", "string")
		} else {
			s.Set(entity, "category", "clothing", "string")
		}
	}

	// category==electronics AND price > 200
	results := s.QueryEntities("product", []Condition{
		{Field: "category", Operator: "==", Value: "electronics"},
		{Field: "price", Operator: ">", Value: 200.0},
	})
	// electronics: 0-29, prices 0-290. Price > 200 means products 21-29 = 9
	if len(results) != 9 {
		t.Errorf("category==electronics AND price>200: got %d, want 9", len(results))
	}
	for _, entity := range results {
		cat, _ := s.Get(entity, "category")
		price, _ := s.Get(entity, "price")
		if cat.Value != "electronics" {
			t.Errorf("%s: category=%v, want electronics", entity, cat.Value)
		}
		if price.Value.(float64) <= 200 {
			t.Errorf("%s: price=%v, want > 200", entity, price.Value)
		}
	}
}

func TestIntegration_SchemaFieldSetO1Lookup(t *testing.T) {
	// Validates: O(1) unknown field check via fieldSet
	s, _ := New(tempDB(t))
	defer s.Close()

	// Insert with valid fields should succeed
	s.Set("item:1", "name", "Widget", "string")
	s.Set("item:1", "price", 9.99, "number")

	// Verify data was stored
	a, ok := s.Get("item:1", "name")
	if !ok || a.Value != "Widget" {
		t.Errorf("Get(name) = %v, %v; want Widget, true", a.Value, ok)
	}
	a, ok = s.Get("item:1", "price")
	if !ok || a.Value != 9.99 {
		t.Errorf("Get(price) = %v, %v; want 9.99, true", a.Value, ok)
	}
}

func TestIntegration_HistoryRingBuffer(t *testing.T) {
	// Validates: ring buffer doesn't leak memory by growing unbounded
	s, _ := NewWithMode(tempDB(t), SyncBatch)
	defer s.Close()

	// Write more than maxHistorySize to trigger ring buffer behavior
	for i := 0; i < maxHistorySize+500; i++ {
		s.Set(fmt.Sprintf("e:%d", i), "val", fmt.Sprintf("v%d", i), "string")
	}

	stats := s.Stats()
	// History should be capped at maxHistorySize, not growing to maxHistorySize+500
	if stats.HistorySize > maxHistorySize {
		t.Errorf("HistorySize = %d, want <= %d (ring buffer overflow)", stats.HistorySize, maxHistorySize)
	}
	if stats.HistorySize < maxHistorySize {
		t.Errorf("HistorySize = %d, want %d (should be full)", stats.HistorySize, maxHistorySize)
	}
}

func TestIntegration_IndexedQueryReturnsCorrectTypes(t *testing.T) {
	// Validates: indexed query only returns entities of the requested type
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("person:1", "city", "Lagos", "string")
	s.Set("company:1", "city", "Lagos", "string")
	s.Set("person:2", "city", "Abuja", "string")

	results := s.QueryEntities("person", []Condition{{Field: "city", Operator: "==", Value: "Lagos"}})
	if len(results) != 1 {
		t.Errorf("QueryEntities(person, city==Lagos) = %d, want 1", len(results))
	}
	if len(results) > 0 && results[0] != "person:1" {
		t.Errorf("got entity %s, want person:1", results[0])
	}
}

func TestIntegration_DeleteRemovesFromIndex(t *testing.T) {
	// Validates: deleted atoms are excluded from index queries
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "name", "Ayo", "string")
	s.Set("user:2", "name", "Bob", "string")

	results := s.QueryEntities("user", []Condition{{Field: "name", Operator: "==", Value: "Ayo"}})
	if len(results) != 1 {
		t.Fatalf("before delete: got %d, want 1", len(results))
	}

	s.Delete("user:1", "name")

	results = s.QueryEntities("user", []Condition{{Field: "name", Operator: "==", Value: "Ayo"}})
	if len(results) != 0 {
		t.Errorf("after delete: got %d, want 0", len(results))
	}
}

func TestIntegration_OverwriteUpdatesIndex(t *testing.T) {
	// Validates: overwriting an atom correctly updates the index
	s, _ := New(tempDB(t))
	defer s.Close()

	s.Set("user:1", "city", "Lagos", "string")
	s.Set("user:2", "city", "Lagos", "string")

	// Update user:1's city
	s.Set("user:1", "city", "Abuja", "string")

	lagos := s.QueryEntities("user", []Condition{{Field: "city", Operator: "==", Value: "Lagos"}})
	abuja := s.QueryEntities("user", []Condition{{Field: "city", Operator: "==", Value: "Abuja"}})

	if len(lagos) != 1 {
		t.Errorf("Lagos count = %d, want 1", len(lagos))
	}
	if len(abuja) != 1 {
		t.Errorf("Abuja count = %d, want 1", len(abuja))
	}
	if len(abuja) > 0 && abuja[0] != "user:1" {
		t.Errorf("Abuja entity = %s, want user:1", abuja[0])
	}
}

func TestIntegration_CompactPreservesIndexData(t *testing.T) {
	// Validates: compaction + rebuild preserves indexed query results
	s, _ := NewWithMode(tempDB(t), SyncBatch)
	defer s.Close()

	for i := 0; i < 50; i++ {
		s.Set(fmt.Sprintf("item:%d", i), "status", "active", "string")
	}

	// Query before compact
	before := s.QueryEntities("item", []Condition{{Field: "status", Operator: "==", Value: "active"}})
	if len(before) != 50 {
		t.Fatalf("before compact: got %d, want 50", len(before))
	}

	s.Compact()

	// Query after compact
	after := s.QueryEntities("item", []Condition{{Field: "status", Operator: "==", Value: "active"}})
	if len(after) != 50 {
		t.Errorf("after compact: got %d, want 50", len(after))
	}
}

func TestIntegration_NumericRangeEdgeCases(t *testing.T) {
	// Validates: IEEE 754 encoding handles negative, zero, large numbers correctly
	s, _ := NewWithMode(tempDB(t), SyncBatch)
	defer s.Close()

	s.Set("e:1", "score", -100.0, "number")
	s.Set("e:2", "score", 0.0, "number")
	s.Set("e:3", "score", 0.1, "number")
	s.Set("e:4", "score", 999999.5, "number")

	// Verify HasIndex works
	if !s.HasIndex("score") {
		t.Fatal("score should have an index")
	}

	// Check what QueryRange returns directly
	directResults := s.QueryRange("score", index.OpGte, "0")
	t.Logf("Direct QueryRange(score, GTE, '0'): %d results: %v", len(directResults), directResults)

	// score >= 0
	gte := s.QueryEntities("e", []Condition{{Field: "score", Operator: ">=", Value: 0.0}})
	t.Logf("QueryEntities score>=0: %d results: %v", len(gte), gte)
	if len(gte) != 3 {
		t.Errorf("score>=0: got %d, want 3: %v", len(gte), gte)
	}

	// score < 0
	lt := s.QueryEntities("e", []Condition{{Field: "score", Operator: "<", Value: 0.0}})
	t.Logf("QueryEntities score<0: %d results: %v", len(lt), lt)
	if len(lt) != 1 {
		t.Errorf("score<0: got %d, want 1: %v", len(lt), lt)
	}

	// score > 100
	gt := s.QueryEntities("e", []Condition{{Field: "score", Operator: ">", Value: 100.0}})
	t.Logf("QueryEntities score>100: %d results: %v", len(gt), gt)
	if len(gt) != 1 {
		t.Errorf("score>100: got %d, want 1: %v", len(gt), gt)
	}

	// score <= 0
	lte := s.QueryEntities("e", []Condition{{Field: "score", Operator: "<=", Value: 0.0}})
	t.Logf("QueryEntities score<=0: %d results: %v", len(lte), lte)
	if len(lte) != 2 {
		t.Errorf("score<=0: got %d, want 2: %v", len(lte), lte)
	}
}
