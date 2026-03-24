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
