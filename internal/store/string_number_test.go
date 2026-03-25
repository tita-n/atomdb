package store

import (
	"testing"

	"github.com/tita-n/atomdb/internal/index"
)

func TestStringVsNumberTypeCoercion(t *testing.T) {
	s, _ := New(tempDB(t))
	defer s.Close()

	// Store a STRING that looks like a number
	s.Set("user:1", "id", "123445", "string")

	// Store an actual NUMBER
	s.Set("user:2", "id", 123445, "number")

	// CRITICAL FIX: Index keys must be DIFFERENT for string vs number
	// Before fix: both "123445" and 123445 produced the same key, causing collisions
	key1 := index.NormalizeValue("123445")
	key2 := index.NormalizeValue(123445)
	t.Logf("Index key for '123445' (string): %q", key1)
	t.Logf("Index key for 123445 (number): %q", key2)
	if key1 == key2 {
		t.Fatalf("FATAL BUG: String and number index keys must NOT match! Both are %q", key1)
	}

	// Verify store preserves original types
	a1, _ := s.Get("user:1", "id")
	a2, _ := s.Get("user:2", "id")
	if a1.Type != "string" {
		t.Errorf("user:1.id type = %q, want 'string'", a1.Type)
	}
	if a1.Value != "123445" {
		t.Errorf("user:1.id value = %v, want '123445'", a1.Value)
	}
	if a2.Type != "number" {
		t.Errorf("user:2.id type = %q, want 'number'", a2.Type)
	}
	if a2.Value != 123445 {
		t.Errorf("user:2.id value = %v, want 123445", a2.Value)
	}

	// Query with NUMBER value should return ONLY the number entity
	entities := s.QueryEntities("user", []Condition{{Field: "id", Operator: "==", Value: 123445}})
	t.Logf("QueryEntities id==123445 (number): %d results: %v", len(entities), entities)
	if len(entities) != 1 {
		t.Errorf("Number query should return exactly 1 result, got %d", len(entities))
	}
	if len(entities) > 0 && entities[0] != "user:2" {
		t.Errorf("Number query should return user:2, got %s", entities[0])
	}

	// Query with STRING value (CLI compatibility): tries numeric key first,
	// finds number match. String key also has a match but numeric is preferred.
	entitiesStr := s.QueryEntities("user", []Condition{{Field: "id", Operator: "==", Value: "123445"}})
	t.Logf("QueryEntities id=='123445' (string): %d results: %v", len(entitiesStr), entitiesStr)
	// The string query finds the numeric match (CLI behavior for numeric strings)
	// This is intentional: when you type "123445" from CLI, you expect to find 123445
	if len(entitiesStr) != 1 {
		t.Errorf("String query should return 1 result, got %d", len(entitiesStr))
	}

	// Verify non-numeric string queries work correctly
	s.Set("user:3", "name", "hello", "string")
	s.Set("user:4", "name", "world", "string")
	nameResults := s.QueryEntities("user", []Condition{{Field: "name", Operator: "==", Value: "hello"}})
	t.Logf("QueryEntities name=='hello': %d results", len(nameResults))
	if len(nameResults) != 1 {
		t.Errorf("String query for 'hello' should return 1 result, got %d", len(nameResults))
	}
	if len(nameResults) > 0 && nameResults[0] != "user:3" {
		t.Errorf("Expected user:3, got %s", nameResults[0])
	}
}
