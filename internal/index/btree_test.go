package index

import (
	"fmt"
	"math"
	"testing"
)

func TestBTreeInsertAndSearch(t *testing.T) {
	tree := New()
	tree.Insert("apple", []string{"user:1.name"})
	tree.Insert("banana", []string{"user:2.name"})
	tree.Insert("cherry", []string{"user:3.name"})

	if got := tree.Search("apple"); len(got) != 1 || got[0] != "user:1.name" {
		t.Errorf("Search(apple) = %v, want [user:1.name]", got)
	}
	if got := tree.Search("banana"); len(got) != 1 || got[0] != "user:2.name" {
		t.Errorf("Search(banana) = %v, want [user:2.name]", got)
	}
	if got := tree.Search("missing"); got != nil {
		t.Errorf("Search(missing) = %v, want nil", got)
	}
}

func TestBTreeInsertDuplicate(t *testing.T) {
	tree := New()
	tree.Insert("key1", []string{"user:1.name"})
	tree.Insert("key1", []string{"user:2.name"})

	got := tree.Search("key1")
	if len(got) != 2 {
		t.Errorf("Search(key1) = %v, want 2 entities", got)
	}
	if tree.Count() != 1 {
		t.Errorf("Count() = %d, want 1", tree.Count())
	}
}

func TestBTreeInsertDedup(t *testing.T) {
	tree := New()
	tree.Insert("key1", []string{"user:1.name"})
	tree.Insert("key1", []string{"user:1.name"})

	got := tree.Search("key1")
	if len(got) != 1 {
		t.Errorf("dedup failed: Search(key1) = %v, want 1 entity", got)
	}
}

func TestBTreeRemove(t *testing.T) {
	tree := New()
	tree.Insert("key1", []string{"user:1.name"})
	tree.Insert("key1", []string{"user:2.name"})

	tree.Remove("key1", "user:1.name")
	got := tree.Search("key1")
	if len(got) != 1 || got[0] != "user:2.name" {
		t.Errorf("after remove: Search(key1) = %v, want [user:2.name]", got)
	}

	tree.Remove("key1", "user:2.name")
	got = tree.Search("key1")
	if got != nil {
		t.Errorf("after removing last entity: Search(key1) = %v, want nil", got)
	}
	if tree.Count() != 0 {
		t.Errorf("Count() = %d, want 0", tree.Count())
	}
}

func TestBTreeRangeGT(t *testing.T) {
	tree := New()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%03d", i)
		tree.Insert(key, []string{fmt.Sprintf("e%d", i)})
	}

	results := tree.RangeQuery(OpGt, "050")
	if len(results) != 49 {
		t.Errorf("RangeQuery(GT, 050) returned %d results, want 49", len(results))
	}

	results = tree.RangeQuery(OpGte, "050")
	if len(results) != 50 {
		t.Errorf("RangeQuery(GTE, 050) returned %d results, want 50", len(results))
	}
}

func TestBTreeRangeLT(t *testing.T) {
	tree := New()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%03d", i)
		tree.Insert(key, []string{fmt.Sprintf("e%d", i)})
	}

	results := tree.RangeQuery(OpLt, "050")
	if len(results) != 50 {
		t.Errorf("RangeQuery(LT, 050) returned %d results, want 50", len(results))
	}

	results = tree.RangeQuery(OpLte, "050")
	if len(results) != 51 {
		t.Errorf("RangeQuery(LTE, 050) returned %d results, want 51", len(results))
	}
}

func TestBTreeLargeInsert(t *testing.T) {
	tree := New()
	n := 1000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		tree.Insert(key, []string{fmt.Sprintf("entity.%d", i)})
	}

	if tree.Count() != n {
		t.Errorf("Count() = %d, want %d", tree.Count(), n)
	}

	got := tree.Search("key-000500")
	if len(got) != 1 {
		t.Errorf("Search(key-000500) = %v, want 1 result", got)
	}
}

func TestBTreeKeysSorted(t *testing.T) {
	tree := New()
	keys := []string{"cherry", "apple", "banana", "date"}
	for _, k := range keys {
		tree.Insert(k, []string{k})
	}

	sorted := tree.Keys()
	for i := 1; i < len(sorted); i++ {
		if sorted[i] <= sorted[i-1] {
			t.Errorf("keys not sorted: %s <= %s", sorted[i], sorted[i-1])
		}
	}
}

// TestEncodeNumericKey verifies that IEEE 754 encoding preserves sort order.
func TestEncodeNumericKey(t *testing.T) {
	tests := []struct {
		a, b float64
	}{
		{9, 25},
		{9, 10},
		{99, 100},
		{-100, -1},
		{-1, 0},
		{0, 1},
		{-1000, 1000},
		{1.5, 2.5},
		{0.1, 0.9},
		{math.SmallestNonzeroFloat64, 1},
	}

	for _, tt := range tests {
		ka := encodeNumericKey(tt.a)
		kb := encodeNumericKey(tt.b)
		if ka >= kb {
			t.Errorf("encodeNumericKey(%v) >= encodeNumericKey(%v): %s >= %s",
				tt.a, tt.b, ka, kb)
		}
	}
}

func TestNormalizeValueNumbers(t *testing.T) {
	k9 := normalizeValue(9.0)
	k25 := normalizeValue(25.0)
	k100 := normalizeValue(100.0)

	if k9 >= k25 {
		t.Errorf("9 should sort before 25: %s >= %s", k9, k25)
	}
	if k25 >= k100 {
		t.Errorf("25 should sort before 100: %s >= %s", k25, k100)
	}
}

func TestNormalizeValueStringNumber(t *testing.T) {
	// String "20" should be parsed as number and encoded
	k20 := normalizeValue("20")
	k25 := normalizeValue(25.0)

	if k20 >= k25 {
		t.Errorf("string '20' should sort before number 25: %s >= %s", k20, k25)
	}
}

func TestBTreeNumericRangeCorrectness(t *testing.T) {
	tree := New()
	tree.Insert(encodeNumericKey(9), []string{"user:1.age"})
	tree.Insert(encodeNumericKey(25), []string{"user:2.age"})
	tree.Insert(encodeNumericKey(100), []string{"user:3.age"})

	// Query > 20 should return 25 and 100
	searchKey := encodeNumericKey(20.0)
	results := tree.RangeQuery(OpGt, searchKey)
	if len(results) != 2 {
		t.Errorf("numeric range GT 20: got %d results, want 2: %v", len(results), results)
	}

	// Query < 30 should return 9 and 25
	results = tree.RangeQuery(OpLt, encodeNumericKey(30.0))
	if len(results) != 2 {
		t.Errorf("numeric range LT 30: got %d results, want 2: %v", len(results), results)
	}

	// Query > 8 should return all 3
	results = tree.RangeQuery(OpGt, encodeNumericKey(8.0))
	if len(results) != 3 {
		t.Errorf("numeric range GT 8: got %d results, want 3: %v", len(results), results)
	}
}
