package query

import (
	"testing"
)

func TestAggregateCount(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "age": 30.0},
		{"name": "Bob", "age": 25.0},
		{"name": "Charlie", "age": 35.0},
	}

	result, err := Aggregate(rows, "count", "")
	if err != nil {
		t.Fatal(err)
	}
	if result != 3.0 {
		t.Errorf("count = %v, want 3", result)
	}
}

func TestAggregateSum(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "age": 30.0},
		{"name": "Bob", "age": 25.0},
		{"name": "Charlie", "age": 35.0},
	}

	result, err := Aggregate(rows, "sum", "age")
	if err != nil {
		t.Fatal(err)
	}
	if result != 90.0 {
		t.Errorf("sum = %v, want 90", result)
	}
}

func TestAggregateAvg(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "age": 30.0},
		{"name": "Bob", "age": 25.0},
		{"name": "Charlie", "age": 35.0},
	}

	result, err := Aggregate(rows, "avg", "age")
	if err != nil {
		t.Fatal(err)
	}
	if result != 30.0 {
		t.Errorf("avg = %v, want 30", result)
	}
}

func TestAggregateMin(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "age": 30.0},
		{"name": "Bob", "age": 25.0},
		{"name": "Charlie", "age": 35.0},
	}

	result, err := Aggregate(rows, "min", "age")
	if err != nil {
		t.Fatal(err)
	}
	if result != 25.0 {
		t.Errorf("min = %v, want 25", result)
	}
}

func TestAggregateMax(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "age": 30.0},
		{"name": "Bob", "age": 25.0},
		{"name": "Charlie", "age": 35.0},
	}

	result, err := Aggregate(rows, "max", "age")
	if err != nil {
		t.Fatal(err)
	}
	if result != 35.0 {
		t.Errorf("max = %v, want 35", result)
	}
}

func TestAggregateEmpty(t *testing.T) {
	rows := []map[string]interface{}{}

	result, err := Aggregate(rows, "count", "")
	if err != nil {
		t.Fatal(err)
	}
	if result != 0 {
		t.Errorf("count of empty = %v, want 0", result)
	}
}

func TestAggregateSkipsNil(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "age": 30.0},
		{"name": "Bob", "age": nil},
		{"name": "Charlie", "age": 35.0},
	}

	result, err := Aggregate(rows, "avg", "age")
	if err != nil {
		t.Fatal(err)
	}
	// Should average only non-nil values: (30 + 35) / 2 = 32.5
	if result != 32.5 {
		t.Errorf("avg = %v, want 32.5", result)
	}
}

func TestGroupByResults(t *testing.T) {
	rows := []map[string]interface{}{
		{"city": "Lagos", "name": "Alice"},
		{"city": "Lagos", "name": "Bob"},
		{"city": "NYC", "name": "Charlie"},
	}

	groups := GroupByResults(rows, "city")
	if len(groups) != 2 {
		t.Errorf("groups = %d, want 2", len(groups))
	}
	if len(groups["Lagos"]) != 2 {
		t.Errorf("Lagos group = %d, want 2", len(groups["Lagos"]))
	}
	if len(groups["NYC"]) != 1 {
		t.Errorf("NYC group = %d, want 1", len(groups["NYC"]))
	}
}
