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

func TestAggregateSumNonNumeric(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice"},
		{"name": "Bob"},
	}
	_, err := Aggregate(rows, "sum", "name")
	if err == nil {
		t.Error("sum on non-numeric field should return error")
	}
}

func TestAggregateMinNonNumeric(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice"},
	}
	_, err := Aggregate(rows, "min", "name")
	if err == nil {
		t.Error("min on non-numeric field should return error")
	}
}

func TestRemoveAggFuncNestedParens(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"sum(price * (1 + tax)) where x == 1", "where x == 1"},
		{"count(person) where age > 25", "where age > 25"},
		{"avg(x * (a + (b - c))) order by name", "order by name"},
	}
	for _, tt := range tests {
		got := removeAggFunc(tt.input)
		if got != tt.expected {
			t.Errorf("removeAggFunc(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestFindClauseIndexRespectsQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		// "order by" inside quotes should not match
		{`name == "order by me" and age > 25`, -1},
		// unquoted "order by" should match
		{`name == test order by age`, 13},
		// "limit" inside quotes should not match
		{`name == "limit 10"`, -1},
	}
	for _, tt := range tests {
		got := findClauseIndex(tt.input)
		if got != tt.expected {
			t.Errorf("findClauseIndex(%q) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestParseWhereTrailingIncomplete(t *testing.T) {
	_, err := parseWhere("name")
	if err == nil {
		t.Error("parseWhere with trailing incomplete condition should return error")
	}

	_, err = parseWhere("name ==")
	if err == nil {
		t.Error("parseWhere with missing value should return error")
	}
}

func TestParseSelectGroupBy(t *testing.T) {
	parsed, err := Parse("SELECT city FROM person GROUP BY city")
	if err != nil {
		t.Fatalf("Parse SELECT GROUP BY error: %v", err)
	}
	if parsed.Query.GroupBy != "city" {
		t.Errorf("GroupBy = %q, want 'city'", parsed.Query.GroupBy)
	}
	if parsed.Query.TypeName != "person" {
		t.Errorf("TypeName = %q, want 'person'", parsed.Query.TypeName)
	}
}

func TestParseSelectGroupByWithAggregation(t *testing.T) {
	parsed, err := Parse("SELECT city, count(*) FROM person GROUP BY city")
	if err != nil {
		t.Fatalf("Parse SELECT GROUP BY with agg error: %v", err)
	}
	if parsed.Query.GroupBy != "city" {
		t.Errorf("GroupBy = %q, want 'city'", parsed.Query.GroupBy)
	}
	if parsed.Query.Aggregate != "count" {
		t.Errorf("Aggregate = %q, want 'count'", parsed.Query.Aggregate)
	}
	if len(parsed.Query.Fields) != 1 || parsed.Query.Fields[0] != "city" {
		t.Errorf("Fields = %v, want ['city']", parsed.Query.Fields)
	}
}
