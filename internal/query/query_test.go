package query

import (
	"reflect"
	"testing"

	"github.com/tita-n/atomdb/internal/atom"
)

func TestParseShorthand(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
		wantCond int
		wantOrd  string
		wantDesc bool
		wantLim  int
	}{
		{"simple type", "person", "person", 0, "", false, 0},
		{"with where", "person where age > 25", "person", 1, "", false, 0},
		{"with where equals", "person where name == Lagos", "person", 1, "", false, 0},
		{"with order by asc", "person order by name where age > 25", "person", 1, "name", false, 0},
		{"with order by desc", "person order by name desc where age > 25", "person", 1, "name", true, 0},
		{"with order by Desc (mixed)", "person order by name Desc where age > 25", "person", 1, "name", true, 0},
		{"with limit", "person where age > 25 limit 10", "person", 1, "", false, 10},
		{"with order by and limit", "person order by name desc limit 10 where age > 25", "person", 1, "name", true, 10},
		{"with field selection", "person.name where city == Lagos", "person", 1, "", false, 0},
		{"with multiple field selection", "person.name email city where age > 25", "person", 1, "", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error = %v", tt.input, err)
			}
			if parsed.Command != "SELECT" {
				t.Errorf("Command = %q, want SELECT", parsed.Command)
			}
			if parsed.Query.TypeName != tt.wantType {
				t.Errorf("TypeName = %q, want %q", parsed.Query.TypeName, tt.wantType)
			}
			if len(parsed.Query.Conditions) != tt.wantCond {
				t.Errorf("Conditions count = %d, want %d", len(parsed.Query.Conditions), tt.wantCond)
			}
			if tt.wantOrd != "" {
				if parsed.Query.OrderBy == nil {
					t.Errorf("OrderBy = nil, want field %q", tt.wantOrd)
				} else if parsed.Query.OrderBy.Field != tt.wantOrd {
					t.Errorf("OrderBy.Field = %q, want %q", parsed.Query.OrderBy.Field, tt.wantOrd)
				} else if parsed.Query.OrderBy.Desc != tt.wantDesc {
					t.Errorf("OrderBy.Desc = %v, want %v", parsed.Query.OrderBy.Desc, tt.wantDesc)
				}
			}
			if parsed.Query.Limit != tt.wantLim {
				t.Errorf("Limit = %d, want %d", parsed.Query.Limit, tt.wantLim)
			}
		})
	}
}

func TestParseSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
		wantCond int
		wantLim  int
	}{
		{"from clause", "name from person", "person", 0, 0},
		{"from with where", "name from person where age > 25", "person", 1, 0},
		{"from with limit", "* from person limit 5", "person", 0, 5},
		{"from with order by desc", "* from person order by age desc", "person", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse("SELECT " + tt.input)
			if err != nil {
				t.Fatalf("Parse(SELECT %q) error = %v", tt.input, err)
			}
			if parsed.Query.TypeName != tt.wantType {
				t.Errorf("TypeName = %q, want %q", parsed.Query.TypeName, tt.wantType)
			}
			if len(parsed.Query.Conditions) != tt.wantCond {
				t.Errorf("Conditions count = %d, want %d", len(parsed.Query.Conditions), tt.wantCond)
			}
			if parsed.Query.Limit != tt.wantLim {
				t.Errorf("Limit = %d, want %d", parsed.Query.Limit, tt.wantLim)
			}
		})
	}
}

func TestParseUpdate(t *testing.T) {
	parsed, err := Parse("UPDATE person where name == John set age = 30")
	if err != nil {
		t.Fatalf("Parse UPDATE error = %v", err)
	}
	if parsed.Command != "UPDATE" {
		t.Errorf("Command = %q, want UPDATE", parsed.Command)
	}
	if parsed.Update.TypeName != "person" {
		t.Errorf("TypeName = %q, want person", parsed.Update.TypeName)
	}
	if len(parsed.Update.Conditions) != 1 {
		t.Errorf("Conditions count = %d, want 1", len(parsed.Update.Conditions))
	}
	if len(parsed.Update.SetFields) != 1 {
		t.Errorf("SetFields count = %d, want 1", len(parsed.Update.SetFields))
	}
}

func TestParseDelete(t *testing.T) {
	parsed, err := Parse("DELETE person where name == John")
	if err != nil {
		t.Fatalf("Parse DELETE error = %v", err)
	}
	if parsed.Command != "DELETE" {
		t.Errorf("Command = %q, want DELETE", parsed.Command)
	}
	if parsed.Delete.TypeName != "person" {
		t.Errorf("TypeName = %q, want person", parsed.Delete.TypeName)
	}
	if len(parsed.Delete.Conditions) != 1 {
		t.Errorf("Conditions count = %d, want 1", len(parsed.Delete.Conditions))
	}
}

func TestMatchConditions_Empty(t *testing.T) {
	attrs := map[string]*atom.Atom{}
	if !MatchConditions(attrs, nil) {
		t.Error("empty conditions should return true")
	}
}

func TestMatchConditions_And(t *testing.T) {
	attrs := map[string]*atom.Atom{
		"age":  {Value: float64(30), Type: "number"},
		"name": {Value: "John", Type: "string"},
	}
	conds := []Condition{
		{Field: "name", Operator: "==", Value: "John", Logic: "AND"},
		{Field: "age", Operator: ">", Value: float64(25), Logic: ""},
	}
	if !MatchConditions(attrs, conds) {
		t.Error("both conditions match, should return true")
	}

	conds[1].Value = float64(35)
	if MatchConditions(attrs, conds) {
		t.Error("age > 35 fails for age=30, should return false")
	}
}

func TestMatchConditions_Or(t *testing.T) {
	attrs := map[string]*atom.Atom{
		"age":  {Value: float64(30), Type: "number"},
		"name": {Value: "John", Type: "string"},
	}

	conds := []Condition{
		{Field: "name", Operator: "==", Value: "Jane", Logic: "OR"},
		{Field: "age", Operator: ">", Value: float64(25), Logic: ""},
	}
	if !MatchConditions(attrs, conds) {
		t.Error("name==Jane OR age>25 should pass (age>25 matches)")
	}

	conds2 := []Condition{
		{Field: "name", Operator: "==", Value: "Jane", Logic: "OR"},
		{Field: "age", Operator: ">", Value: float64(35), Logic: ""},
	}
	if MatchConditions(attrs, conds2) {
		t.Error("name==Jane OR age>35 should fail (both fail)")
	}
}

func TestMatchConditions_MixedAndOr(t *testing.T) {
	attrs := map[string]*atom.Atom{
		"age":    {Value: float64(30), Type: "number"},
		"name":   {Value: "John", Type: "string"},
		"city":   {Value: "NYC", Type: "string"},
		"active": {Value: true, Type: "boolean"},
	}

	conds := []Condition{
		{Field: "name", Operator: "==", Value: "John", Logic: "OR"},
		{Field: "city", Operator: "==", Value: "NYC", Logic: "AND"},
		{Field: "active", Operator: "==", Value: true, Logic: ""},
	}

	if !MatchConditions(attrs, conds) {
		t.Error("(name==John OR city==NYC) AND active==true AND age==30 should pass")
	}
}

func TestMatchConditions_MissingField(t *testing.T) {
	attrs := map[string]*atom.Atom{
		"age": {Value: float64(30), Type: "number"},
	}
	conds := []Condition{
		{Field: "name", Operator: "==", Value: "John", Logic: ""},
	}
	if MatchConditions(attrs, conds) {
		t.Error("missing field should return false")
	}
}

func TestMatchConditions_Deleted(t *testing.T) {
	attrs := map[string]*atom.Atom{
		"name": {Value: "John", Type: "deleted"},
	}
	conds := []Condition{
		{Field: "name", Operator: "==", Value: "John", Logic: ""},
	}
	if MatchConditions(attrs, conds) {
		t.Error("deleted field should return false")
	}
}

func TestCompareAtomValue(t *testing.T) {
	tests := []struct {
		atomVal  interface{}
		op       string
		condVal  interface{}
		expected bool
	}{
		{float64(25), "==", float64(25), true},
		{float64(25), "==", float64(30), false},
		{float64(25), "!=", float64(30), true},
		{float64(25), ">", float64(20), true},
		{float64(25), "<", float64(30), true},
		{float64(25), ">=", float64(25), true},
		{float64(25), "<=", float64(25), true},
		{float64(25), ">", float64(25), false},
		{float64(25), "<", float64(25), false},
		{"John", "==", "John", true},
		{"John", "==", "Jane", false},
		{true, "==", true, true},
		{false, "==", false, true},
		{float64(25), ">", float64(25), false},
	}
	for _, tt := range tests {
		result := compareAtomValue(tt.atomVal, tt.op, tt.condVal)
		if result != tt.expected {
			t.Errorf("compare(%v %s %v) = %v, want %v", tt.atomVal, tt.op, tt.condVal, result, tt.expected)
		}
	}
}

func TestParseWhere_Operators(t *testing.T) {
	for _, op := range []string{"==", "!=", ">", "<", ">=", "<="} {
		parsed, err := Parse("person where age " + op + " 25")
		if err != nil {
			t.Errorf("Parse with op %q: %v", op, err)
			continue
		}
		if len(parsed.Query.Conditions) != 1 {
			t.Errorf("op %q: got %d conditions, want 1", op, len(parsed.Query.Conditions))
		} else if parsed.Query.Conditions[0].Operator != op {
			t.Errorf("op %q: got operator %q", op, parsed.Query.Conditions[0].Operator)
		}
	}
}

func TestParseWhere_QuotedStrings(t *testing.T) {
	parsed, err := Parse(`person where city == "New, York"`)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(parsed.Query.Conditions) != 1 {
		t.Fatalf("Conditions = %d, want 1", len(parsed.Query.Conditions))
	}
	if parsed.Query.Conditions[0].Value != "New, York" {
		t.Errorf("Value = %v, want %q", parsed.Query.Conditions[0].Value, "New, York")
	}
}

func TestParseSetPairs_Commas(t *testing.T) {
	parsed, err := Parse(`UPDATE person where name == John set city = "New, York", age = 30`)
	if err != nil {
		t.Fatalf("Parse UPDATE with comma: %v", err)
	}
	cityVal, ok := parsed.Update.SetFields["city"]
	if !ok {
		t.Error("city field not found in SetFields")
	} else if cityVal != "New, York" {
		t.Errorf("city = %v, want %q", cityVal, "New, York")
	}
	ageVal, ok := parsed.Update.SetFields["age"]
	if !ok {
		t.Error("age field not found in SetFields")
	} else if ageVal != float64(30) {
		t.Errorf("age = %v, want 30", ageVal)
	}
}

func TestSortResults(t *testing.T) {
	rows := []map[string]interface{}{
		{"_entity": "e1", "age": float64(25)},
		{"_entity": "e2", "age": float64(30)},
		{"_entity": "e3", "age": float64(20)},
	}

	SortResults(rows, &OrderBy{Field: "age", Desc: false})
	if rows[0]["age"] != float64(20) || rows[2]["age"] != float64(30) {
		t.Error("sort ascending failed")
	}

	SortResults(rows, &OrderBy{Field: "age", Desc: true})
	if rows[0]["age"] != float64(30) || rows[2]["age"] != float64(20) {
		t.Error("sort descending failed")
	}
}

func TestConditionLogic(t *testing.T) {
	_, err := Parse("person where name == John and age > 25")
	if err != nil {
		t.Fatalf("AND parse error: %v", err)
	}
	_, err = Parse("person where name == John or age > 25")
	if err != nil {
		t.Fatalf("OR parse error: %v", err)
	}
	_, err = Parse("person where name == John or age > 25 and city == NYC")
	if err != nil {
		t.Fatalf("mixed AND/OR parse error: %v", err)
	}
}

func TestOrderByCaseInsensitive(t *testing.T) {
	tests := []string{"desc", "Desc", "DESC", "dEsC"}
	for _, c := range tests {
		parsed, err := Parse("person order by name " + c + " limit 10 where age > 25")
		if err != nil {
			t.Errorf("Parse with %q: %v", c, err)
			continue
		}
		if parsed.Query.OrderBy == nil || !parsed.Query.OrderBy.Desc {
			t.Errorf("order by name %s: Desc should be true, got %+v", c, parsed.Query.OrderBy)
		}
		if parsed.Query.Limit != 10 {
			t.Errorf("order by name %s: Limit should be 10, got %d", c, parsed.Query.Limit)
		}
	}
}

var _ = reflect.ValueOf
