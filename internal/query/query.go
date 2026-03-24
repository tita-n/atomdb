package query

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tita-n/atomdb/internal/atom"
)

// Condition represents a single WHERE clause condition.
type Condition struct {
	Field    string
	Operator string // ==, !=, >, <, >=, <=
	Value    interface{}
	Logic    string // AND or OR to next condition (empty for last)
}

// OrderBy represents an ORDER BY clause.
type OrderBy struct {
	Field string
	Desc  bool
}

// Query represents a parsed query.
type Query struct {
	TypeName   string
	Fields     []string // fields to return (empty = all)
	Conditions []Condition
	OrderBy    *OrderBy
	Limit      int
	GroupBy    string
	Aggregate  string // count, sum, avg, min, max
	AggField   string // field for aggregate
}

// ParsedInput is the result of parsing a query string.
type ParsedInput struct {
	Command string // SELECT, INSERT, UPDATE, DELETE, TYPE, etc
	Query   *Query
	Insert  *InsertOp
	Update  *UpdateOp
	Delete  *DeleteOp
	TypeDef *TypeDefOp
	Raw     string // raw command for unrecognized
}

// InsertOp represents an INSERT command.
type InsertOp struct {
	TypeName string
	Fields   map[string]interface{}
}

// UpdateOp represents an UPDATE command.
type UpdateOp struct {
	TypeName   string
	Conditions []Condition
	SetFields  map[string]interface{}
}

// DeleteOp represents a DELETE command.
type DeleteOp struct {
	TypeName   string
	Conditions []Condition
}

// TypeDefOp represents a TYPE definition.
type TypeDefOp struct {
	Name string
	Body string // raw body for schema package to parse
}

// Parse parses a command string into a structured ParsedInput.
func Parse(input string) (*ParsedInput, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty input")
	}

	upper := strings.ToUpper(input)

	switch {
	case strings.HasPrefix(upper, "TYPE "):
		return parseType(input)
	case strings.HasPrefix(upper, "INSERT "):
		return parseInsert(input)
	case strings.HasPrefix(upper, "UPDATE "):
		return parseUpdate(input)
	case strings.HasPrefix(upper, "DELETE "):
		return parseDelete(input)
	case strings.HasPrefix(upper, "SELECT "):
		return parseSelect(input[7:])
	default:
		// Try as shorthand query: "type where condition" or "type.field where ..."
		return parseShorthand(input)
	}
}

func parseType(input string) (*ParsedInput, error) {
	// TYPE name { ... }
	return &ParsedInput{
		Command: "TYPE",
		TypeDef: &TypeDefOp{
			Name: "",
			Body: input,
		},
	}, nil
}

func parseInsert(input string) (*ParsedInput, error) {
	// INSERT type field:value field:value
	rest := strings.TrimSpace(input[7:])

	// First word is type name
	parts := strings.SplitN(rest, " ", 2)
	typeName := parts[0]
	var fieldsStr string
	if len(parts) > 1 {
		fieldsStr = parts[1]
	}

	fields, err := parseFieldPairs(fieldsStr)
	if err != nil {
		return nil, fmt.Errorf("INSERT: %w", err)
	}

	return &ParsedInput{
		Command: "INSERT",
		Insert: &InsertOp{
			TypeName: typeName,
			Fields:   fields,
		},
	}, nil
}

func parseUpdate(input string) (*ParsedInput, error) {
	// UPDATE type where condition set field = value
	rest := strings.TrimSpace(input[7:])

	// Find "where"
	whereIdx := strings.Index(strings.ToLower(rest), " where ")
	if whereIdx < 0 {
		return nil, fmt.Errorf("UPDATE requires WHERE clause")
	}

	typeName := strings.TrimSpace(rest[:whereIdx])
	rest = strings.TrimSpace(rest[whereIdx+7:])

	// Find "set"
	setIdx := strings.Index(strings.ToLower(rest), " set ")
	if setIdx < 0 {
		return nil, fmt.Errorf("UPDATE requires SET clause")
	}

	whereStr := strings.TrimSpace(rest[:setIdx])
	setStr := strings.TrimSpace(rest[setIdx+5:])

	conditions, err := parseWhere(whereStr)
	if err != nil {
		return nil, fmt.Errorf("UPDATE WHERE: %w", err)
	}

	setFields, err := parseSetPairs(setStr)
	if err != nil {
		return nil, fmt.Errorf("UPDATE SET: %w", err)
	}

	return &ParsedInput{
		Command: "UPDATE",
		Update: &UpdateOp{
			TypeName:   typeName,
			Conditions: conditions,
			SetFields:  setFields,
		},
	}, nil
}

func parseDelete(input string) (*ParsedInput, error) {
	// DELETE type where condition
	rest := strings.TrimSpace(input[7:])

	whereIdx := strings.Index(strings.ToLower(rest), " where ")
	if whereIdx < 0 {
		return nil, fmt.Errorf("DELETE requires WHERE clause")
	}

	typeName := strings.TrimSpace(rest[:whereIdx])
	whereStr := strings.TrimSpace(rest[whereIdx+7:])

	conditions, err := parseWhere(whereStr)
	if err != nil {
		return nil, fmt.Errorf("DELETE WHERE: %w", err)
	}

	return &ParsedInput{
		Command: "DELETE",
		Delete: &DeleteOp{
			TypeName:   typeName,
			Conditions: conditions,
		},
	}, nil
}

func parseSelect(input string) (*ParsedInput, error) {
	// [fields] [from type] [where conditions] [order by field] [limit n] [group by field]
	q := &Query{}
	input = strings.TrimSpace(input)

	// Find "from" keyword
	fromIdx := strings.Index(strings.ToLower(input), " from ")
	var fieldsStr string
	if fromIdx >= 0 {
		fieldsStr = strings.TrimSpace(input[:fromIdx])
		input = strings.TrimSpace(input[fromIdx+6:])
	} else {
		// No FROM: parse as "field.field where ..."
		return parseShorthand("SELECT " + input)
	}

	// Parse fields
	if fieldsStr != "" && fieldsStr != "*" {
		for _, f := range strings.Split(fieldsStr, ",") {
			q.Fields = append(q.Fields, strings.TrimSpace(f))
		}
	}

	// First word of remaining is type name
	parts := strings.SplitN(input, " ", 2)
	q.TypeName = parts[0]
	if len(parts) > 1 {
		input = strings.TrimSpace(parts[1])
	} else {
		input = ""
	}

	// Parse WHERE
	if strings.HasPrefix(strings.ToLower(input), "where ") {
		input = strings.TrimSpace(input[6:])
		rest := input

		// Find next clause (order, limit, group)
		clauseIdx := findClauseIndex(rest)
		var whereStr string
		if clauseIdx >= 0 {
			whereStr = strings.TrimSpace(rest[:clauseIdx])
			rest = strings.TrimSpace(rest[clauseIdx:])
		} else {
			whereStr = rest
			rest = ""
		}

		conditions, err := parseWhere(whereStr)
		if err != nil {
			return nil, err
		}
		q.Conditions = conditions
		input = rest
	}

	// Parse ORDER BY
	if strings.HasPrefix(strings.ToLower(input), "order by ") {
		input = strings.TrimSpace(input[9:])
		parts := strings.SplitN(input, " ", 2)
		field := strings.TrimSpace(parts[0])
		desc := false
		if len(parts) > 1 {
			next := strings.ToLower(strings.TrimSpace(parts[1]))
			if strings.HasPrefix(next, "desc") {
				desc = true
				input = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(parts[1]), "desc"))
			} else {
				input = strings.TrimSpace(parts[1])
			}
		} else {
			input = ""
		}
		q.OrderBy = &OrderBy{Field: field, Desc: desc}
	}

	// Parse LIMIT
	if strings.HasPrefix(strings.ToLower(input), "limit ") {
		input = strings.TrimSpace(input[6:])
		parts := strings.SplitN(input, " ", 2)
		n, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err == nil {
			q.Limit = n
		}
	}

	return &ParsedInput{
		Command: "SELECT",
		Query:   q,
	}, nil
}

func parseShorthand(input string) (*ParsedInput, error) {
	// "person where age > 25" → SELECT * FROM person WHERE age > 25
	// "person.name where city == Lagos" → SELECT name FROM person WHERE city == "Lagos"
	// Supports: "person order by name limit 10 where age > 25"

	lower := strings.ToLower(input)

	whereIdx := strings.Index(lower, " where ")
	if whereIdx < 0 {
		return &ParsedInput{
			Command: "SELECT",
			Query: &Query{
				TypeName: strings.TrimSpace(input),
			},
		}, nil
	}

	beforeWhere := strings.TrimSpace(input[:whereIdx])
	afterWhere := strings.TrimSpace(input[whereIdx+7:])

	q := &Query{}

	typeAndClauses := strings.TrimSpace(beforeWhere)
	orderIdx := strings.Index(strings.ToLower(typeAndClauses), " order by ")
	limitIdx := strings.Index(strings.ToLower(typeAndClauses), " limit ")

	if orderIdx >= 0 && (limitIdx < 0 || orderIdx < limitIdx) {
		typePart := strings.TrimSpace(typeAndClauses[:orderIdx])
		rest := strings.TrimSpace(typeAndClauses[orderIdx+10:])
		q.TypeName = extractTypeAndFields(typePart, q)
		rest = parseOrderByAndLimit(rest, q)
	} else if limitIdx >= 0 {
		typePart := strings.TrimSpace(typeAndClauses[:limitIdx])
		rest := strings.TrimSpace(typeAndClauses[limitIdx+7:])
		q.TypeName = extractTypeAndFields(typePart, q)
		if n, err := strconv.Atoi(strings.TrimSpace(strings.Fields(rest)[0])); err == nil {
			q.Limit = n
		}
	} else {
		q.TypeName = extractTypeAndFields(typeAndClauses, q)
	}

	conditions, err := parseWhere(afterWhere)
	if err != nil {
		return nil, err
	}
	q.Conditions = conditions

	// Also extract ORDER BY and LIMIT from afterWhere (after "where")
	if q.OrderBy == nil {
		afterWhereLower := strings.ToLower(afterWhere)
		orderIdx := strings.Index(afterWhereLower, " order by ")
		limitIdx := strings.Index(afterWhereLower, " limit ")

		if orderIdx >= 0 && (limitIdx < 0 || orderIdx < limitIdx) {
			beforeOrder := strings.TrimSpace(afterWhere[:orderIdx])
			q.Conditions, _ = parseWhere(beforeOrder)
			remaining := strings.TrimSpace(afterWhere[orderIdx+10:])
			parts := strings.SplitN(remaining, " ", 2)
			field := strings.TrimSpace(parts[0])
			desc := false
			rest := ""
			if len(parts) > 1 {
				rest = strings.TrimSpace(parts[1])
				if strings.HasPrefix(strings.ToLower(rest), "desc") {
					desc = true
					rest = strings.TrimSpace(rest[4:])
				}
			}
			q.OrderBy = &OrderBy{Field: field, Desc: desc}
			if rest != "" {
				restLower := strings.ToLower(rest)
				li := strings.Index(restLower, "limit ")
				if li >= 0 {
					q.Limit, _ = strconv.Atoi(strings.TrimSpace(strings.Fields(strings.TrimSpace(rest[li+6:]))[0]))
				}
			}
		} else if limitIdx >= 0 && q.Limit == 0 {
			beforeLimit := strings.TrimSpace(afterWhere[:limitIdx])
			q.Conditions, _ = parseWhere(beforeLimit)
			q.Limit, _ = strconv.Atoi(strings.TrimSpace(strings.Fields(strings.TrimSpace(afterWhere[limitIdx+7:]))[0]))
		}
	}

	return &ParsedInput{
		Command: "SELECT",
		Query:   q,
	}, nil
}

func extractTypeAndFields(beforeWhere string, q *Query) string {
	if strings.Contains(beforeWhere, ".") {
		parts := strings.Fields(beforeWhere)
		typeName := ""
		for _, part := range parts {
			if strings.Contains(part, ".") {
				dotParts := strings.SplitN(part, ".", 2)
				if typeName == "" {
					typeName = dotParts[0]
				}
				q.Fields = append(q.Fields, dotParts[1])
			} else if typeName == "" {
				typeName = part
			}
		}
		return typeName
	}
	return beforeWhere
}

func parseOrderByAndLimit(rest string, q *Query) string {
	parts := strings.SplitN(rest, " ", 2)
	field := strings.TrimSpace(parts[0])
	desc := false
	remaining := ""
	if len(parts) > 1 {
		remaining = strings.TrimSpace(parts[1])
		lowerNext := strings.ToLower(remaining)
		if strings.HasPrefix(lowerNext, "desc") {
			desc = true
			remaining = strings.TrimSpace(remaining[4:])
		} else if strings.HasPrefix(lowerNext, "asc") {
			desc = false
			remaining = strings.TrimSpace(remaining[3:])
		}
	}
	q.OrderBy = &OrderBy{Field: field, Desc: desc}

	if remaining != "" {
		lowerRem := strings.ToLower(remaining)
		li := strings.Index(lowerRem, "limit ")
		if li >= 0 {
			limitStr := strings.TrimSpace(remaining[li+6:])
			if n, err := strconv.Atoi(strings.TrimSpace(strings.Fields(limitStr)[0])); err == nil {
				q.Limit = n
			}
			remaining = strings.TrimSpace(remaining[:li])
		}
	}
	return remaining
}

// parseWhere parses WHERE clause conditions.
// Supports: field op value [AND|OR field op value ...]
func parseWhere(input string) ([]Condition, error) {
	var conditions []Condition
	tokens := tokenizeWhere(input)

	i := 0
	for i < len(tokens) {
		if i+2 >= len(tokens) {
			break
		}

		field := tokens[i]
		op := tokens[i+1]
		val := tokens[i+2]

		if !isOperator(op) {
			return nil, fmt.Errorf("expected operator, got %q", op)
		}

		logic := ""
		i += 3
		if i < len(tokens) {
			upper := strings.ToUpper(tokens[i])
			if upper == "AND" || upper == "OR" {
				logic = upper
				i++
			}
		}

		conditions = append(conditions, Condition{
			Field:    field,
			Operator: op,
			Value:    parseValue(val),
			Logic:    logic,
		})
	}

	return conditions, nil
}

func tokenizeWhere(input string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for i := 0; i < len(input); i++ {
		r := rune(input[i])
		if inQuote {
			if r == quoteChar {
				inQuote = false
				tokens = append(tokens, current.String())
				current.Reset()
			} else {
				current.WriteRune(r)
			}
			continue
		}

		if r == '"' || r == '\'' {
			inQuote = true
			quoteChar = r
			continue
		}

		if r == ' ' || r == '\t' || r == '\n' {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			continue
		}

		// Operators as separate tokens
		if r == '=' || r == '!' || r == '>' || r == '<' {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			// Check for ==, !=, >=, <=
			if i+1 < len(input) {
				next := rune(input[i+1])
				if (r == '=' && next == '=') ||
					(r == '!' && next == '=') ||
					(r == '>' && next == '=') ||
					(r == '<' && next == '=') {
					tokens = append(tokens, string(r)+string(next))
					i++ // skip the second character
					continue
				}
			}
			tokens = append(tokens, string(r))
			continue
		}

		current.WriteRune(r)
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

func isOperator(s string) bool {
	switch s {
	case "==", "!=", ">", "<", ">=", "<=":
		return true
	}
	return false
}

func parseValue(s string) interface{} {
	// Try number
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	// Try boolean
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}
	// Try nil
	if s == "nil" || s == "null" {
		return nil
	}
	// String
	return s
}

func parseFieldPairs(input string) (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	if input == "" {
		return fields, nil
	}

	tokens := tokenizeWhere(input)
	i := 0
	for i < len(tokens) {
		token := tokens[i]
		// Expect field:value
		if strings.Contains(token, ":") {
			parts := strings.SplitN(token, ":", 2)
			fields[parts[0]] = parseValue(parts[1])
			i++
			continue
		}
		// Or field: value (separate tokens)
		if i+1 < len(tokens) && tokens[i+1] == ":" {
			if i+2 < len(tokens) {
				fields[token] = parseValue(tokens[i+2])
				i += 3
				continue
			}
		}
		// Or key value pairs with space
		if i+1 < len(tokens) && !isOperator(tokens[i+1]) && tokens[i+1] != ":" {
			fields[token] = parseValue(tokens[i+1])
			i += 2
			continue
		}
		i++
	}

	return fields, nil
}

func parseSetPairs(input string) (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	if input == "" {
		return fields, nil
	}
	tokens := tokenizeWhere(input)
	i := 0
	for i < len(tokens) {
		field := tokens[i]
		if i+2 < len(tokens) && tokens[i+1] == "=" {
			fields[field] = parseValue(tokens[i+2])
			i += 3
			continue
		}
		i++
	}
	return fields, nil
}

func findClauseIndex(input string) int {
	clauses := []string{"order by", "limit", "group by"}
	lower := strings.ToLower(input)
	minIdx := -1
	for _, c := range clauses {
		idx := strings.Index(lower, c)
		if idx >= 0 && (minIdx < 0 || idx < minIdx) {
			minIdx = idx
		}
	}
	return minIdx
}

// MatchConditions checks if an entity's attributes satisfy the conditions.
// Each top-level OR-separated group must pass (AND within group).
// e.g. "age > 25 OR name == 'John'" evaluates as two OR groups.
func MatchConditions(attrs map[string]*atom.Atom, conditions []Condition) bool {
	if len(conditions) == 0 {
		return true
	}

	orGroups := [][]Condition{}
	var currentGroup []Condition

	for i, cond := range conditions {
		currentGroup = append(currentGroup, cond)
		if i < len(conditions)-1 && strings.ToUpper(conditions[i].Logic) == "OR" {
			orGroups = append(orGroups, currentGroup)
			currentGroup = nil
		}
	}
	if len(currentGroup) > 0 {
		orGroups = append(orGroups, currentGroup)
	}

	for _, group := range orGroups {
		groupPass := true
		for _, cond := range group {
			a, ok := attrs[cond.Field]
			if !ok || a.Type == "deleted" || !compareAtomValue(a.Value, cond.Operator, cond.Value) {
				groupPass = false
				break
			}
		}
		if groupPass {
			return true
		}
	}
	return false
}

func compareAtomValue(atomVal interface{}, op string, condVal interface{}) bool {
	// Try numeric comparison
	af := toFloat(atomVal)
	bf := toFloat(condVal)

	switch op {
	case "==":
		return fmt.Sprintf("%v", atomVal) == fmt.Sprintf("%v", condVal)
	case "!=":
		return fmt.Sprintf("%v", atomVal) != fmt.Sprintf("%v", condVal)
	case ">":
		return af > bf
	case "<":
		return af < bf
	case ">=":
		return af >= bf
	case "<=":
		return af <= bf
	}
	return false
}

func toFloat(v interface{}) float64 {
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
		if f, err := strconv.ParseFloat(fmt.Sprintf("%v", v), 64); err == nil {
			return f
		}
		return 0
	}
}

// SortResults sorts query results by a field.
func SortResults(results []map[string]interface{}, orderBy *OrderBy) {
	if orderBy == nil {
		return
	}
	sort.Slice(results, func(i, j int) bool {
		vi := results[i][orderBy.Field]
		vj := results[j][orderBy.Field]
		if vi == nil || vj == nil {
			return false
		}
		fi := toFloat(vi)
		fj := toFloat(vj)
		if orderBy.Desc {
			return fi > fj
		}
		return fi < fj
	})
}

// Aggregate computes an aggregate function over result rows.
func Aggregate(rows []map[string]interface{}, fn string, field string) (interface{}, error) {
	if len(rows) == 0 {
		switch fn {
		case "count":
			return 0, nil
		default:
			return nil, fmt.Errorf("no rows to aggregate")
		}
	}

	switch fn {
	case "count":
		return float64(len(rows)), nil

	case "sum":
		var total float64
		for _, row := range rows {
			if v, ok := row[field]; ok && v != nil {
				total += toFloat(v)
			}
		}
		return total, nil

	case "avg":
		var total float64
		count := 0
		for _, row := range rows {
			if v, ok := row[field]; ok && v != nil {
				total += toFloat(v)
				count++
			}
		}
		if count == 0 {
			return nil, fmt.Errorf("no non-null values for avg")
		}
		return total / float64(count), nil

	case "min":
		var min float64
		found := false
		for _, row := range rows {
			if v, ok := row[field]; ok && v != nil {
				f := toFloat(v)
				if !found || f < min {
					min = f
					found = true
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("no non-null values for min")
		}
		return min, nil

	case "max":
		var max float64
		found := false
		for _, row := range rows {
			if v, ok := row[field]; ok && v != nil {
				f := toFloat(v)
				if !found || f > max {
					max = f
					found = true
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("no non-null values for max")
		}
		return max, nil

	default:
		return nil, fmt.Errorf("unknown aggregate function: %q", fn)
	}
}

// GroupByResults groups rows by a field value.
func GroupByResults(rows []map[string]interface{}, groupField string) map[string][]map[string]interface{} {
	groups := make(map[string][]map[string]interface{})
	for _, row := range rows {
		key := fmt.Sprintf("%v", row[groupField])
		groups[key] = append(groups[key], row)
	}
	return groups
}
