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
	Logic      string // AND or OR between conditions
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
			}
			input = strings.TrimSpace(parts[1])
			if desc {
				input = strings.TrimPrefix(input, "desc")
				input = strings.TrimPrefix(input, "DESC")
				input = strings.TrimSpace(input)
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

	lower := strings.ToLower(input)

	// Find "where"
	whereIdx := strings.Index(lower, " where ")
	if whereIdx < 0 {
		// Just a type name - return all
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

	// Parse "type.field1, field2" or just "type"
	if strings.Contains(beforeWhere, ".") {
		// Split on spaces to get individual type.field pairs
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
		q.TypeName = typeName
	} else {
		q.TypeName = beforeWhere
	}

	// Parse conditions
	conditions, err := parseWhere(afterWhere)
	if err != nil {
		return nil, err
	}
	q.Conditions = conditions

	return &ParsedInput{
		Command: "SELECT",
		Query:   q,
	}, nil
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

		conditions = append(conditions, Condition{
			Field:    field,
			Operator: op,
			Value:    parseValue(val),
		})

		i += 3
		// Skip AND/OR
		if i < len(tokens) {
			upper := strings.ToUpper(tokens[i])
			if upper == "AND" || upper == "OR" {
				i++
			}
		}
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
	// field = value, field = value
	parts := strings.Split(input, ",")
	for _, part := range parts {
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		field := strings.TrimSpace(part[:eqIdx])
		val := strings.TrimSpace(part[eqIdx+1:])
		fields[field] = parseValue(val)
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
func MatchConditions(attrs map[string]*atom.Atom, conditions []Condition) bool {
	if len(conditions) == 0 {
		return true
	}

	for _, cond := range conditions {
		a, ok := attrs[cond.Field]
		if !ok {
			return false
		}
		if a.Type == "deleted" {
			return false
		}
		if !compareAtomValue(a.Value, cond.Operator, cond.Value) {
			return false
		}
	}
	return true
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
