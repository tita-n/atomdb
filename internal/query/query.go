package query

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

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
	lower := strings.ToLower(input)

	// Route explicit commands first, before checking for aggregation keywords
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
	}

	// Then check for group by and aggregation in shorthand queries
	if strings.Contains(lower, "group by") || hasAggFunc(lower) {
		return parseShorthand(input)
	}

	// Try as shorthand query: "type where condition" or "type.field where ..."
	return parseShorthand(input)
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
		if len(parts) > 1 {
			input = strings.TrimSpace(parts[1])
		} else {
			input = ""
		}
	}

	// Parse GROUP BY
	if strings.HasPrefix(strings.ToLower(input), "group by ") {
		groupField := strings.TrimSpace(input[9:])
		// Check for aggregation in fields part (e.g., "count(*)" in selected fields)
		for i, f := range q.Fields {
			if af, _, afField := parseAggFunc(f); af != "" {
				q.Aggregate = af
				q.AggField = afField
				q.Fields = append(q.Fields[:i], q.Fields[i+1:]...)
				break
			}
		}
		// Infer type from group field or selected fields
		if dotIdx := strings.Index(groupField, "."); dotIdx >= 0 {
			q.TypeName = groupField[:dotIdx]
			q.GroupBy = groupField[dotIdx+1:]
		} else {
			q.GroupBy = groupField
			for _, f := range q.Fields {
				if dotIdx := strings.Index(f, "."); dotIdx >= 0 {
					q.TypeName = f[:dotIdx]
					break
				}
			}
		}
	}

	return &ParsedInput{
		Command: "SELECT",
		Query:   q,
	}, nil
}

func parseShorthand(input string) (*ParsedInput, error) {
	q := &Query{}

	lower := strings.ToLower(input)

	// Check for GROUP BY FIRST (before aggregation function check)
	if gbIdx := strings.Index(lower, " group by "); gbIdx >= 0 {
		fieldsPart := strings.TrimSpace(input[:gbIdx])
		groupField := strings.TrimSpace(input[gbIdx+10:])

		// Check if fieldsPart contains aggregation
		subAggFn, _, subAggField := parseAggFunc(fieldsPart)
		if subAggFn != "" {
			q.Aggregate = subAggFn
			q.AggField = subAggField
			fieldsPart = removeAggFunc(fieldsPart)
		}

		fieldsPart = strings.TrimSpace(fieldsPart)
		fieldsPart = strings.TrimSuffix(fieldsPart, ",")
		if fieldsPart != "" {
			for _, f := range strings.Split(fieldsPart, ",") {
				f = strings.TrimSpace(f)
				if f != "" && f != "*" {
					q.Fields = append(q.Fields, f)
				}
			}
		}

		q.GroupBy = groupField
		if dotIdx := strings.Index(groupField, "."); dotIdx >= 0 {
			q.TypeName = groupField[:dotIdx]
			q.GroupBy = groupField[dotIdx+1:]
		} else {
			for _, f := range q.Fields {
				if dotIdx := strings.Index(f, "."); dotIdx >= 0 {
					q.TypeName = f[:dotIdx]
					break
				}
			}
		}

		return &ParsedInput{
			Command: "SELECT",
			Query:   q,
		}, nil
	}

	// Check for aggregation functions: count(type), sum(type.field), etc.
	aggFn, aggType, aggField := parseAggFunc(input)
	if aggFn != "" {
		q.Aggregate = aggFn
		q.AggField = aggField
		q.TypeName = aggType

		// Remove the agg function from input, parse the rest
		input = removeAggFunc(input)

		// Extract ORDER BY and LIMIT
		input = strings.TrimSpace(extractOrderByLimit(input, q))

		// Parse WHERE if present
		lower := strings.ToLower(input)
		if strings.HasPrefix(lower, "where ") {
			condPart := strings.TrimSpace(input[6:])
			conditions, err := parseWhere(condPart)
			if err != nil {
				return nil, err
			}
			q.Conditions = conditions
		}

		return &ParsedInput{
			Command: "SELECT",
			Query:   q,
		}, nil
	}

	// Standard shorthand: "type where condition"
	// First extract ORDER BY and LIMIT from the full input
	input = strings.TrimSpace(extractOrderByLimit(input, q))

	// Now split on "where" (ORDER BY/LIMIT are already extracted)
	lower = strings.ToLower(input)
	whereIdx := strings.Index(lower, " where ")

	var typeSelector, condPart string
	if whereIdx < 0 {
		typeSelector = strings.TrimSpace(input)
		condPart = ""
	} else {
		typeSelector = strings.TrimSpace(input[:whereIdx])
		condPart = strings.TrimSpace(input[whereIdx+7:])
	}

	// Parse type name and field selection
	q.TypeName = extractTypeAndFields(typeSelector, q)

	// Parse conditions
	if condPart != "" {
		conditions, err := parseWhere(condPart)
		if err != nil {
			return nil, err
		}
		q.Conditions = conditions
	}

	return &ParsedInput{
		Command: "SELECT",
		Query:   q,
	}, nil
}

// parseAggFunc detects aggregation functions like count(person), sum(expense.amount)
// Returns (function, type, field).
func parseAggFunc(input string) (fn string, typeName string, field string) {
	aggFuncs := []string{"count", "sum", "avg", "min", "max"}
	lower := strings.ToLower(input)

	for _, af := range aggFuncs {
		idx := strings.Index(lower, af+"(")
		if idx < 0 {
			continue
		}
		// Find closing paren
		start := idx + len(af) + 1
		end := strings.Index(input[start:], ")")
		if end < 0 {
			continue
		}
		inner := input[start : start+end]

		// Parse "type.field" or "type" or "*"
		if inner == "*" {
			return af, "", ""
		}
		if dotIdx := strings.Index(inner, "."); dotIdx >= 0 {
			return af, inner[:dotIdx], inner[dotIdx+1:]
		}
		return af, inner, ""
	}
	return "", "", ""
}

// removeAggFunc strips aggregation function from input string.
// Handles nested parentheses correctly by tracking depth.
func removeAggFunc(input string) string {
	aggFuncs := []string{"count", "sum", "avg", "min", "max"}
	lower := strings.ToLower(input)

	for _, af := range aggFuncs {
		idx := strings.Index(lower, af+"(")
		if idx < 0 {
			continue
		}
		start := idx + len(af) // position of '('
		depth := 0
		end := -1
		for i := start; i < len(input); i++ {
			if input[i] == '(' {
				depth++
			} else if input[i] == ')' {
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		if end < 0 {
			continue
		}
		before := strings.TrimSpace(input[:idx])
		after := strings.TrimSpace(input[end+1:])
		return strings.TrimSpace(before + " " + after)
	}
	return input
}

// hasAggFunc checks if the input contains an aggregation function.
func hasAggFunc(lower string) bool {
	for _, fn := range []string{"count(", "sum(", "avg(", "min(", "max("} {
		if strings.Contains(lower, fn) {
			return true
		}
	}
	return false
}

// inferTypeFromField tries to guess the type name from a field reference.
func inferTypeFromField(field string) string {
	// If it looks like "type.field", return the type part
	if dotIdx := strings.Index(field, "."); dotIdx >= 0 {
		return field[:dotIdx]
	}
	return ""
}

// extractOrderByLimit extracts ORDER BY and LIMIT from an input string.
// Returns the remaining part of the string after removing ORDER BY/LIMIT clauses.
func extractOrderByLimit(input string, q *Query) string {
	if q == nil {
		q = &Query{}
	}
	input = strings.TrimSpace(input)
	lower := strings.ToLower(input)

	orderIdx := strings.Index(lower, "order by")
	limitIdx := strings.Index(lower, "limit ")

	if orderIdx >= 0 && (limitIdx < 0 || orderIdx < limitIdx) {
		before := strings.TrimSpace(input[:orderIdx])
		rest := strings.TrimSpace(input[orderIdx+8:])
		parts := strings.SplitN(rest, " ", 2)
		q.OrderBy = &OrderBy{Field: strings.TrimSpace(parts[0]), Desc: false}
		remaining := ""
		if len(parts) > 1 {
			remaining = strings.TrimSpace(parts[1])
			if strings.HasPrefix(strings.ToLower(remaining), "desc") {
				q.OrderBy.Desc = true
				remaining = strings.TrimSpace(remaining[4:])
			} else if strings.HasPrefix(strings.ToLower(remaining), "asc") {
				remaining = strings.TrimSpace(remaining[3:])
			}
		}
		if remaining != "" {
			li := strings.Index(strings.ToLower(remaining), "limit ")
			if li >= 0 {
				afterLimit := strings.TrimSpace(remaining[li+6:])
				limitFields := strings.Fields(afterLimit)
				if len(limitFields) > 0 {
					if n, err := strconv.Atoi(limitFields[0]); err == nil {
						q.Limit = n
					}
					if len(limitFields) > 1 {
						remaining = strings.TrimSpace(strings.Join(limitFields[1:], " "))
					} else {
						remaining = ""
					}
				}
			}
		}
		return strings.TrimSpace(before + " " + remaining)
	} else if limitIdx >= 0 && q.Limit == 0 {
		before := strings.TrimSpace(input[:limitIdx])
		rest := strings.TrimSpace(input[limitIdx+6:])
		if n, err := strconv.Atoi(strings.TrimSpace(strings.Fields(rest)[0])); err == nil {
			q.Limit = n
		}
		return before
	}
	return input
}

func extractOrderLimit(input string, q *Query) (typeName string, remainder string) {
	typeName = strings.TrimSpace(input)
	lower := strings.ToLower(input)
	orderIdx := strings.Index(lower, "order by")
	limitIdx := strings.Index(lower, "limit ")
	if orderIdx >= 0 && (limitIdx < 0 || orderIdx < limitIdx) {
		typeName = strings.TrimSpace(input[:orderIdx])
		remainder = strings.TrimSpace(input[orderIdx+9:])
		parts := strings.SplitN(remainder, " ", 2)
		field := strings.TrimSpace(parts[0])
		desc := false
		rest := ""
		if len(parts) > 1 {
			rest = strings.TrimSpace(parts[1])
			lowerRest := strings.ToLower(rest)
			if strings.HasPrefix(lowerRest, "desc") {
				desc = true
				rest = strings.TrimSpace(rest[4:])
			} else if strings.HasPrefix(lowerRest, "asc") {
				rest = strings.TrimSpace(rest[3:])
			}
		}
		q.OrderBy = &OrderBy{Field: field, Desc: desc}
		if rest != "" {
			li := strings.Index(strings.ToLower(rest), "limit ")
			if li >= 0 {
				rest = strings.TrimSpace(rest[li+6:])
				if n, err := strconv.Atoi(strings.TrimSpace(strings.Fields(rest)[0])); err == nil {
					q.Limit = n
				}
				remainder = strings.TrimSpace(rest[:li])
			}
		}
	} else if limitIdx >= 0 {
		typeName = strings.TrimSpace(input[:limitIdx])
		remainder = strings.TrimSpace(input[limitIdx+6:])
		if n, err := strconv.Atoi(strings.TrimSpace(strings.Fields(remainder)[0])); err == nil {
			q.Limit = n
		}
	}
	return typeName, remainder
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
			// Trailing incomplete condition
			if i < len(tokens) {
				return nil, fmt.Errorf("incomplete condition at %q (need field, operator, value)", tokens[i])
			}
			break
		}

		field := tokens[i]
		op := tokens[i+1]
		val := tokens[i+2]

		if !isOperator(op) {
			return nil, fmt.Errorf("expected operator, got %q", op)
		}

		// Validate field name to prevent injection
		if err := validateFieldName(field); err != nil {
			return nil, fmt.Errorf("invalid field name %q: %w", field, err)
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
	if !utf8.ValidString(input) {
		return nil
	}
	var tokens []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	runes := []rune(input)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		// Reject control chars in unquoted context
		if !inQuote && unicode.IsControl(r) && r != '\t' && r != '\n' && r != '\r' {
			continue
		}

		if inQuote {
			if r == '\\' && i+1 < len(runes) {
				i++
				next := runes[i]
				switch next {
				case '"', '\'', '\\':
					current.WriteRune(next)
				case 'n':
					current.WriteRune('\n')
				case 't':
					current.WriteRune('\t')
				default:
					current.WriteRune(r)
					current.WriteRune(next)
				}
				continue
			}
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

		if unicode.IsSpace(r) {
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
			if i+1 < len(runes) {
				next := runes[i+1]
				if (r == '=' && next == '=') ||
					(r == '!' && next == '=') ||
					(r == '>' && next == '=') ||
					(r == '<' && next == '=') {
					tokens = append(tokens, string(r)+string(next))
					i++
					continue
				}
			}
			tokens = append(tokens, string(r))
			continue
		}

		current.WriteRune(r)
	}
	if current.Len() > 0 {
		if inQuote {
			// Unterminated quote — treat as raw token
			tokens = append(tokens, current.String())
		} else {
			tokens = append(tokens, current.String())
		}
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

// validateFieldName checks that a field name contains only safe characters.
// Fields may be "type.field" references.
func validateFieldName(name string) error {
	if name == "" {
		return fmt.Errorf("field name cannot be empty")
	}
	if len(name) > 256 {
		return fmt.Errorf("field name too long")
	}
	for i, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("control character at position %d", i)
		}
		if r == '\u2028' || r == '\u2029' {
			return fmt.Errorf("Unicode line separator at position %d", i)
		}
		if r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
			return fmt.Errorf("unsafe character %q at position %d", r, i)
		}
	}
	return nil
}

func parseValue(s string) interface{} {
	// Try number
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		// Reject NaN and Infinity to prevent injection
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return s
		}
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

	// Track quoted regions to avoid matching clause keywords inside quotes
	inQuote := false
	quoteChar := byte(0)
	for i := 0; i < len(input); i++ {
		if inQuote {
			if input[i] == quoteChar {
				inQuote = false
			}
			continue
		}
		if input[i] == '"' || input[i] == '\'' {
			inQuote = true
			quoteChar = input[i]
			continue
		}
	}

	// Build a mask of quoted positions
	quoted := make([]bool, len(input))
	inQuote = false
	quoteChar = 0
	for i := 0; i < len(input); i++ {
		if inQuote {
			quoted[i] = true
			if input[i] == quoteChar {
				inQuote = false
			}
			continue
		}
		if input[i] == '"' || input[i] == '\'' {
			inQuote = true
			quoteChar = input[i]
			quoted[i] = true
		}
	}

	for _, c := range clauses {
		idx := strings.Index(lower, c)
		// Walk through all matches, skip those inside quotes
		for idx >= 0 {
			// Check if this match is inside a quoted region
			if !quoted[idx] {
				if minIdx < 0 || idx < minIdx {
					minIdx = idx
				}
				break
			}
			nextSearch := idx + len(c)
			if nextSearch >= len(lower) {
				break
			}
			idx = strings.Index(lower[nextSearch:], c)
			if idx >= 0 {
				idx += nextSearch
			}
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

// sameTypeKind returns a category for type-safe comparison.
// Returns "nil", "bool", "number", or "string".
func sameTypeKind(a, b interface{}) string {
	ka := valueKind(a)
	kb := valueKind(b)
	if ka == kb {
		return ka
	}
	return ""
}

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

// compareEqual compares two values for equality without fmt.Sprintf allocation.
// Uses direct type assertion first, falls back to string comparison only for mixed types.
func compareEqual(a, b interface{}) bool {
	switch va := a.(type) {
	case float64:
		if vb, ok := b.(float64); ok {
			return va == vb
		}
	case string:
		if vb, ok := b.(string); ok {
			return va == vb
		}
	case bool:
		if vb, ok := b.(bool); ok {
			return va == vb
		}
	case int:
		if vb, ok := b.(int); ok {
			return va == vb
		}
	case int64:
		if vb, ok := b.(int64); ok {
			return va == vb
		}
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// compareAtomValue compares atom values using the given operator.
// Uses type-aware fast paths to avoid fmt.Sprintf allocations on hot path.
func compareAtomValue(atomVal interface{}, op string, condVal interface{}) bool {
	switch op {
	case "==":
		if atomVal == nil && condVal == nil {
			return true
		}
		if atomVal == nil || condVal == nil {
			return false
		}
		return compareEqual(atomVal, condVal)
	case "!=":
		if atomVal == nil && condVal == nil {
			return false
		}
		if atomVal == nil || condVal == nil {
			return true
		}
		return !compareEqual(atomVal, condVal)
	default:
		af := toFloat(atomVal)
		bf := toFloat(condVal)
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

// toFloat converts a value to float64 without allocation.
// Handles all common numeric types directly via type switch.
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
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f
		}
		return 0
	default:
		if f, err := strconv.ParseFloat(fmt.Sprintf("%v", v), 64); err == nil {
			return f
		}
		return 0
	}
}

// SortResults sorts query results by a field.
// Nil values are always sorted to the end regardless of sort direction.
func SortResults(results []map[string]interface{}, orderBy *OrderBy) {
	if orderBy == nil {
		return
	}
	sort.Slice(results, func(i, j int) bool {
		vi := results[i][orderBy.Field]
		vj := results[j][orderBy.Field]

		// Nil values always go to the end
		if vi == nil && vj == nil {
			return false
		}
		if vi == nil {
			return false
		}
		if vj == nil {
			return true
		}

		fi := toFloat(vi)
		fj := toFloat(vj)
		if orderBy.Desc {
			return fi > fj
		}
		return fi < fj
	})
}

// NULLGroupKey is the group key for rows where the GROUP BY field is nil.
const NULLGroupKey = "\x00__NULL__\x00"

// Aggregate computes an aggregate function over result rows.
func Aggregate(rows []map[string]interface{}, fn string, field string) (interface{}, error) {
	if len(rows) == 0 {
		switch fn {
		case "count":
			return 0, nil
		default:
			return nil, nil
		}
	}

	switch fn {
	case "count":
		return float64(len(rows)), nil

	case "sum":
		var total float64
		for _, row := range rows {
			if v, ok := row[field]; ok && v != nil {
				if !isNumericValue(v) {
					return nil, fmt.Errorf("sum: field %q contains non-numeric value %v", field, v)
				}
				total += toFloat(v)
			}
		}
		return total, nil

	case "avg":
		var total float64
		count := 0
		for _, row := range rows {
			if v, ok := row[field]; ok && v != nil {
				if !isNumericValue(v) {
					return nil, fmt.Errorf("avg: field %q contains non-numeric value %v", field, v)
				}
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
				if !isNumericValue(v) {
					return nil, fmt.Errorf("min: field %q contains non-numeric value %v", field, v)
				}
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
				if !isNumericValue(v) {
					return nil, fmt.Errorf("max: field %q contains non-numeric value %v", field, v)
				}
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

func isNumericValue(v interface{}) bool {
	switch v.(type) {
	case float64, float32, int, int64:
		return true
	case string:
		_, err := strconv.ParseFloat(v.(string), 64)
		return err == nil
	default:
		return false
	}
}

// groupKey converts a value to a string key for grouping without fmt.Sprintf allocation.
// Uses type assertion for common types, falls back to fmt.Sprintf only for unknown types.
func groupKey(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		// Use strconv directly for float64 to avoid allocation
		return strconv.FormatFloat(val, 'f', -1, 64)
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// GroupByResults groups rows by a field value.
// Rows with a nil group field are grouped under NULLGroupKey.
// Uses type-aware key conversion to avoid fmt.Sprintf allocation.
func GroupByResults(rows []map[string]interface{}, groupField string) map[string][]map[string]interface{} {
	groups := make(map[string][]map[string]interface{})
	for _, row := range rows {
		val, exists := row[groupField]
		var key string
		if !exists || val == nil {
			key = NULLGroupKey
		} else {
			key = groupKey(val)
		}
		groups[key] = append(groups[key], row)
	}
	return groups
}
