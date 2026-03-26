package cli

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/tita-n/atomdb/internal/atom"
	"github.com/tita-n/atomdb/internal/query"
	"github.com/tita-n/atomdb/internal/schema"
	"github.com/tita-n/atomdb/internal/store"
)

// DB holds the store and schema for CLI operations.
type DB struct {
	Store          *store.AtomStore
	Schema         *schema.Schema
	DataDir        string
	SchemaEnforced bool
	QueryStats     *QueryStats
}

// QueryStats tracks query execution metrics.
type QueryStats struct {
	mu          sync.Mutex
	Total       int64
	IndexHits   int64
	FullScans   int64
	TotalTimeMs float64
}

func (qs *QueryStats) Record(useIndex bool, durationMs float64) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.Total++
	if useIndex {
		qs.IndexHits++
	} else {
		qs.FullScans++
	}
	qs.TotalTimeMs += durationMs
}

func NewDB(s *store.AtomStore, sc *schema.Schema, dataDir string) *DB {
	sc.Migrations().SetPath(dataDir + "/schema.json")
	return &DB{Store: s, Schema: sc, DataDir: dataDir, QueryStats: &QueryStats{}}
}

// persistSchema saves the schema to a JSON file in the data directory.
func (db *DB) persistSchema() {
	if db.DataDir == "" {
		return
	}
	db.Schema.SaveToFile(db.DataDir + "/schema.json")
}

func Run(db *DB, args []string) error {
	if len(args) == 0 {
		printHelp()
		return nil
	}

	// Shell may pass "create index on type (field)" as single arg.
	// Split first arg on spaces if it contains a space.
	if len(args) == 1 && strings.Contains(args[0], " ") {
		args = strings.Fields(args[0])
	}

	cmd := strings.ToLower(args[0])

	switch cmd {
	case "type":
		return cmdType(db, args[1:])
	case "insert":
		return cmdInsert(db, args[1:])
	case "update":
		return cmdUpdate(db, args[1:])
	case "delete":
		return cmdDelete(db, args[1:])
	case "types":
		return cmdTypes(db)
	case "create":
		return cmdCreate(db, args[1:])
	case "drop":
		return cmdDrop(db, args[1:])
	case "indexes":
		return cmdIndexes(db)
	case "set":
		return cmdSet(db.Store, args[1:])
	case "get":
		return cmdGet(db.Store, args[1:])
	case "getall":
		return cmdGetAll(db.Store, args[1:])
	case "query":
		return cmdQuery(db.Store, args[1:])
	case "explain":
		return cmdExplainNew(db, args[1:])
	case "search":
		return cmdSearch(db.Store, args[1:])
	case "index":
		return cmdIndex(db.Store, args[1:])
	case "stats":
		return cmdStatsNew(db)
	case "compact":
		return cmdCompact(db.Store)
	case "help":
		printHelp()
		return nil
	default:
		// Try as shorthand query: "type where condition" or aggregation
		return tryShorthandQuery(db, args)
	}
}

// cmdType handles: TYPE name { fields }
func cmdType(db *DB, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: type name { field: type, ... }")
	}

	// Rejoin args and handle shell splitting of braces
	input := "TYPE " + strings.Join(args, " ")

	// Fix: shell may split "{name:" into separate args
	// Rejoin to ensure braces are together
	input = strings.ReplaceAll(input, "{ ", "{")
	input = strings.ReplaceAll(input, " }", "}")

	name, fields, err := schema.ParseTypeDefinition(input)
	if err != nil {
		return err
	}

	if err := db.Schema.DefineType(name, fields); err != nil {
		return err
	}

	// Persist schema to the data directory
	db.persistSchema()

	fmt.Printf("Type %q defined with %d fields\n", name, len(fields))
	return nil
}

// cmdInsert handles: INSERT type field:value field:value
func cmdInsert(db *DB, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: insert <type> field:value [field:value ...]")
	}

	typeName := args[0]
	fields := make(map[string]interface{})

	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("expected field:value, got %q", arg)
		}
		fields[parts[0]] = parseRawVal(parts[1])
	}

	// Validate against schema if type is defined
	validated, err := db.Schema.Validate(typeName, fields)
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Validate ref fields point to existing entities
	if err := db.Schema.ValidateRefs(typeName, validated, func(entityID string) bool {
		return db.Store.Exists(entityID, "__type") || db.Store.Exists(entityID, "name") || db.Store.Exists(entityID, "id")
	}); err != nil {
		return fmt.Errorf("ref validation failed: %w", err)
	}

	// Generate entity ID
	entity := fmt.Sprintf("%s:%s", typeName, generateSecureID())

	// Check if we need a better ID (use a name-like field if present)
	for _, idField := range []string{"name", "id", "email", "title"} {
		if v, ok := validated[idField]; ok {
			safeID := sanitizeEntityIDValue(v)
			entity = fmt.Sprintf("%s:%s", typeName, safeID)
			break
		}
	}

	// Atomically insert with duplicate detection (race-safe)
	if err := db.Store.InsertIfNotExists(entity, validated, inferType); err != nil {
		return err
	}

	fmt.Printf("Inserted: %s (%d fields)\n", entity, len(validated))
	return nil
}

// cmdUpdate handles: UPDATE type where condition set field = value
func cmdUpdate(db *DB, args []string) error {
	input := "UPDATE " + strings.Join(args, " ")
	parsed, err := query.Parse(input)
	if err != nil {
		return err
	}

	if parsed.Command != "UPDATE" {
		return fmt.Errorf("expected UPDATE command")
	}

	op := parsed.Update
	results := db.queryEntities(op.TypeName, op.Conditions)

	if len(results) == 0 {
		fmt.Println("No matching records found.")
		return nil
	}

	// Validate ref fields point to existing entities
	if err := db.Schema.ValidateRefs(op.TypeName, op.SetFields, func(entityID string) bool {
		return db.Store.Exists(entityID, "__type") || db.Store.Exists(entityID, "name") || db.Store.Exists(entityID, "id")
	}); err != nil {
		return fmt.Errorf("ref validation failed: %w", err)
	}

	for _, entity := range results {
		for attr, val := range op.SetFields {
			valType := inferType(val)
			if err := db.Store.Set(entity, attr, val, valType); err != nil {
				return fmt.Errorf("failed to update %s.%s: %w", entity, attr, err)
			}
		}
	}

	fmt.Printf("Updated %d record(s)\n", len(results))
	return nil
}

// cmdDelete handles: DELETE type where condition
func cmdDelete(db *DB, args []string) error {
	input := "DELETE " + strings.Join(args, " ")
	parsed, err := query.Parse(input)
	if err != nil {
		return err
	}

	if parsed.Command != "DELETE" {
		return fmt.Errorf("expected DELETE command")
	}

	op := parsed.Delete
	results := db.queryEntities(op.TypeName, op.Conditions)

	if len(results) == 0 {
		fmt.Println("No matching records found.")
		return nil
	}

	for _, entity := range results {
		attrs := db.Store.GetAll(entity)
		for attr := range attrs {
			db.Store.Delete(entity, attr)
		}
	}

	fmt.Printf("Deleted %d record(s)\n", len(results))
	return nil
}

// cmdTypes lists all defined types.
func cmdTypes(db *DB) error {
	names := db.Schema.ListTypes()
	if len(names) == 0 {
		fmt.Println("No types defined. Use: type name { field: type }")
		return nil
	}

	fmt.Println("Types:")
	for _, name := range names {
		td, _ := db.Schema.GetType(name)
		fmt.Printf("  %s (%d fields)\n", name, len(td.Fields))
		for _, f := range td.Fields {
			opt := ""
			if f.Optional {
				opt = "?"
			}
			def := ""
			if f.Default != nil {
				def = fmt.Sprintf(" = %v", f.Default)
			}
			fmt.Printf("    %s: %s%s%s\n", f.Name, f.Type, opt, def)
		}
	}
	return nil
}

// tryShorthandQuery tries to parse args as a shorthand query.
func tryShorthandQuery(db *DB, args []string) error {
	input := strings.Join(args, " ")
	parsed, err := query.Parse(input)
	if err != nil || parsed.Command != "SELECT" {
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[0])
		printHelp()
		return fmt.Errorf("unknown command: %s", args[0])
	}

	return executeQuery(db, parsed.Query)
}

// executeQuery runs a query and prints results. Handles aggregations, GROUP BY, ORDER BY, LIMIT.
func executeQuery(db *DB, q *query.Query) error {
	start := time.Now()

	// Execute query to get matching entities
	entities := db.queryEntities(q.TypeName, q.Conditions)
	useIndex := hasUsableIndex(db, q)

	// Build result rows
	var rows []map[string]interface{}
	for _, entity := range entities {
		attrs := db.Store.GetAll(entity)
		row := make(map[string]interface{})
		row["_entity"] = entity

		shouldLoadAll := q.Aggregate != "" || q.GroupBy != "" || len(q.Fields) == 0
		if shouldLoadAll {
			// Aggregation/group by: need all fields for grouping and aggregation
			for attr, a := range attrs {
				row[attr] = a.Value
			}
		} else {
			// Specific fields requested
			for _, f := range q.Fields {
				attrName := f
				if dotIdx := strings.Index(f, "."); dotIdx >= 0 {
					attrName = f[dotIdx+1:]
				}
				if a, ok := attrs[attrName]; ok {
					row[attrName] = a.Value
				}
			}
		}
		rows = append(rows, row)
	}

	// Handle GROUP BY aggregations
	if q.GroupBy != "" {
		return executeGroupBy(db, q, rows, useIndex, start)
	}

	// Handle simple aggregation (no GROUP BY)
	if q.Aggregate != "" {
		return executeAggregation(db, q, rows, useIndex, start)
	}

	// Sort
	query.SortResults(rows, q.OrderBy)

	// Limit
	if q.Limit > 0 && q.Limit < len(rows) {
		rows = rows[:q.Limit]
	}

	// Print results
	printRows(rows, q.Fields)

	// Track stats
	elapsed := float64(time.Since(start).Microseconds()) / 1000.0
	db.QueryStats.Record(useIndex, elapsed)

	return nil
}

// hasUsableIndex checks if the query can use a B-Tree index.
func hasUsableIndex(db *DB, q *query.Query) bool {
	for _, cond := range q.Conditions {
		if db.Store.HasIndex(cond.Field) {
			return true
		}
	}
	return false
}

// executeAggregation runs a simple aggregation and prints the result.
func executeAggregation(db *DB, q *query.Query, rows []map[string]interface{}, useIndex bool, start time.Time) error {
	result, err := query.Aggregate(rows, q.Aggregate, q.AggField)
	if err != nil {
		return err
	}

	fmt.Printf("%s = %v\n", formatAggLabel(q.Aggregate, q.AggField, q.TypeName), result)

	elapsed := float64(time.Since(start).Microseconds()) / 1000.0
	db.QueryStats.Record(useIndex, elapsed)
	return nil
}

// executeGroupBy runs GROUP BY with aggregation.
func executeGroupBy(db *DB, q *query.Query, rows []map[string]interface{}, useIndex bool, start time.Time) error {
	if len(rows) == 0 {
		fmt.Println("No results to group.")
		return nil
	}

	groups := query.GroupByResults(rows, q.GroupBy)

	// Sort group keys numerically if all keys are numeric, else lexicographically
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sortGroupKeys(keys)

	for _, key := range keys {
		groupRows := groups[key]
		displayKey := key
		if key == query.NULLGroupKey {
			displayKey = "NULL"
		}
		if q.Aggregate != "" {
			aggResult, err := query.Aggregate(groupRows, q.Aggregate, q.AggField)
			if err != nil {
				fmt.Printf("%s = %v (0 rows)\n", displayKey, 0)
			} else {
				fmt.Printf("%s = %v (%d rows)\n", displayKey, aggResult, len(groupRows))
			}
		} else {
			fmt.Printf("%s (%d rows)\n", displayKey, len(groupRows))
		}
	}

	elapsed := float64(time.Since(start).Microseconds()) / 1000.0
	db.QueryStats.Record(useIndex, elapsed)
	return nil
}

// sortGroupKeys sorts group keys numerically if all are numeric, otherwise lexicographically.
// NULLGroupKey is always placed last.
func sortGroupKeys(keys []string) {
	allNumeric := true
	for _, k := range keys {
		if k == query.NULLGroupKey {
			continue
		}
		if _, err := strconv.ParseFloat(k, 64); err != nil {
			allNumeric = false
			break
		}
	}

	if allNumeric {
		sort.Slice(keys, func(i, j int) bool {
			if keys[i] == query.NULLGroupKey {
				return false
			}
			if keys[j] == query.NULLGroupKey {
				return true
			}
			fi, _ := strconv.ParseFloat(keys[i], 64)
			fj, _ := strconv.ParseFloat(keys[j], 64)
			return fi < fj
		})
	} else {
		sort.Strings(keys)
	}
}

// formatAggLabel creates a readable label for aggregation results.
func formatAggLabel(fn, field, typeName string) string {
	if field == "" {
		return fmt.Sprintf("%s(%s)", fn, typeName)
	}
	return fmt.Sprintf("%s(%s.%s)", fn, typeName, field)
}

// printRows outputs query results in a readable format.
func printRows(rows []map[string]interface{}, fields []string) {
	if len(rows) == 0 {
		fmt.Println("No results found.")
		return
	}

	for _, row := range rows {
		var parts []string
		if len(fields) == 0 {
			keys := make([]string, 0, len(row))
			for k := range row {
				if k != "_entity" {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			for _, k := range keys {
				parts = append(parts, fmt.Sprintf("%s=%v", k, row[k]))
			}
			fmt.Printf("%s: %s\n", row["_entity"], strings.Join(parts, " "))
		} else {
			for _, f := range fields {
				if v, ok := row[f]; ok {
					parts = append(parts, fmt.Sprintf("%s=%v", f, v))
				}
			}
			fmt.Printf("%s.%s\n", row["_entity"], strings.Join(parts, " "))
		}
	}
	fmt.Printf("(%d results)\n", len(rows))
}

// queryEntities finds all entities of a type matching conditions.
// Delegates to Store.QueryEntities which uses B-Tree indexes when available.
func (db *DB) queryEntities(typeName string, conditions []query.Condition) []string {
	// Convert query.Condition to store.Condition
	storeConds := make([]store.Condition, len(conditions))
	for i, c := range conditions {
		storeConds[i] = store.Condition{
			Field:    c.Field,
			Operator: c.Operator,
			Value:    c.Value,
		}
	}
	return db.Store.QueryEntities(typeName, storeConds)
}

// --- Raw commands (backward compatibility) ---

func cmdSet(s *store.AtomStore, args []string) error {
	if len(args) < 4 {
		return fmt.Errorf("usage: set <entity> <attribute> <value> <type>")
	}
	entity, attribute, rawValue, valueType := args[0], args[1], args[2], args[3]

	// Validate entity and attribute names
	if err := validateName(entity); err != nil {
		return fmt.Errorf("invalid entity name: %w", err)
	}
	if err := validateName(attribute); err != nil {
		return fmt.Errorf("invalid attribute name: %w", err)
	}
	// Validate value type
	if err := validateValueType(valueType); err != nil {
		return err
	}

	value, err := parseRawValue(rawValue, valueType)
	if err != nil {
		return err
	}
	fmt.Printf("WARNING: raw 'set' command bypasses schema validation\n")
	fmt.Printf("Stored: %s.%s = %v\n", entity, attribute, value)
	if err := s.Set(entity, attribute, value, valueType); err != nil {
		return err
	}
	return nil
}

func cmdGet(s *store.AtomStore, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: get <entity> <attribute>")
	}
	a, ok := s.Get(args[0], args[1])
	if !ok {
		fmt.Printf("Not found: %s.%s\n", args[0], args[1])
		return nil
	}
	fmt.Printf("Entity: %s\nAttribute: %s\nValue: %v\nType: %s\n", a.Entity, a.Attribute, a.Value, a.Type)
	return nil
}

func cmdGetAll(s *store.AtomStore, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: getall <entity>")
	}
	attrs := s.GetAll(args[0])
	if len(attrs) == 0 {
		fmt.Printf("No attributes found for: %s\n", args[0])
		return nil
	}
	names := make([]string, 0, len(attrs))
	for n := range attrs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Printf("%s.%s = %v (%s)\n", args[0], n, attrs[n].Value, attrs[n].Type)
	}
	return nil
}

func cmdQuery(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: query <attribute> <op> <value>")
	}
	attr, op, val := args[0], args[1], args[2]
	results := s.Query(attr, func(a *atom.Atom) bool {
		af := toFloatVal(a.Value)
		bf, _ := strconv.ParseFloat(val, 64)
		return compareVals(af, bf, op)
	})
	if len(results) == 0 {
		fmt.Println("No results found.")
		return nil
	}
	for _, a := range results {
		fmt.Printf("%s.%s = %v\n", a.Entity, a.Attribute, a.Value)
	}
	return nil
}

func cmdExplainNew(db *DB, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: explain <query>")
	}

	input := strings.Join(args, " ")
	parsed, err := query.Parse(input)
	if err != nil || parsed.Command != "SELECT" {
		// Fall back to raw explain: explain attr op value
		if len(args) >= 3 {
			plan := db.Store.QueryExplain(args[0], args[1], args[2])
			printPlan(plan)
			return nil
		}
		return fmt.Errorf("usage: explain <type> where <condition>")
	}

	q := parsed.Query
	fmt.Printf("Query: %s\n", input)
	fmt.Println()

	// Show condition plans
	for _, cond := range q.Conditions {
		plan := db.Store.QueryExplain(cond.Field, cond.Operator, fmt.Sprintf("%v", cond.Value))
		printPlan(plan)
	}

	if len(q.Conditions) == 0 {
		fmt.Printf("Full scan: %s (no filter)\n", q.TypeName)
	}

	if q.OrderBy != nil {
		fmt.Printf("Sort: %s (", q.OrderBy.Field)
		if q.OrderBy.Desc {
			fmt.Print("desc")
		} else {
			fmt.Print("asc")
		}
		fmt.Println(")")
	}

	if q.Limit > 0 {
		fmt.Printf("Limit: %d\n", q.Limit)
	}

	if q.Aggregate != "" {
		fmt.Printf("Aggregate: %s(%s)\n", q.Aggregate, q.AggField)
	}

	return nil
}

func printPlan(plan store.QueryPlan) {
	if plan.UseIndex {
		cost := "very low"
		if plan.ScanType == "range" {
			cost = "low"
		}
		fmt.Printf("Index scan: %s (%s) - cost: %s\n", plan.Attribute, plan.ScanType, cost)
		fmt.Printf("Estimated rows: %d\n", plan.EstimatedRows)
	} else {
		fmt.Printf("Full scan: no index on %s - cost: high\n", plan.Attribute)
		fmt.Printf("Estimated rows: %d\n", plan.EstimatedRows)
		fmt.Printf("Recommend: CREATE INDEX ON %s (%s)\n", plan.Attribute, plan.Attribute)
	}
}

func cmdSearch(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: search <attribute> contains <word>")
	}
	results := s.FullTextSearch(args[0], args[2])
	for _, a := range results {
		fmt.Printf("%s.%s = %v\n", a.Entity, a.Attribute, a.Value)
	}
	return nil
}

func cmdIndex(s *store.AtomStore, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: index <list|rebuild>")
	}
	switch args[0] {
	case "list":
		stats := s.Stats()
		for _, a := range stats.IndexedAttrs {
			fmt.Printf("  %s\n", a)
		}
	case "rebuild":
		s.RebuildIndexes()
		fmt.Println("Indexes rebuilt.")
	}
	return nil
}

func cmdStatsNew(db *DB) error {
	stats := db.Store.Stats()
	qs := db.QueryStats
	qs.mu.Lock()
	total := qs.Total
	indexHits := qs.IndexHits
	fullScans := qs.FullScans
	avgTime := 0.0
	if total > 0 {
		avgTime = qs.TotalTimeMs / float64(total)
	}
	qs.mu.Unlock()

	fmt.Printf("Storage:\n")
	fmt.Printf("  Entities: %d\n", stats.EntityCount)
	fmt.Printf("  Atoms: %d\n", stats.AtomCount)
	fmt.Printf("  Indexes: %s\n", strings.Join(stats.IndexedAttrs, ", "))
	fmt.Printf("  Index keys: %d\n", stats.IndexKeyCount)
	fmt.Println()
	fmt.Printf("Query stats:\n")
	fmt.Printf("  Queries executed: %d\n", total)
	if total > 0 {
		indexPct := float64(indexHits) / float64(total) * 100
		scanPct := float64(fullScans) / float64(total) * 100
		fmt.Printf("  Index hits: %d (%.0f%%)\n", indexHits, indexPct)
		fmt.Printf("  Full scans: %d (%.0f%%)\n", fullScans, scanPct)
		fmt.Printf("  Average query time: %.2fms\n", avgTime)
	}
	return nil
}

func cmdCompact(s *store.AtomStore) error {
	if err := s.Compact(); err != nil {
		return err
	}
	fmt.Println("Compaction complete.")
	return nil
}

// cmdCreate handles: CREATE INDEX ON type (field)
func cmdCreate(db *DB, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: create index on <type> (<field>)")
	}

	input := strings.ToUpper(strings.Join(args, " "))
	if !strings.Contains(input, "INDEX") {
		return fmt.Errorf("usage: create index on <type> (<field>)")
	}

	// Parse "index on type (field)" or "index on type(field)"
	rest := strings.TrimSpace(strings.Join(args, " "))
	rest = strings.TrimPrefix(strings.ToLower(rest), "index ")
	rest = strings.TrimPrefix(rest, "on ")
	rest = strings.TrimSpace(rest)

	// Extract type and field
	parenIdx := strings.Index(rest, "(")
	if parenIdx < 0 {
		// Try "index on type field"
		parts := strings.Fields(rest)
		if len(parts) < 2 {
			return fmt.Errorf("usage: create index on <type> (<field>)")
		}
		typeName := parts[0]
		fieldName := parts[1]
		if err := db.Store.CreateIndex(typeName, fieldName); err != nil {
			return err
		}
		fmt.Printf("Index created: %s.%s\n", typeName, fieldName)
		return nil
	}

	typeName := strings.TrimSpace(rest[:parenIdx])
	fieldPart := rest[parenIdx+1:]
	fieldPart = strings.TrimSuffix(fieldPart, ")")
	fieldName := strings.TrimSpace(fieldPart)

	if err := db.Store.CreateIndex(typeName, fieldName); err != nil {
		return err
	}
	fmt.Printf("Index created: %s.%s\n", typeName, fieldName)
	return nil
}

// cmdDrop handles: DROP INDEX name or DROP INDEX type (field)
func cmdDrop(db *DB, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: drop index <field>")
	}

	input := strings.Join(args, " ")
	// Strip "index" prefix if present
	input = strings.TrimPrefix(strings.ToLower(input), "index ")
	input = strings.TrimSpace(input)

	db.Store.DropIndex(input)
	fmt.Printf("Index dropped: %s\n", input)
	return nil
}

// cmdIndexes lists all indexes.
func cmdIndexes(db *DB) error {
	stats := db.Store.Stats()
	if len(stats.IndexedAttrs) == 0 {
		fmt.Println("No indexes. Use: create index on <type> (<field>)")
		return nil
	}
	fmt.Println("Indexes:")
	for _, attr := range stats.IndexedAttrs {
		fmt.Printf("  %s\n", attr)
	}
	fmt.Printf("(%d indexes, %d keys total)\n", len(stats.IndexedAttrs), stats.IndexKeyCount)
	return nil
}

func parseRawValue(raw, valueType string) (interface{}, error) {
	switch strings.ToLower(valueType) {
	case "string":
		return raw, nil
	case "number":
		return strconv.ParseFloat(raw, 64)
	case "boolean", "bool":
		return strconv.ParseBool(raw)
	default:
		return raw, nil
	}
}

func parseRawVal(raw string) interface{} {
	if raw == "true" {
		return true
	}
	if raw == "false" {
		return false
	}
	if raw == "nil" || raw == "null" {
		return nil
	}
	// Try number
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}
	return raw
}

// validateName checks that a name (entity/attribute) contains only safe characters.
func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	if len(name) > 1024 {
		return fmt.Errorf("name too long (max 1024 bytes)")
	}
	for i, r := range name {
		if unicode.IsControl(r) && r != '\t' {
			return fmt.Errorf("control character at position %d", i)
		}
		if r == '\u2028' || r == '\u2029' {
			return fmt.Errorf("Unicode line separator at position %d", i)
		}
		if r == '/' || r == '\\' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
			return fmt.Errorf("unsafe character %q at position %d", r, i)
		}
	}
	return nil
}

// validateValueType checks that the value type is known.
func validateValueType(t string) error {
	switch strings.ToLower(t) {
	case "string", "number", "boolean", "bool", "ref", "timestamp", "deleted":
		return nil
	default:
		return fmt.Errorf("unknown value type %q", t)
	}
}

func toFloatVal(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		f, _ := strconv.ParseFloat(fmt.Sprintf("%v", v), 64)
		return f
	}
}

func compareVals(a, b float64, op string) bool {
	switch op {
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	case "==":
		return a == b
	case "!=":
		return a != b
	}
	return false
}

func inferType(v interface{}) string {
	switch v.(type) {
	case float64, float32, int, int64:
		return "number"
	case bool:
		return "boolean"
	default:
		return "string"
	}
}

func sanitizeEntityIDValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		var safe strings.Builder
		safe.Grow(len(val))
		for _, r := range val {
			if unicode.IsControl(r) && r != '\t' {
				safe.WriteRune('_')
				continue
			}
			if r == '\u2028' || r == '\u2029' {
				safe.WriteRune('_')
				continue
			}
			if r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
				safe.WriteRune('_')
				continue
			}
			safe.WriteRune(r)
		}
		result := safe.String()
		if len(result) > 256 {
			result = result[:256]
		}
		if result == "" {
			return generateSecureID()
		}
		return result
	case float64:
		return fmt.Sprintf("n%d", int(val))
	case int:
		return fmt.Sprintf("n%d", val)
	case int64:
		return fmt.Sprintf("n%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return generateSecureID()
	}
}

// generateSecureID creates a cryptographically random 8-byte hex ID.
func generateSecureID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("x%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func printHelp() {
	fmt.Println("AtomDB - Modern database with clean syntax")
	fmt.Println()
	fmt.Println("Types:")
	fmt.Println("  type name { field: type, field: type? }    Define a type")
	fmt.Println("  types                                       List all types")
	fmt.Println()
	fmt.Println("CRUD:")
	fmt.Println("  insert type field:value [field:value ...]   Insert a record")
	fmt.Println("  type where condition                        Query records")
	fmt.Println("  update type where cond set field = value    Update records")
	fmt.Println("  delete type where condition                 Delete records")
	fmt.Println()
	fmt.Println("Queries:")
	fmt.Println("  person where age > 25                       Filter by field")
	fmt.Println("  person.name where city == Lagos             Select specific fields")
	fmt.Println("  person order by name limit 10               Sort and limit")
	fmt.Println("  person where age > 25 and city == Lagos     Compound condition")
	fmt.Println()
	fmt.Println("Aggregations:")
	fmt.Println("  count(person) where age > 25                Count records")
	fmt.Println("  sum(expense.amount) where category == food  Sum field values")
	fmt.Println("  avg(person.age) where city == Lagos         Average")
	fmt.Println("  min(task.priority)                          Minimum value")
	fmt.Println("  max(expense.amount)                         Maximum value")
	fmt.Println()
	fmt.Println("Group by:")
	fmt.Println("  city, count(*) group by city                Group and count")
	fmt.Println("  category, sum(amount) group by category     Group and sum")
	fmt.Println()
	fmt.Println("Indexes:")
	fmt.Println("  create index on person (age)                Create index")
	fmt.Println("  indexes                                     List indexes")
	fmt.Println("  drop index age                              Drop index")
	fmt.Println()
	fmt.Println("Explain:")
	fmt.Println("  explain person where age > 25               Show query plan")
	fmt.Println()
	fmt.Println("Stats:")
	fmt.Println("  stats                                       Show stats")
	fmt.Println("  compact                                     Compact data file")
	fmt.Println()
	fmt.Println("Operators: ==, !=, >, <, >=, <=")
	fmt.Println("Types: string, number, boolean, ref")
	fmt.Println()
	fmt.Println("Raw: set, get, getall, query, search, index")
}

// cmdBackup creates a point-in-time backup.
func cmdBackup(s *store.AtomStore, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: backup <path>")
	}
	path := args[0]
	if err := s.Backup(path); err != nil {
		return err
	}
	fmt.Printf("Backup created: %s\n", path)
	return nil
}

// cmdRestore restores from a backup file.
func cmdRestore(s *store.AtomStore, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: restore <path>")
	}
	path := args[0]
	if err := s.Restore(path); err != nil {
		return err
	}
	fmt.Printf("Restored from: %s\n", path)
	return nil
}

// cmdConstraint adds a constraint.
func cmdConstraint(db *DB, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: constraint add <type> <field> <unique|notnull>")
	}

	action := args[0]
	typeName := args[1]
	fieldName := args[2]

	switch action {
	case "add":
		if len(args) < 4 {
			return fmt.Errorf("usage: constraint add <type> <field> <unique|notnull>")
		}
		constraintType := args[3]
		var ct store.ConstraintType
		switch constraintType {
		case "unique":
			ct = store.ConstraintUnique
		case "notnull":
			ct = store.ConstraintNotNull
		default:
			return fmt.Errorf("unknown constraint type: %q", constraintType)
		}
		db.Store.AddConstraint(store.Constraint{
			Type:      ct,
			TypeName:  typeName,
			FieldName: fieldName,
		})
		fmt.Printf("Added %s constraint on %s.%s\n", constraintType, typeName, fieldName)
	default:
		return fmt.Errorf("unknown action: %q (use 'add')", action)
	}
	return nil
}

// cmdConstraints lists constraints for a type.
func cmdConstraints(db *DB, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: constraints <type>")
	}
	typeName := args[0]
	constraints := db.Store.ListConstraints(typeName)
	if len(constraints) == 0 {
		fmt.Printf("No constraints for type %q\n", typeName)
		return nil
	}
	fmt.Printf("Constraints for %s:\n", typeName)
	for _, c := range constraints {
		var ct string
		switch c.Type {
		case store.ConstraintUnique:
			ct = "UNIQUE"
		case store.ConstraintNotNull:
			ct = "NOT NULL"
		case store.ConstraintCheck:
			ct = "CHECK"
		}
		fmt.Printf("  %s: %s\n", c.FieldName, ct)
	}
	return nil
}

// cmdRelation manages type relations.
func cmdRelation(db *DB, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: relation add <from.field> <to-type> <cardinality>")
	}

	action := args[0]
	switch action {
	case "add":
		if len(args) < 4 {
			return fmt.Errorf("usage: relation add <from.field> <to-type> <cardinality>")
		}
		fromParts := strings.SplitN(args[1], ".", 2)
		if len(fromParts) != 2 {
			return fmt.Errorf("expected from.field, got %q", args[1])
		}
		toType := args[2]
		cardinality := args[3]

		r := schema.Relation{
			FromType:    fromParts[0],
			FromField:   fromParts[1],
			ToType:      toType,
			ToField:     "id",
			Cardinality: cardinality,
		}
		if err := db.Schema.Relations().AddRelation(r); err != nil {
			return err
		}
		fmt.Printf("Added relation: %s.%s -> %s (%s)\n", fromParts[0], fromParts[1], toType, cardinality)

	case "remove":
		if len(args) < 2 {
			return fmt.Errorf("usage: relation remove <from.field>")
		}
		fromParts := strings.SplitN(args[1], ".", 2)
		if len(fromParts) != 2 {
			return fmt.Errorf("expected from.field, got %q", args[1])
		}
		db.Schema.Relations().RemoveRelation(fromParts[0], fromParts[1])
		fmt.Printf("Removed relation: %s.%s\n", fromParts[0], fromParts[1])

	default:
		return fmt.Errorf("unknown action: %q (use 'add' or 'remove')", action)
	}
	return nil
}

// cmdRelations lists relations for a type.
func cmdRelations(db *DB, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: relations <type>")
	}
	typeName := args[0]
	relations := db.Schema.Relations().GetRelations(typeName)
	if len(relations) == 0 {
		fmt.Printf("No relations from type %q\n", typeName)
		return nil
	}
	fmt.Printf("Relations from %s:\n", typeName)
	for _, r := range relations {
		fmt.Printf("  %s.%s -> %s (%s)\n", r.FromType, r.FromField, r.ToType, r.Cardinality)
	}
	return nil
}

// cmdMigrate handles schema migrations.
func cmdMigrate(db *DB, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: migrate <add|remove> <type> <field> [type]")
	}

	action := args[0]
	switch action {
	case "add":
		if len(args) < 4 {
			return fmt.Errorf("usage: migrate add <type> <field> <field-type>")
		}
		typeName := args[1]
		fieldName := args[2]
		fieldType := args[3]

		m := db.Schema.Migrations().AddField(typeName, schema.FieldDef{
			Name:     fieldName,
			Type:     schema.ParseFieldType(fieldType),
			Optional: true,
		})
		if err := db.Schema.ApplyMigration(typeName, m); err != nil {
			return err
		}

		// Backfill existing entities with default value if provided
		backfilled := 0
		for _, change := range m.Changes {
			if change.Type == "add_field" && change.DefaultVal != nil {
				for _, entity := range db.queryEntities(typeName, nil) {
					if !db.Store.Exists(entity, fieldName) {
						valType := inferType(change.DefaultVal)
						if err := db.Store.Set(entity, fieldName, change.DefaultVal, valType); err == nil {
							backfilled++
						}
					}
				}
			}
		}

		db.Schema.Migrations().Record(m)
		db.persistSchema()
		if backfilled > 0 {
			fmt.Printf("Migration applied: %s (backfilled %d entities)\n", m.Name, backfilled)
		} else {
			fmt.Printf("Migration applied: %s\n", m.Name)
		}

	case "remove":
		if len(args) < 3 {
			return fmt.Errorf("usage: migrate remove <type> <field>")
		}
		typeName := args[1]
		fieldName := args[2]

		m := db.Schema.Migrations().RemoveField(typeName, fieldName)
		if err := db.Schema.ApplyMigration(typeName, m); err != nil {
			return err
		}
		db.Schema.Migrations().Record(m)
		db.persistSchema()
		fmt.Printf("Migration applied: %s\n", m.Name)

	default:
		return fmt.Errorf("unknown action: %q (use 'add' or 'remove')", action)
	}
	return nil
}

// cmdMigrations lists applied migrations.
func cmdMigrations(db *DB) error {
	migrations := db.Schema.Migrations().Applied()
	if len(migrations) == 0 {
		fmt.Println("No migrations applied")
		return nil
	}
	fmt.Printf("Applied migrations (current version: %d):\n", db.Schema.Migrations().CurrentVersion())
	for _, m := range migrations {
		fmt.Printf("  v%d: %s (%s)\n", m.Version, m.Name, m.Timestamp.Format("2006-01-02 15:04:05"))
	}
	return nil
}

// cmdJoin performs a join query between two types.
func cmdJoin(db *DB, args []string) error {
	if len(args) < 4 {
		return fmt.Errorf("usage: join <left.field> <right-type> <right-field> [where conditions]")
	}

	leftField := args[0]
	rightType := args[1]
	rightField := args[2]

	// Get left type from field reference
	leftParts := strings.SplitN(leftField, ".", 2)
	if len(leftParts) != 2 {
		return fmt.Errorf("expected left.field, got %q", leftField)
	}
	leftType := leftParts[0]
	leftFieldName := leftParts[1]

	// Fetch left entities
	leftEntities := db.queryEntities(leftType, nil)
	leftRows := make([]map[string]interface{}, 0, len(leftEntities))
	for _, entity := range leftEntities {
		attrs := db.Store.GetAll(entity)
		row := make(map[string]interface{})
		row["_entity"] = entity
		for attr, a := range attrs {
			row[attr] = a.Value
		}
		leftRows = append(leftRows, row)
	}

	// Fetch right entities
	rightEntities := db.queryEntities(rightType, nil)
	rightRows := make([]map[string]interface{}, 0, len(rightEntities))
	for _, entity := range rightEntities {
		attrs := db.Store.GetAll(entity)
		row := make(map[string]interface{})
		row["_entity"] = entity
		for attr, a := range attrs {
			row[attr] = a.Value
		}
		rightRows = append(rightRows, row)
	}

	// Perform join
	results := schema.Join(leftRows, rightRows, leftFieldName, rightField)

	// Print results
	fmt.Printf("Join results (%d rows):\n", len(results))
	for _, row := range results {
		var parts []string
		for k, v := range row {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(parts)
		fmt.Println(strings.Join(parts, " "))
	}
	return nil
}

// cmdTransaction handles transaction control.
func cmdTransaction(db *DB, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: tx <begin|commit|rollback>")
	}

	action := args[0]
	switch action {
	case "begin":
		fmt.Println("Transaction support available via WithTransaction() API")
		fmt.Println("Example: store.WithTransaction(func(tx *store.Transaction) error { ... })")
	case "commit":
		fmt.Println("Use transaction.Commit() in code")
	case "rollback":
		fmt.Println("Use transaction.Rollback() in code")
	default:
		return fmt.Errorf("unknown action: %q (use 'begin', 'commit', 'rollback')", action)
	}
	return nil
}
