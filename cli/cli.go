package cli

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/tita-n/atomdb/internal/atom"
	"github.com/tita-n/atomdb/internal/index"
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
}

func NewDB(s *store.AtomStore, sc *schema.Schema, dataDir string) *DB {
	return &DB{Store: s, Schema: sc, DataDir: dataDir}
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

	cmd := strings.ToLower(args[0])

	switch cmd {
	case "type":
		return cmdType(db, args[1:])
	case "insert":
		return cmdInsert(db, args[1:])
	case "update":
		return cmdUpdate(db, args[1:])
	case "delete":
		return cmdDeleteCmd(db, args[1:])
	case "types":
		return cmdTypes(db)
	case "set":
		return cmdSet(db.Store, args[1:])
	case "get":
		return cmdGet(db.Store, args[1:])
	case "getall":
		return cmdGetAll(db.Store, args[1:])
	case "query":
		return cmdQuery(db.Store, args[1:])
	case "explain":
		return cmdExplain(db.Store, args[1:])
	case "search":
		return cmdSearch(db.Store, args[1:])
	case "index":
		return cmdIndex(db.Store, args[1:])
	case "stats":
		return cmdStats(db.Store)
	case "compact":
		return cmdCompact(db.Store)
	case "backup":
		return cmdBackup(db.Store, args[1:])
	case "restore":
		return cmdRestore(db.Store, args[1:])
	case "constraint":
		return cmdConstraint(db, args[1:])
	case "constraints":
		return cmdConstraints(db, args[1:])
	case "relation":
		return cmdRelation(db, args[1:])
	case "relations":
		return cmdRelations(db, args[1:])
	case "migrate":
		return cmdMigrate(db, args[1:])
	case "migrations":
		return cmdMigrations(db)
	case "join":
		return cmdJoin(db, args[1:])
	case "tx":
		return cmdTransaction(db, args[1:])
	case "help":
		printHelp()
		return nil
	default:
		// Try as shorthand query: "type where condition"
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

	// Generate entity ID
	entity := fmt.Sprintf("%s:%d", typeName, len(validated))

	// Check if we need a better ID (use a name-like field if present)
	for _, idField := range []string{"name", "id", "email", "title"} {
		if v, ok := validated[idField]; ok {
			safeID := sanitizeEntityIDValue(v)
			entity = fmt.Sprintf("%s:%s", typeName, safeID)
			break
		}
	}

	// Store each field as an atom
	for attr, val := range validated {
		valType := inferType(val)
		if err := db.Store.Set(entity, attr, val, valType); err != nil {
			return fmt.Errorf("failed to store %s.%s: %w", entity, attr, err)
		}
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

// cmdDeleteCmd handles: DELETE type where condition
func cmdDeleteCmd(db *DB, args []string) error {
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
// e.g., "person where age > 25" or "person.name where city == Lagos"
func tryShorthandQuery(db *DB, args []string) error {
	input := strings.Join(args, " ")
	parsed, err := query.Parse(input)
	if err != nil || parsed.Command != "SELECT" {
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[0])
		printHelp()
		return fmt.Errorf("unknown command: %s", args[0])
	}

	return executeSelect(db, parsed.Query)
}

// executeSelect runs a SELECT query and prints results.
func executeSelect(db *DB, q *query.Query) error {
	results := db.queryEntities(q.TypeName, q.Conditions)

	if len(results) == 0 {
		fmt.Println("No results found.")
		return nil
	}

	// Build result rows
	var rows []map[string]interface{}
	for _, entity := range results {
		attrs := db.Store.GetAll(entity)
		row := make(map[string]interface{})
		row["_entity"] = entity

		if len(q.Fields) == 0 {
			// Return all fields
			for attr, a := range attrs {
				row[attr] = a.Value
			}
		} else {
			// Return specified fields
			for _, f := range q.Fields {
				if a, ok := attrs[f]; ok {
					row[f] = a.Value
				}
			}
		}
		rows = append(rows, row)
	}

	// Sort
	query.SortResults(rows, q.OrderBy)

	// Limit
	if q.Limit > 0 && q.Limit < len(rows) {
		rows = rows[:q.Limit]
	}

	// Print
	for _, row := range rows {
		// Collect fields (skip _entity for display unless no specific fields)
		var parts []string
		if len(q.Fields) == 0 {
			// Show entity + all fields
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
			// Show specified fields
			for _, f := range q.Fields {
				if v, ok := row[f]; ok {
					parts = append(parts, fmt.Sprintf("%s=%v", f, v))
				}
			}
			fmt.Printf("%s.%s\n", row["_entity"], strings.Join(parts, " "))
		}
	}

	fmt.Printf("(%d results)\n", len(rows))
	return nil
}

// queryEntities finds all entities of a type matching conditions.
// Uses indexes when available, falls back to full scan otherwise.
func (db *DB) queryEntities(typeName string, conditions []query.Condition) []string {
	prefix := typeName + ":"

	// Try to use index for the first equality condition on an indexed field
	candidateEntities := db.resolveCandidatesViaIndex(prefix, conditions)

	var results []string
	seen := make(map[string]bool)

	// If we got candidates from index, filter them further
	if candidateEntities != nil {
		for _, entity := range candidateEntities {
			if !strings.HasPrefix(entity, prefix) {
				continue
			}
			if seen[entity] {
				continue
			}
			attrs := db.Store.GetAll(entity)
			if attrs == nil {
				continue
			}
			// Validate types at query time
			if err := db.validateQueryAttrs(typeName, attrs); err != nil {
				continue
			}
			if query.MatchConditions(attrs, conditions) {
				seen[entity] = true
				results = append(results, entity)
			}
		}
		return results
	}

	// Fall back to full scan
	db.Store.Query("", func(a *atom.Atom) bool {
		if !strings.HasPrefix(a.Entity, prefix) {
			return false
		}
		if seen[a.Entity] {
			return false
		}

		attrs := db.Store.GetAll(a.Entity)
		if attrs == nil {
			return false
		}
		// Validate types at query time
		if err := db.validateQueryAttrs(typeName, attrs); err != nil {
			return false
		}
		if query.MatchConditions(attrs, conditions) {
			seen[a.Entity] = true
			results = append(results, a.Entity)
		}
		return false
	})

	return results
}

// resolveCandidatesViaIndex attempts to use B-Tree indexes to narrow candidate set.
// Returns nil if no suitable index found (caller should fall back to full scan).
func (db *DB) resolveCandidatesViaIndex(prefix string, conditions []query.Condition) []string {
	if len(conditions) == 0 {
		return nil
	}

	// Find the first condition on an indexed field
	for _, cond := range conditions {
		if !db.Store.HasIndex(cond.Field) {
			continue
		}

		var atoms []*atom.Atom
		switch cond.Operator {
		case "==":
			valStr := fmt.Sprintf("%v", cond.Value)
			atoms = db.Store.QueryIndexed(cond.Field, valStr)
		case ">", ">=", "<", "<=":
			valStr := fmt.Sprintf("%v", cond.Value)
			var rangeOp index.RangeOp
			switch cond.Operator {
			case ">":
				rangeOp = index.OpGt
			case ">=":
				rangeOp = index.OpGte
			case "<":
				rangeOp = index.OpLt
			case "<=":
				rangeOp = index.OpLte
			}
			atoms = db.Store.QueryRange(cond.Field, rangeOp, valStr)
		default:
			continue
		}

		if len(atoms) == 0 {
			return []string{} // Index says no matches
		}

		// Extract unique entity names, filtered by type prefix
		entities := make([]string, 0, len(atoms))
		seen := make(map[string]bool)
		for _, a := range atoms {
			if strings.HasPrefix(a.Entity, prefix) && !seen[a.Entity] {
				seen[a.Entity] = true
				entities = append(entities, a.Entity)
			}
		}
		return entities
	}

	return nil // No usable index found
}

// validateQueryAttrs checks that attribute values match the schema type definition.
func (db *DB) validateQueryAttrs(typeName string, attrs map[string]*atom.Atom) error {
	td, ok := db.Schema.GetType(typeName)
	if !ok {
		return nil // No schema defined, skip validation
	}

	for _, fd := range td.Fields {
		a, exists := attrs[fd.Name]
		if !exists {
			if !fd.Optional && fd.Default == nil {
				return fmt.Errorf("missing required field %q", fd.Name)
			}
			continue
		}
		if a.Type == "deleted" {
			continue
		}
		if err := validateAtomType(fd, a); err != nil {
			return err
		}
	}
	return nil
}

// validateAtomType checks an atom's value matches the expected field type.
func validateAtomType(fd schema.FieldDef, a *atom.Atom) error {
	if a.Value == nil {
		if fd.Optional {
			return nil
		}
		return fmt.Errorf("field %q cannot be nil", fd.Name)
	}
	switch fd.Type {
	case schema.TypeString:
		if _, ok := a.Value.(string); !ok {
			return fmt.Errorf("field %q: expected string, got %T", fd.Name, a.Value)
		}
	case schema.TypeNumber:
		switch a.Value.(type) {
		case float64, float32, int, int64:
			// ok
		default:
			return fmt.Errorf("field %q: expected number, got %T", fd.Name, a.Value)
		}
	case schema.TypeBoolean:
		if _, ok := a.Value.(bool); !ok {
			return fmt.Errorf("field %q: expected boolean, got %T", fd.Name, a.Value)
		}
	case schema.TypeEnum:
		s, ok := a.Value.(string)
		if !ok {
			return fmt.Errorf("field %q: expected string for enum, got %T", fd.Name, a.Value)
		}
		found := false
		for _, ev := range fd.EnumVals {
			if ev == s {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("field %q: invalid enum value %q", fd.Name, s)
		}
	case schema.TypeRef:
		if _, ok := a.Value.(string); !ok {
			return fmt.Errorf("field %q: expected string reference, got %T", fd.Name, a.Value)
		}
	}
	return nil
}

// --- Raw commands (backward compatibility) ---

func cmdSet(s *store.AtomStore, args []string) error {
	if len(args) < 4 {
		return fmt.Errorf("usage: set <entity> <attribute> <value> <type>")
	}
	entity, attribute, rawValue, valueType := args[0], args[1], args[2], args[3]
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

func cmdExplain(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: explain <attribute> <op> <value>")
	}
	plan := s.QueryExplain(args[0], args[1], args[2])
	if plan.UseIndex {
		fmt.Printf("Index scan: %s (%s)\nEstimated rows: %d\n", plan.Attribute, plan.ScanType, plan.EstimatedRows)
	} else {
		fmt.Printf("Full scan\nEstimated rows: %d\nRecommend: CREATE INDEX ON (%s)\n", plan.EstimatedRows, plan.Attribute)
	}
	return nil
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

func cmdStats(s *store.AtomStore) error {
	stats := s.Stats()
	fmt.Printf("Entities: %d\nAtoms: %d\nIndexed: %d\nIndexes: %s\n",
		stats.EntityCount, stats.AtomCount, len(stats.IndexedAttrs),
		strings.Join(stats.IndexedAttrs, ", "))
	return nil
}

func cmdCompact(s *store.AtomStore) error {
	if err := s.Compact(); err != nil {
		return err
	}
	fmt.Println("Compaction complete.")
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
	// Try number
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}
	// Try boolean
	if raw == "true" {
		return true
	}
	if raw == "false" {
		return false
	}
	return raw
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
		safe := strings.ReplaceAll(val, ":", "_")
		safe = strings.ReplaceAll(safe, "/", "_")
		safe = strings.ReplaceAll(safe, "\\", "_")
		safe = strings.ReplaceAll(safe, "\n", "_")
		safe = strings.ReplaceAll(safe, "\r", "_")
		if len(safe) > 256 {
			safe = safe[:256]
		}
		return safe
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
		return "unknown"
	}
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
	fmt.Println()
	fmt.Println("Transactions:")
	fmt.Println("  tx begin|commit|rollback                    Transaction control")
	fmt.Println()
	fmt.Println("Constraints:")
	fmt.Println("  constraint add type field unique|notnull    Add constraint")
	fmt.Println("  constraints type                            List constraints")
	fmt.Println()
	fmt.Println("Relations:")
	fmt.Println("  relation add from.field to.type cardinality  Add relation")
	fmt.Println("  relations type                               List relations")
	fmt.Println()
	fmt.Println("Migrations:")
	fmt.Println("  migrate add type field type                  Add field migration")
	fmt.Println("  migrate remove type field                    Remove field migration")
	fmt.Println("  migrations                                   List migrations")
	fmt.Println()
	fmt.Println("Backup:")
	fmt.Println("  backup path                                  Create backup")
	fmt.Println("  restore path                                 Restore from backup")
	fmt.Println()
	fmt.Println("Joins:")
	fmt.Println("  join left.field right.type right.field       Join types")
	fmt.Println()
	fmt.Println("Aggregations:")
	fmt.Println("  type where cond aggregate count|sum|avg|...")
	fmt.Println()
	fmt.Println("Operators: ==, !=, >, <, >=, <=")
	fmt.Println("Types: string, number, boolean, ref")
	fmt.Println()
	fmt.Println("Raw commands: set, get, getall, query, explain, search, index, stats, compact")
	fmt.Println("             backup, restore, constraint, relation, migrate, join, tx")
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
		db.Schema.Migrations().Record(m)
		db.persistSchema()
		fmt.Printf("Migration applied: %s\n", m.Name)

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
