package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/user/atomdb/internal/atom"
	"github.com/user/atomdb/internal/index"
	"github.com/user/atomdb/internal/store"
)

// Run dispatches CLI arguments to the appropriate command.
func Run(s *store.AtomStore, args []string) error {
	if len(args) == 0 {
		printHelp()
		return nil
	}

	cmd := strings.ToLower(args[0])

	switch cmd {
	case "set":
		return cmdSet(s, args[1:])
	case "get":
		return cmdGet(s, args[1:])
	case "getall":
		return cmdGetAll(s, args[1:])
	case "query":
		return cmdQuery(s, args[1:])
	case "explain":
		return cmdExplain(s, args[1:])
	case "delete":
		return cmdDelete(s, args[1:])
	case "search":
		return cmdSearch(s, args[1:])
	case "index":
		return cmdIndex(s, args[1:])
	case "stats":
		return cmdStats(s)
	case "compact":
		return cmdCompact(s)
	case "help":
		printHelp()
		return nil
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printHelp()
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

// cmdSet handles: set <entity> <attribute> <value> <type>
// Empty string values are supported: set test:1 empty "" string
func cmdSet(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: set <entity> <attribute> <value> <type>")
	}

	entity := args[0]
	attribute := args[1]

	var rawValue string
	var valueType string

	if len(args) == 3 {
		// Format: set entity attribute type (empty value)
		// Or: set entity attribute value (missing type)
		// We need at least 4 args. If 3, treat last as type with empty value.
		rawValue = ""
		valueType = args[2]
	} else if len(args) == 4 {
		rawValue = args[2]
		valueType = args[3]
	} else {
		// More than 4 args: value might be split by shell
		// Last arg is type, everything in between is value
		rawValue = strings.Join(args[2:len(args)-1], " ")
		valueType = args[len(args)-1]
	}

	value, err := parseValue(rawValue, valueType)
	if err != nil {
		return fmt.Errorf("invalid value for type %s: %w", valueType, err)
	}

	if err := s.Set(entity, attribute, value, valueType); err != nil {
		return err
	}

	if rawValue == "" {
		fmt.Printf("Stored: %s.%s = \"\" (%s)\n", entity, attribute, valueType)
	} else {
		fmt.Printf("Stored: %s.%s = %v\n", entity, attribute, value)
	}
	return nil
}

// cmdGet handles: get <entity> <attribute>
func cmdGet(s *store.AtomStore, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: get <entity> <attribute>")
	}

	entity := args[0]
	attribute := args[1]

	a, ok := s.Get(entity, attribute)
	if !ok {
		fmt.Printf("Not found: %s.%s\n", entity, attribute)
		return nil
	}

	fmt.Printf("Entity: %s\n", a.Entity)
	fmt.Printf("Attribute: %s\n", a.Attribute)
	fmt.Printf("Value: %v\n", a.Value)
	fmt.Printf("Type: %s\n", a.Type)
	fmt.Printf("Timestamp: %s\n", a.Timestamp.Format("2006-01-02 15:04:05.000000000"))
	fmt.Printf("Version: %d\n", a.Version)

	return nil
}

// cmdGetAll handles: getall <entity>
func cmdGetAll(s *store.AtomStore, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: getall <entity>")
	}

	entity := args[0]
	attrs := s.GetAll(entity)

	if len(attrs) == 0 {
		fmt.Printf("No attributes found for: %s\n", entity)
		return nil
	}

	// Sort attribute names for consistent output
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, attr := range names {
		a := attrs[attr]
		fmt.Printf("%s.%s = %v (%s)\n", entity, attr, a.Value, a.Type)
	}

	return nil
}

// cmdQuery handles compound queries:
//
//	query attribute op value
//	query attribute op value AND attribute op value
//	query attribute op value OR attribute op value
//
// Operators: ==, !=, >, <, >=, <=
// Uses B-Tree index when available, falls back to full scan.
// Shell note: use quotes for operators, e.g. query age ">" 25
func cmdQuery(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: query <attribute> <operator> <value> [AND|OR ...]")
	}

	// Parse conditions separated by AND/OR
	conditions := parseConditions(args)
	if len(conditions) == 0 {
		return fmt.Errorf("no valid conditions found")
	}

	// Execute each condition
	var resultSets [][]*atom.Atom
	for _, cond := range conditions {
		results, err := executeCondition(s, cond)
		if err != nil {
			return err
		}
		resultSets = append(resultSets, results)
	}

	// Combine results using the logic operator from the last condition
	// (all conditions between queries share the same logic in this simple parser)
	logic := "AND" // default
	for _, cond := range conditions {
		if cond.Logic != "" {
			logic = cond.Logic
		}
	}
	final := combineResults(resultSets, logic)

	if len(final) == 0 {
		fmt.Println("No results found.")
		return nil
	}

	for _, a := range final {
		fmt.Printf("%s.%s = %v\n", a.Entity, a.Attribute, a.Value)
	}

	return nil
}

// cmdExplain shows the query execution plan.
func cmdExplain(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: explain <attribute> <operator> <value>")
	}

	attribute := args[0]
	operator := args[1]
	value := args[2]

	if !isValidOperator(operator) {
		return fmt.Errorf("unsupported operator %q", operator)
	}

	plan := s.QueryExplain(attribute, operator, value)

	if plan.UseIndex {
		fmt.Printf("Using index on %s (%s scan)\n", plan.Attribute, plan.ScanType)
	} else {
		fmt.Printf("Full scan (no index on %s)\n", plan.Attribute)
	}
	fmt.Printf("Operator: %s\n", plan.Operator)
	fmt.Printf("Value: %s\n", plan.Value)
	fmt.Printf("Estimated cost: %d rows\n", plan.EstimatedRows)

	return nil
}

// cmdDelete handles: delete <entity> <attribute>
// Checks existence before deleting.
func cmdDelete(s *store.AtomStore, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: delete <entity> <attribute>")
	}

	entity := args[0]
	attribute := args[1]

	// Check existence first
	if !s.Exists(entity, attribute) {
		fmt.Printf("Not found: %s.%s\n", entity, attribute)
		return nil
	}

	if err := s.Delete(entity, attribute); err != nil {
		return err
	}

	fmt.Printf("Deleted: %s.%s\n", entity, attribute)
	return nil
}

// cmdSearch handles: search <attribute> contains <word>
// Full-text search using inverted index.
func cmdSearch(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: search <attribute> contains <word>")
	}

	attribute := args[0]
	if strings.ToLower(args[1]) != "contains" {
		return fmt.Errorf("usage: search <attribute> contains <word>")
	}
	word := args[2]

	results := s.FullTextSearch(attribute, word)

	if len(results) == 0 {
		fmt.Println("No results found.")
		return nil
	}

	for _, a := range results {
		fmt.Printf("%s.%s = %v\n", a.Entity, a.Attribute, a.Value)
	}

	return nil
}

// cmdIndex handles: index list | index rebuild
func cmdIndex(s *store.AtomStore, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: index <list|rebuild>")
	}

	switch strings.ToLower(args[0]) {
	case "list":
		stats := s.Stats()
		if len(stats.IndexedAttrs) == 0 {
			fmt.Println("No indexes.")
			return nil
		}
		fmt.Println("Indexed attributes:")
		for _, attr := range stats.IndexedAttrs {
			fmt.Printf("  %s\n", attr)
		}
		fmt.Printf("Total index keys: %d\n", stats.IndexKeyCount)
		return nil

	case "rebuild":
		s.RebuildIndexes()
		fmt.Println("Indexes rebuilt.")
		return nil

	default:
		return fmt.Errorf("usage: index <list|rebuild>")
	}
}

// cmdStats shows store statistics.
func cmdStats(s *store.AtomStore) error {
	stats := s.Stats()

	fmt.Printf("Entities: %d\n", stats.EntityCount)
	fmt.Printf("Atoms: %d\n", stats.AtomCount)
	fmt.Printf("Indexed attributes: %d\n", len(stats.IndexedAttrs))
	fmt.Printf("Index keys: %d\n", stats.IndexKeyCount)
	fmt.Printf("History size: %d\n", stats.HistorySize)

	if len(stats.IndexedAttrs) > 0 {
		fmt.Printf("Indexes: %s\n", strings.Join(stats.IndexedAttrs, ", "))
	}

	return nil
}

// cmdCompact handles: compact
func cmdCompact(s *store.AtomStore) error {
	if err := s.Compact(); err != nil {
		return err
	}
	fmt.Println("Compaction complete.")
	return nil
}

// --- Query condition parsing ---

type condition struct {
	Attribute string
	Operator  string
	Value     string
	Logic     string // "AND", "OR", or "" for first/only
}

// parseConditions splits args like [age, >, 25, AND, city, ==, Lagos] into conditions.
func parseConditions(args []string) []condition {
	var conditions []condition
	current := condition{}
	logic := ""

	for i := 0; i < len(args); i++ {
		arg := args[i]
		upper := strings.ToUpper(arg)

		if upper == "AND" || upper == "OR" {
			if current.Attribute != "" {
				current.Logic = logic
				conditions = append(conditions, current)
				current = condition{}
			}
			logic = upper
			continue
		}

		if current.Attribute == "" {
			current.Attribute = arg
		} else if current.Operator == "" {
			current.Operator = arg
		} else if current.Value == "" {
			current.Value = arg
		}
	}

	if current.Attribute != "" && current.Operator != "" {
		current.Logic = logic
		conditions = append(conditions, current)
	}

	return conditions
}

// executeCondition runs a single query condition, trying index first.
func executeCondition(s *store.AtomStore, cond condition) ([]*atom.Atom, error) {
	if !isValidOperator(cond.Operator) {
		return nil, fmt.Errorf("unsupported operator %q: use ==, !=, >, <, >=, <=", cond.Operator)
	}

	// Try index first for ==, >, <, >=, <=
	if cond.Operator == "==" {
		if indexed := s.QueryIndexed(cond.Attribute, cond.Value); indexed != nil {
			return indexed, nil
		}
	}

	// Range queries via index
	if cond.Operator == ">" || cond.Operator == ">=" || cond.Operator == "<" || cond.Operator == "<=" {
		var op index.RangeOp
		switch cond.Operator {
		case ">":
			op = index.OpGt
		case ">=":
			op = index.OpGte
		case "<":
			op = index.OpLt
		case "<=":
			op = index.OpLte
		}
		if indexed := s.QueryRange(cond.Attribute, op, cond.Value); indexed != nil {
			return indexed, nil
		}
	}

	// Fall back to scan
	searchValue := cond.Value
	results := s.Query(cond.Attribute, func(a *atom.Atom) bool {
		aFloat, aErr := toFloat64(a.Value)
		sFloat, sErr := strconv.ParseFloat(searchValue, 64)

		if aErr == nil && sErr == nil {
			return compareFloats(aFloat, sFloat, cond.Operator)
		}

		aStr := fmt.Sprintf("%v", a.Value)
		return compareStrings(aStr, searchValue, cond.Operator)
	})

	return results, nil
}

// combineResults merges result sets based on AND/OR logic.
func combineResults(sets [][]*atom.Atom, primaryLogic string) []*atom.Atom {
	if len(sets) == 0 {
		return nil
	}
	if len(sets) == 1 {
		return sets[0]
	}

	// Use primary logic to combine all sets
	if strings.ToUpper(primaryLogic) == "OR" {
		return unionAtoms(sets)
	}
	// Default to AND
	return intersectAtoms(sets)
}

// intersectAtoms returns atoms from the first set whose entity appears in ALL sets.
func intersectAtoms(sets [][]*atom.Atom) []*atom.Atom {
	if len(sets) == 0 {
		return nil
	}

	// For each set, collect the set of entities
	entitySets := make([]map[string]bool, len(sets))
	for i, set := range sets {
		entities := make(map[string]bool)
		for _, a := range set {
			entities[a.Entity] = true
		}
		entitySets[i] = entities
	}

	// Find entities that appear in ALL sets
	commonEntities := make(map[string]bool)
	for e := range entitySets[0] {
		inAll := true
		for i := 1; i < len(entitySets); i++ {
			if !entitySets[i][e] {
				inAll = false
				break
			}
		}
		if inAll {
			commonEntities[e] = true
		}
	}

	// Return results from the first set that match common entities
	seen := make(map[string]bool)
	var result []*atom.Atom
	for _, a := range sets[0] {
		if commonEntities[a.Entity] {
			key := a.Entity + "." + a.Attribute
			if !seen[key] {
				seen[key] = true
				result = append(result, a)
			}
		}
	}
	return result
}

// unionAtoms returns all unique atoms across all sets.
func unionAtoms(sets [][]*atom.Atom) []*atom.Atom {
	seen := make(map[string]bool)
	var result []*atom.Atom

	for _, set := range sets {
		for _, a := range set {
			key := a.Entity + "." + a.Attribute
			if !seen[key] {
				seen[key] = true
				result = append(result, a)
			}
		}
	}
	return result
}

// --- Value parsing and comparison ---

func parseValue(raw, valueType string) (interface{}, error) {
	switch strings.ToLower(valueType) {
	case "string":
		return raw, nil
	case "number":
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as number: %w", raw, err)
		}
		return val, nil
	case "boolean", "bool":
		val, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as boolean: %w", raw, err)
		}
		return val, nil
	case "ref":
		return raw, nil
	case "timestamp":
		return raw, nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", valueType)
	}
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case json.Number:
		return val.Float64()
	default:
		str := fmt.Sprintf("%v", val)
		if f, err := strconv.ParseFloat(str, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("not a number")
	}
}

func compareFloats(a, b float64, op string) bool {
	switch op {
	case "==":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	default:
		return false
	}
}

func compareStrings(a, b, op string) bool {
	switch op {
	case "==":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	default:
		return false
	}
}

func isValidOperator(op string) bool {
	switch op {
	case "==", "!=", ">", "<", ">=", "<=":
		return true
	default:
		return false
	}
}

func printHelp() {
	fmt.Println("AtomDB - Local-first database built on atoms")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  set <entity> <attribute> <value> <type>      Store an atom")
	fmt.Println("  get <entity> <attribute>                     Retrieve an atom")
	fmt.Println("  getall <entity>                              Get all attributes for entity")
	fmt.Println("  query <attribute> <op> <value> [AND|OR ...]  Query atoms")
	fmt.Println("  explain <attribute> <op> <value>             Show query execution plan")
	fmt.Println("  delete <entity> <attribute>                  Delete an atom")
	fmt.Println("  search <attribute> contains <word>           Full-text search")
	fmt.Println("  index list                                   List indexed attributes")
	fmt.Println("  index rebuild                                Rebuild all indexes")
	fmt.Println("  stats                                        Show store statistics")
	fmt.Println("  compact                                      Compact the data file")
	fmt.Println("  help                                         Show this help")
	fmt.Println()
	fmt.Println("Operators: ==, !=, >, <, >=, <=")
	fmt.Println("Types: string, number, boolean, ref, timestamp")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  query age > 25")
	fmt.Println("  query age > 25 AND city == Lagos")
	fmt.Println("  query name == \"Ayo Adeleke\"")
	fmt.Println("  search name contains Ayo")
}
