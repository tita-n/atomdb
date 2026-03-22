package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/tita-n/atomdb/internal/atom"
	"github.com/tita-n/atomdb/internal/index"
	"github.com/tita-n/atomdb/internal/store"
)

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

func cmdSet(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: set <entity> <attribute> <value> <type>")
	}

	entity := args[0]
	attribute := args[1]

	var rawValue string
	var valueType string

	if len(args) == 3 {
		rawValue = ""
		valueType = args[2]
	} else if len(args) == 4 {
		rawValue = args[2]
		valueType = args[3]
	} else {
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

func cmdQuery(s *store.AtomStore, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: query <attribute> <operator> <value> [AND|OR ...]")
	}

	conditions := parseConditions(args)
	if len(conditions) == 0 {
		return fmt.Errorf("no valid conditions found")
	}

	var resultSets [][]*atom.Atom
	for _, cond := range conditions {
		results, err := executeCondition(s, cond)
		if err != nil {
			return err
		}
		resultSets = append(resultSets, results)
	}

	logic := "AND"
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

func cmdDelete(s *store.AtomStore, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: delete <entity> <attribute>")
	}

	entity := args[0]
	attribute := args[1]

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

func cmdCompact(s *store.AtomStore) error {
	if err := s.Compact(); err != nil {
		return err
	}
	fmt.Println("Compaction complete.")
	return nil
}

type condition struct {
	Attribute string
	Operator  string
	Value     string
	Logic     string
}

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

func executeCondition(s *store.AtomStore, cond condition) ([]*atom.Atom, error) {
	if !isValidOperator(cond.Operator) {
		return nil, fmt.Errorf("unsupported operator %q: use ==, !=, >, <, >=, <=", cond.Operator)
	}

	if cond.Operator == "==" {
		if indexed := s.QueryIndexed(cond.Attribute, cond.Value); indexed != nil {
			return indexed, nil
		}
	}

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

func combineResults(sets [][]*atom.Atom, primaryLogic string) []*atom.Atom {
	if len(sets) == 0 {
		return nil
	}
	if len(sets) == 1 {
		return sets[0]
	}

	if strings.ToUpper(primaryLogic) == "OR" {
		return unionAtoms(sets)
	}
	return intersectAtoms(sets)
}

func intersectAtoms(sets [][]*atom.Atom) []*atom.Atom {
	if len(sets) == 0 {
		return nil
	}

	entitySets := make([]map[string]bool, len(sets))
	for i, set := range sets {
		entities := make(map[string]bool)
		for _, a := range set {
			entities[a.Entity] = true
		}
		entitySets[i] = entities
	}

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
	fmt.Println("  query age \">\" 25")
	fmt.Println("  query age \">\" 25 AND city == Lagos")
}
