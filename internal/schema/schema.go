package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// FieldType represents the type of a field in a type definition.
type FieldType int

const (
	TypeString FieldType = iota
	TypeNumber
	TypeBoolean
	TypeRef // reference to another type
	TypeTimestamp
	TypeEnum   // one of a set of values
	TypeNested // inline nested object
)

func (ft FieldType) String() string {
	switch ft {
	case TypeString:
		return "string"
	case TypeNumber:
		return "number"
	case TypeBoolean:
		return "boolean"
	case TypeRef:
		return "ref"
	case TypeTimestamp:
		return "timestamp"
	case TypeEnum:
		return "enum"
	case TypeNested:
		return "nested"
	default:
		return "unknown"
	}
}

// FieldDef defines a single field in a type.
type FieldDef struct {
	Name     string
	Type     FieldType
	RefType  string // target type name for ref fields
	Optional bool   // true if field can be nil
	Default  interface{}
	EnumVals []string   // valid values for enum fields
	Fields   []FieldDef // nested fields for nested type
}

// TypeDef defines a complete type (like a table schema).
type TypeDef struct {
	Name     string
	Fields   []FieldDef
	fieldSet map[string]struct{} // cached set for O(1) field lookup
}

// Schema manages all type definitions.
type Schema struct {
	mu         sync.RWMutex
	types      map[string]*TypeDef
	relations  *RelationManager
	migrations *MigrationLog
}

const (
	MaxTypes         = 1000
	MaxFieldsPerType = 100
	MaxEnumVals      = 100
	MaxNameLength    = 256
	MaxDefaultLen    = 1024
)

func New() *Schema {
	return &Schema{
		types:      make(map[string]*TypeDef),
		relations:  NewRelationManager(),
		migrations: NewMigrationLog(""),
	}
}

// Relations returns the relation manager.
func (s *Schema) Relations() *RelationManager {
	return s.relations
}

// Migrations returns the migration log.
func (s *Schema) Migrations() *MigrationLog {
	return s.migrations
}

func validateIdentifier(name string) error {
	if name == "" {
		return errors.New("name cannot be empty")
	}
	if len(name) > MaxNameLength {
		return fmt.Errorf("name exceeds maximum length of %d bytes", MaxNameLength)
	}
	for i, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("name contains control character at position %d", i)
		}
		if r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
			return fmt.Errorf("name contains unsafe character %q at position %d", r, i)
		}
		if r == '\u2028' || r == '\u2029' {
			return fmt.Errorf("name contains Unicode line separator at position %d", i)
		}
	}
	return nil
}

func validateDefaultValue(val interface{}) error {
	if val == nil {
		return nil
	}
	if s, ok := val.(string); ok {
		if len(s) > MaxDefaultLen {
			return fmt.Errorf("default value exceeds maximum length of %d bytes", MaxDefaultLen)
		}
	}
	return nil
}

// DefineType parses and registers a type definition.
// Input format: TYPE name { field: type, field: type?, field: default_type = value }
func (s *Schema) DefineType(name string, fields []FieldDef) error {
	if err := validateIdentifier(name); err != nil {
		return fmt.Errorf("invalid type name: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.types) >= MaxTypes {
		return fmt.Errorf("maximum number of types (%d) exceeded", MaxTypes)
	}

	if len(fields) > MaxFieldsPerType {
		return fmt.Errorf("type %q has too many fields (max %d)", name, MaxFieldsPerType)
	}

	for _, fd := range fields {
		if err := validateIdentifier(fd.Name); err != nil {
			return fmt.Errorf("invalid field name: %w", err)
		}
		if len(fd.EnumVals) > MaxEnumVals {
			return fmt.Errorf("field %q has too many enum values (max %d)", fd.Name, MaxEnumVals)
		}
		if err := validateDefaultValue(fd.Default); err != nil {
			return fmt.Errorf("field %q: %w", fd.Name, err)
		}
	}

	fs := make(map[string]struct{}, len(fields))
	for _, fd := range fields {
		fs[fd.Name] = struct{}{}
	}

	s.types[name] = &TypeDef{
		Name:     name,
		Fields:   fields,
		fieldSet: fs,
	}
	return nil
}

// GetType returns a type definition by name.
func (s *Schema) GetType(name string) (*TypeDef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	td, ok := s.types[name]
	return td, ok
}

// ListTypes returns all type names sorted alphabetically.
func (s *Schema) ListTypes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.types))
	for n := range s.types {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// HasType checks if a type is defined.
func (s *Schema) HasType(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.types[name]
	return ok
}

// Validate checks that a set of field values matches the type definition.
// Returns the validated fields with defaults applied, or an error.
func (s *Schema) Validate(typeName string, fields map[string]interface{}) (map[string]interface{}, error) {
	s.mu.RLock()
	td, ok := s.types[typeName]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown type %q", typeName)
	}

	result := make(map[string]interface{})

	// Apply defaults and validate
	for _, fd := range td.Fields {
		val, exists := fields[fd.Name]

		if !exists {
			if fd.Default != nil {
				result[fd.Name] = fd.Default
			} else if !fd.Optional {
				return nil, fmt.Errorf("missing required field %q for type %q", fd.Name, typeName)
			}
			continue
		}

		// Validate type
		if err := validateValue(fd, val); err != nil {
			return nil, fmt.Errorf("field %q: %w", fd.Name, err)
		}

		result[fd.Name] = val
	}

	// Check for unknown fields — O(1) lookup per input field using cached set
	for k := range fields {
		if _, ok := td.fieldSet[k]; !ok {
			return nil, fmt.Errorf("unknown field %q for type %q", k, typeName)
		}
	}

	return result, nil
}

func validateValue(fd FieldDef, val interface{}) error {
	if val == nil {
		if fd.Optional {
			return nil
		}
		return fmt.Errorf("field %q cannot be nil", fd.Name)
	}

	switch fd.Type {
	case TypeString:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected string, got %T", val)
		}
	case TypeNumber:
		switch val.(type) {
		case float64, float32, int, int64:
			// OK
		default:
			return fmt.Errorf("expected number, got %T", val)
		}
	case TypeBoolean:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", val)
		}
	case TypeEnum:
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for enum, got %T", val)
		}
		found := false
		for _, ev := range fd.EnumVals {
			if ev == s {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("invalid enum value %q, must be one of: %s", s, strings.Join(fd.EnumVals, ", "))
		}
	case TypeRef:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected string reference, got %T", val)
		}
	}
	return nil
}

// ParseTypeDefinition parses a type definition string.
// Format: TYPE name { field: type, field: type?, field: type = default }
func ParseTypeDefinition(input string) (string, []FieldDef, error) {
	input = strings.TrimSpace(input)

	// Expect: TYPE name { ... }
	if !strings.HasPrefix(strings.ToUpper(input), "TYPE ") {
		return "", nil, fmt.Errorf("type definition must start with TYPE")
	}

	// Extract name and body
	rest := strings.TrimSpace(input[5:])
	openBrace := strings.Index(rest, "{")
	if openBrace < 0 {
		return "", nil, fmt.Errorf("missing opening brace")
	}

	name := strings.TrimSpace(rest[:openBrace])
	if err := validateIdentifier(name); err != nil {
		return "", nil, fmt.Errorf("invalid type name: %w", err)
	}
	body := rest[openBrace+1:]

	closeBrace := strings.LastIndex(body, "}")
	if closeBrace < 0 {
		return "", nil, fmt.Errorf("missing closing brace")
	}
	body = strings.TrimSpace(body[:closeBrace])

	// Parse fields - split on newlines and commas
	body = strings.ReplaceAll(body, ",", "\n")
	var fields []FieldDef
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimSuffix(line, ",")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fd, err := parseFieldDef(line)
		if err != nil {
			return "", nil, fmt.Errorf("parsing field %q: %w", line, err)
		}
		fields = append(fields, fd)
	}

	return name, fields, nil
}

func parseFieldDef(line string) (FieldDef, error) {
	// Format: name: type, name: type?, name: type = default, name: val1|val2|val3
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return FieldDef{}, fmt.Errorf("expected 'name: type', got %q", line)
	}

	name := strings.TrimSpace(parts[0])
	typeStr := strings.TrimSpace(parts[1])

	fd := FieldDef{Name: name}

	// Check for optional (?)
	if strings.HasSuffix(typeStr, "?") {
		fd.Optional = true
		typeStr = strings.TrimSpace(strings.TrimSuffix(typeStr, "?"))
	}

	// Check for default value (= value)
	if idx := strings.Index(typeStr, "="); idx >= 0 {
		defaultStr := strings.TrimSpace(typeStr[idx+1:])
		typeStr = strings.TrimSpace(typeStr[:idx])
		fd.Default = parseDefaultValue(defaultStr)
	}

	// Check for enum (val1|val2|val3)
	if strings.Contains(typeStr, "|") && !strings.Contains(typeStr, "ref(") {
		vals := strings.Split(typeStr, "|")
		fd.Type = TypeEnum
		fd.EnumVals = make([]string, len(vals))
		for i, v := range vals {
			fd.EnumVals[i] = strings.TrimSpace(v)
		}
		return fd, nil
	}

	// Parse type
	switch strings.ToLower(typeStr) {
	case "string":
		fd.Type = TypeString
	case "number":
		fd.Type = TypeNumber
	case "boolean", "bool":
		fd.Type = TypeBoolean
	case "timestamp":
		fd.Type = TypeTimestamp
	default:
		if strings.HasPrefix(strings.ToLower(typeStr), "ref(") && strings.HasSuffix(typeStr, ")") {
			fd.Type = TypeRef
			fd.RefType = typeStr[4 : len(typeStr)-1]
		} else {
			// Treat as nested type reference
			fd.Type = TypeRef
			fd.RefType = typeStr
		}
	}

	return fd, nil
}

func parseDefaultValue(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}
	if s == "nil" || s == "null" {
		return nil
	}
	// Try number
	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
		return f
	}
	// Strip quotes for strings
	if (strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) ||
		(strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) {
		return s[1 : len(s)-1]
	}
	return s
}

// serializableTypeDef is the JSON representation of a TypeDef.
type serializableTypeDef struct {
	Name   string              `json:"name"`
	Fields []serializableField `json:"fields"`
}

type serializableField struct {
	Name     string      `json:"name"`
	Type     string      `json:"type"`
	RefType  string      `json:"ref_type,omitempty"`
	Optional bool        `json:"optional,omitempty"`
	Default  interface{} `json:"default,omitempty"`
	EnumVals []string    `json:"enum_vals,omitempty"`
}

// serializableSchema is the top-level JSON structure for the schema file.
type serializableSchema struct {
	Types      []serializableTypeDef `json:"types"`
	Migrations []Migration           `json:"migrations,omitempty"`
}

// SaveToFile persists the schema to a JSON file.
func (s *Schema) SaveToFile(path string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var defs []serializableTypeDef
	for _, td := range s.types {
		var fields []serializableField
		for _, f := range td.Fields {
			fields = append(fields, serializableField{
				Name:     f.Name,
				Type:     f.Type.String(),
				RefType:  f.RefType,
				Optional: f.Optional,
				Default:  f.Default,
				EnumVals: f.EnumVals,
			})
		}
		defs = append(defs, serializableTypeDef{Name: td.Name, Fields: fields})
	}

	schema := serializableSchema{
		Types:      defs,
		Migrations: s.migrations.Applied(),
	}

	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// LoadFromFile loads the schema from a JSON file.
func (s *Schema) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Try new format first (with migrations)
	var schema serializableSchema
	if err := json.Unmarshal(data, &schema); err == nil && schema.Types != nil {
		return s.loadFromSerializable(schema)
	}

	// Fall back to legacy format (array of type defs)
	var defs []serializableTypeDef
	if err := json.Unmarshal(data, &defs); err != nil {
		return err
	}

	return s.loadFromSerializable(serializableSchema{Types: defs})
}

// loadFromSerializable loads schema from a serializableSchema structure.
func (s *Schema) loadFromSerializable(schema serializableSchema) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, d := range schema.Types {
		if err := validateIdentifier(d.Name); err != nil {
			return fmt.Errorf("invalid type name %q: %w", d.Name, err)
		}
		if len(s.types) >= MaxTypes {
			return fmt.Errorf("maximum number of types (%d) exceeded in schema file", MaxTypes)
		}
		var fields []FieldDef
		for _, f := range d.Fields {
			if err := validateIdentifier(f.Name); err != nil {
				return fmt.Errorf("invalid field name %q: %w", f.Name, err)
			}
			if err := validateDefaultValue(f.Default); err != nil {
				return fmt.Errorf("field %q: %w", f.Name, err)
			}
			fields = append(fields, FieldDef{
				Name:     f.Name,
				Type:     ParseFieldType(f.Type),
				RefType:  f.RefType,
				Optional: f.Optional,
				Default:  f.Default,
				EnumVals: f.EnumVals,
			})
		}
		fs := make(map[string]struct{}, len(fields))
		for _, fd := range fields {
			fs[fd.Name] = struct{}{}
		}
		s.types[d.Name] = &TypeDef{Name: d.Name, Fields: fields, fieldSet: fs}
	}

	// Restore migrations
	if len(schema.Migrations) > 0 {
		s.migrations.migrations = schema.Migrations
	}

	return nil
}

// ParseFieldType converts a string to a FieldType.
func ParseFieldType(s string) FieldType {
	switch s {
	case "string":
		return TypeString
	case "number":
		return TypeNumber
	case "boolean":
		return TypeBoolean
	case "ref":
		return TypeRef
	case "timestamp":
		return TypeTimestamp
	case "enum":
		return TypeEnum
	case "nested":
		return TypeNested
	default:
		return TypeString
	}
}

// ValidateRefs checks that all ref fields in the given values point to existing entities.
// The existsFn callback should return true if the entity exists in the store.
func (s *Schema) ValidateRefs(typeName string, fields map[string]interface{}, existsFn func(entityID string) bool) error {
	s.mu.RLock()
	td, ok := s.types[typeName]
	s.mu.RUnlock()

	if !ok {
		return nil // unknown type, skip ref validation
	}

	for _, fd := range td.Fields {
		if fd.Type != TypeRef {
			continue
		}
		val, exists := fields[fd.Name]
		if !exists || val == nil {
			continue // missing or nil ref is OK if optional
		}
		refID, ok := val.(string)
		if !ok {
			continue // type error caught by Validate
		}
		if refID == "" {
			continue
		}
		if !existsFn(refID) {
			return fmt.Errorf("referential integrity violation: field %q references non-existent entity %q", fd.Name, refID)
		}
	}
	return nil
}
