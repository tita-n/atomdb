package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestValidateIdentifier_ValidNames(t *testing.T) {
	validNames := []string{
		"user",
		"user_name",
		"userName",
		"User123",
		"name_with_underscores",
		"camelCase",
		"PascalCase",
		"with-dashes",
		"with.dots",
		"with@ats",
		"with#hashes",
		"with$dollars",
		"with%percent",
		"with^carets",
		"with&ampersands",
		"with+pluses",
		"with=equals",
		"with_brackets",
		"with~tildes",
		"with`backticks",
		"with[brackets]",
		"with{braces}",
		"with(parentheses)",
		"with'singlequotes",
		"with spaces",
		"with    many    spaces",
		"unicode_café",
		"unicode_日本語",
	}

	for _, name := range validNames {
		err := validateIdentifier(name)
		if err != nil {
			t.Errorf("validateIdentifier(%q) returned error: %v", name, err)
		}
	}
}

func TestValidateIdentifier_InvalidNames(t *testing.T) {
	invalidCases := []struct {
		name        string
		shouldError bool
	}{
		{"", true},
		{string([]byte{0}), true},
		{"name\x00withnull", true},
		{"name\x01withctrl", true},
		{"name\x1b[31mwithansi", true},
		{"/starts/with/slash", true},
		{"back\\slash", true},
		{"colon:name", true},
		{"star*name", true},
		{"question?name", true},
		{"quote\"name", true},
		{"angle<name>", true},
		{"pipe|name", true},
		{"name" + string(rune(0x2028)), true},
		{"name" + string(rune(0x2029)), true},
		{string(make([]byte, 257)), true},
	}

	for _, tc := range invalidCases {
		err := validateIdentifier(tc.name)
		if tc.shouldError && err == nil {
			t.Errorf("validateIdentifier(%q) should have returned error but didn't", tc.name)
		}
		if !tc.shouldError && err != nil {
			t.Errorf("validateIdentifier(%q) returned unexpected error: %v", tc.name, err)
		}
	}
}

func TestDefineType_ValidTypes(t *testing.T) {
	s := New()

	validTypes := []struct {
		name   string
		fields []FieldDef
	}{
		{"user", []FieldDef{{Name: "name", Type: TypeString}, {Name: "age", Type: TypeNumber}}},
		{"Product", []FieldDef{{Name: "id", Type: TypeNumber}, {Name: "price", Type: TypeNumber}}},
		{"order", []FieldDef{{Name: "status", Type: TypeEnum, EnumVals: []string{"pending", "shipped", "delivered"}}}},
	}

	for _, tc := range validTypes {
		err := s.DefineType(tc.name, tc.fields)
		if err != nil {
			t.Errorf("DefineType(%q) returned error: %v", tc.name, err)
		}
	}
}

func TestDefineType_InvalidNames(t *testing.T) {
	s := New()

	invalidNames := []string{
		"",
		"type/name",
		"type\\name",
		"type:name",
		"type\x00name",
		"type\x1b[31mred",
	}

	for _, name := range invalidNames {
		err := s.DefineType(name, []FieldDef{{Name: "field", Type: TypeString}})
		if err == nil {
			t.Errorf("DefineType(%q) should have returned error for invalid name", name)
		}
	}
}

func TestDefineType_InvalidFieldNames(t *testing.T) {
	s := New()

	invalidFieldNames := []string{
		"",
		"field/name",
		"field\\name",
		"field:name",
		"field\x00name",
	}

	for _, fname := range invalidFieldNames {
		err := s.DefineType("mytype", []FieldDef{{Name: fname, Type: TypeString}})
		if err == nil {
			t.Errorf("DefineType with field %q should have returned error", fname)
		}
	}
}

func TestDefineType_NameLengthLimit(t *testing.T) {
	s := New()

	longName := string(make([]byte, MaxNameLength+1))
	err := s.DefineType(longName, []FieldDef{{Name: "field", Type: TypeString}})
	if err == nil {
		t.Error("DefineType with name exceeding MaxNameLength should fail")
	}
}

func TestDefineType_MaxTypesLimit(t *testing.T) {
	s := New()

	for i := 0; i < MaxTypes; i++ {
		err := s.DefineType("type_"+fmt.Sprintf("%d", i), []FieldDef{{Name: "f", Type: TypeString}})
		if err != nil {
			t.Fatalf("DefineType %d failed unexpectedly: %v", i, err)
		}
	}

	err := s.DefineType("overflow_type", []FieldDef{{Name: "f", Type: TypeString}})
	if err == nil {
		t.Error("DefineType should fail when exceeding MaxTypes")
	}
}

func TestDefineType_MaxFieldsLimit(t *testing.T) {
	s := New()

	fields := make([]FieldDef, MaxFieldsPerType+1)
	for i := range fields {
		fields[i] = FieldDef{Name: "field" + string(rune('a'+i%26)), Type: TypeString}
	}

	err := s.DefineType("toomanyfields", fields)
	if err == nil {
		t.Error("DefineType should fail when exceeding MaxFieldsPerType")
	}
}

func TestDefineType_MaxEnumValsLimit(t *testing.T) {
	s := New()

	enumVals := make([]string, MaxEnumVals+1)
	for i := range enumVals {
		enumVals[i] = "val" + string(rune('a'+i%26))
	}

	err := s.DefineType("toomanyenums", []FieldDef{
		{Name: "field", Type: TypeEnum, EnumVals: enumVals},
	})
	if err == nil {
		t.Error("DefineType should fail when exceeding MaxEnumVals")
	}
}

func TestDefineType_DefaultValueLengthLimit(t *testing.T) {
	s := New()

	longDefault := string(make([]byte, MaxDefaultLen+1))
	err := s.DefineType("longdefault", []FieldDef{
		{Name: "field", Type: TypeString, Default: longDefault},
	})
	if err == nil {
		t.Error("DefineType should fail when default value exceeds MaxDefaultLen")
	}
}

func TestParseTypeDefinition_ValidInput(t *testing.T) {
	validInputs := []string{
		"TYPE user { name: string, age: number }",
		"TYPE product { id: number, name: string?, price: number = 0 }",
		"TYPE status { state: pending|active|completed }",
		"TYPE ref_test { owner: ref(user) }",
	}

	for _, input := range validInputs {
		_, _, err := ParseTypeDefinition(input)
		if err != nil {
			t.Errorf("ParseTypeDefinition(%q) returned error: %v", input, err)
		}
	}
}

func TestParseTypeDefinition_InvalidNames(t *testing.T) {
	invalidInputs := []string{
		"TYPE type/name { f: string }",
		"TYPE type\\name { f: string }",
		"TYPE :name { f: string }",
	}

	for _, input := range invalidInputs {
		_, _, err := ParseTypeDefinition(input)
		if err == nil {
			t.Errorf("ParseTypeDefinition(%q) should have returned error for invalid type name", input)
		}
	}
}

func TestSaveToFileAndLoadFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "schema.json")

	s1 := New()
	err := s1.DefineType("user", []FieldDef{
		{Name: "name", Type: TypeString},
		{Name: "age", Type: TypeNumber, Optional: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s1.SaveToFile(path)
	if err != nil {
		t.Fatal(err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if fi.Mode().Perm() != 0600 {
			t.Errorf("schema file permissions = %o, want 0600", fi.Mode().Perm())
		}
	}

	s2 := New()
	err = s2.LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}

	td, ok := s2.GetType("user")
	if !ok {
		t.Fatal("type 'user' not found after reload")
	}
	if len(td.Fields) != 2 {
		t.Errorf("got %d fields, want 2", len(td.Fields))
	}
}

func TestLoadFromFile_InvalidData(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "schema.json")

	if err := os.WriteFile(path, []byte("invalid json"), 0600); err != nil {
		t.Fatal(err)
	}

	s := New()
	err := s.LoadFromFile(path)
	if err == nil {
		t.Error("LoadFromFile should fail for invalid JSON")
	}
}

func TestLoadFromFile_InvalidTypeName(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "schema.json")

	invalidSchema := `[{"name": "type/name", "fields": [{"name": "f", "type": "string"}]}]`
	if err := os.WriteFile(path, []byte(invalidSchema), 0600); err != nil {
		t.Fatal(err)
	}

	s := New()
	err := s.LoadFromFile(path)
	if err == nil {
		t.Error("LoadFromFile should fail for invalid type name in JSON")
	}
}

func TestLoadFromFile_TooManyTypes(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "schema.json")

	types := make([]map[string]interface{}, MaxTypes+1)
	for i := range types {
		types[i] = map[string]interface{}{
			"name": "type" + string(rune(i)),
			"fields": []map[string]string{
				{"name": "f", "type": "string"},
			},
		}
	}

	data, err := json.Marshal(types)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	s := New()
	err = s.LoadFromFile(path)
	if err == nil {
		t.Error("LoadFromFile should fail when exceeding MaxTypes")
	}
}
