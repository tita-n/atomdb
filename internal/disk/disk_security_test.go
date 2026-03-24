package disk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tita-n/atomdb/internal/atom"
)

func TestLoad_ValidAtoms(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	data := `{"entity":"user:1","attribute":"name","value":"Alice","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
{"entity":"user:1","attribute":"age","value":25,"type":"number","timestamp":"2024-01-01T00:00:00Z","version":2}
{"entity":"user:2","attribute":"name","value":"Bob","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	atoms, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(atoms) != 3 {
		t.Errorf("got %d atoms, want 3", len(atoms))
	}
}

func TestLoad_SkipsCorruptLines(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	data := `{"entity":"user:1","attribute":"name","value":"Alice","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
invalid json line
{"entity":"user:2","attribute":"name","value":"Bob","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	atoms, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(atoms) != 2 {
		t.Errorf("got %d atoms, want 2 (skipped corrupt line)", len(atoms))
	}
}

func TestLoad_SkipsMissingFields(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	data := `{"entity":"user:1","attribute":"name","value":"Alice","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
{"entity":"","attribute":"name","value":"Empty","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
{"entity":"user:2","attribute":"","value":"Bob","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
{"entity":"user:3","attribute":"name","value":"Carol","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	atoms, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(atoms) != 2 {
		t.Errorf("got %d atoms, want 2 (skipped empty entity/attribute)", len(atoms))
	}
}

func TestLoad_SkipsUnknownTypes(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	data := `{"entity":"user:1","attribute":"name","value":"Alice","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
{"entity":"user:2","attribute":"field","value":"Evil","type":"unknown_type","timestamp":"2024-01-01T00:00:00Z","version":1}
{"entity":"user:3","attribute":"name","value":"Bob","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	atoms, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(atoms) != 2 {
		t.Errorf("got %d atoms, want 2 (skipped unknown type)", len(atoms))
	}
}

func TestLoad_RejectsOversizedStrings(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	largeValue := make([]byte, 1048577)
	for i := range largeValue {
		largeValue[i] = 'x'
	}

	data := `{"entity":"user:1","attribute":"name","value":"Alice","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
{"entity":"user:2","attribute":"data","value":"` + string(largeValue) + `","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
{"entity":"user:3","attribute":"name","value":"Bob","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	atoms, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(atoms) != 2 {
		t.Errorf("got %d atoms, want 2 (skipped oversized value)", len(atoms))
	}
}

func TestLoad_AcceptsMaxSizeStrings(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	maxValue := make([]byte, 1048576)
	for i := range maxValue {
		maxValue[i] = 'x'
	}

	data := `{"entity":"user:1","attribute":"data","value":"` + string(maxValue) + `","type":"string","timestamp":"2024-01-01T00:00:00Z","version":1}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	atoms, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(atoms) != 1 {
		t.Errorf("got %d atoms, want 1 (max size should be allowed)", len(atoms))
	}
}

func TestSaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	a := &atom.Atom{
		Entity:    "user:1",
		Attribute: "name",
		Value:     "Test",
		Type:      "string",
	}

	if err := Save(a, file); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	file.Close()

	atoms, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(atoms) != 1 {
		t.Fatalf("got %d atoms, want 1", len(atoms))
	}

	if atoms[0].Entity != "user:1" || atoms[0].Value != "Test" {
		t.Errorf("atom mismatch: got %+v", atoms[0])
	}
}

func TestCompact(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	atoms := map[string]map[string]*atom.Atom{
		"user:1": {
			"name": {Entity: "user:1", Attribute: "name", Value: "Alice", Type: "string"},
		},
		"user:2": {
			"name": {Entity: "user:2", Attribute: "name", Value: "Bob", Type: "string"},
		},
	}

	err := Compact(path, atoms, true)
	if err != nil {
		t.Fatalf("Compact returned error: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load after compact returned error: %v", err)
	}

	if len(loaded) != 2 {
		t.Errorf("got %d atoms, want 2", len(loaded))
	}
}
