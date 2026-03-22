package index

import (
	"testing"

	"github.com/tita-n/atomdb/internal/atom"
)

func TestIndexManagerBasic(t *testing.T) {
	im := NewIndexManager()

	a1, _ := atom.NewAtom("user:1", "name", "Ayo", "string")
	a2, _ := atom.NewAtom("user:2", "name", "Bob", "string")
	a3, _ := atom.NewAtom("user:1", "age", 28.0, "number")

	im.IndexAtom(a1)
	im.IndexAtom(a2)
	im.IndexAtom(a3)

	// Exact string search
	results := im.Search("name", "Ayo")
	if len(results) != 1 || results[0] != "user:1.name" {
		t.Errorf("Search(name, Ayo) = %v, want [user:1.name]", results)
	}

	// Exact number search
	results = im.Search("age", "28")
	if len(results) != 1 || results[0] != "user:1.age" {
		t.Errorf("Search(age, 28) = %v, want [user:1.age]", results)
	}
}

func TestIndexManagerRangeSearch(t *testing.T) {
	im := NewIndexManager()

	im.IndexAtom(mustAtom("user:1", "age", 9.0, "number"))
	im.IndexAtom(mustAtom("user:2", "age", 25.0, "number"))
	im.IndexAtom(mustAtom("user:3", "age", 100.0, "number"))

	results := im.RangeSearch("age", OpGt, "20")
	if len(results) != 2 {
		t.Errorf("RangeSearch(age, GT, 20) = %d results, want 2: %v", len(results), results)
	}

	results = im.RangeSearch("age", OpLt, "30")
	if len(results) != 2 {
		t.Errorf("RangeSearch(age, LT, 30) = %d results, want 2: %v", len(results), results)
	}
}

func TestIndexManagerRemoveAtom(t *testing.T) {
	im := NewIndexManager()

	a := mustAtom("user:1", "name", "Ayo", "string")
	im.IndexAtom(a)
	im.RemoveAtom(a)

	results := im.Search("name", "Ayo")
	if len(results) != 0 {
		t.Errorf("after RemoveAtom: Search = %v, want empty", results)
	}
}

func TestFullTextSearch(t *testing.T) {
	im := NewIndexManager()

	a := mustAtom("user:1", "name", "Ayo Adeleke", "string")
	im.IndexAtom(a)

	results := im.FullTextSearch("name", "Ayo")
	if len(results) != 1 || results[0] != "user:1.name" {
		t.Errorf("FullTextSearch(name, Ayo) = %v, want [user:1.name]", results)
	}

	results = im.FullTextSearch("name", "the")
	if len(results) != 0 {
		t.Errorf("stop word 'the' should return no results, got %v", results)
	}
}

func TestFullTextSearchMultiple(t *testing.T) {
	im := NewIndexManager()

	im.IndexAtom(mustAtom("doc:1", "body", "The quick brown fox", "string"))
	im.IndexAtom(mustAtom("doc:2", "body", "The lazy brown dog", "string"))

	results := im.FullTextSearch("body", "brown")
	if len(results) != 2 {
		t.Errorf("FullTextSearch(body, brown) = %d results, want 2", len(results))
	}

	results = im.FullTextSearch("body", "quick")
	if len(results) != 1 {
		t.Errorf("FullTextSearch(body, quick) = %d results, want 1", len(results))
	}
}

func TestRebuildFromAtoms(t *testing.T) {
	im := NewIndexManager()

	atoms := map[string]map[string]*atom.Atom{
		"user:1": {
			"name": mustAtomP("user:1", "name", "Ayo", "string"),
			"age":  mustAtomP("user:1", "age", 28.0, "number"),
		},
		"user:2": {
			"name": mustAtomP("user:2", "name", "Bob", "string"),
		},
	}

	im.RebuildFromAtoms(atoms)

	results := im.Search("name", "Ayo")
	if len(results) != 1 {
		t.Errorf("after rebuild: Search(name, Ayo) = %v, want 1 result", results)
	}

	attrs := im.IndexedAttributes()
	if len(attrs) != 2 {
		t.Errorf("IndexedAttributes = %v, want 2 attributes", attrs)
	}
}

func mustAtom(entity, attribute string, value interface{}, valueType string) *atom.Atom {
	a, _ := atom.NewAtom(entity, attribute, value, valueType)
	return a
}

func mustAtomP(entity, attribute string, value interface{}, valueType string) *atom.Atom {
	a, _ := atom.NewAtom(entity, attribute, value, valueType)
	return a
}
