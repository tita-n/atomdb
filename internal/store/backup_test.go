package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBackupAndRestore(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	// Insert data
	s.Set("user:1", "name", "Alice", "string")
	s.Set("user:1", "age", 30.0, "number")
	s.Set("user:2", "name", "Bob", "string")

	// Create backup
	backupPath := filepath.Join(t.TempDir(), "backup.bak")
	err := s.Backup(backupPath)
	if err != nil {
		t.Fatal(err)
	}

	// Verify backup file exists
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Fatal("backup file not created")
	}

	// Create new store and restore
	path2 := tempDB(t)
	s2, _ := New(path2)
	defer s2.Close()

	err = s2.Restore(backupPath)
	if err != nil {
		t.Fatal(err)
	}

	// Verify restored data
	a, ok := s2.Get("user:1", "name")
	if !ok || a.Value != "Alice" {
		t.Errorf("restored user:1.name = %v, %v; want Alice, true", a.Value, ok)
	}

	a, ok = s2.Get("user:1", "age")
	if !ok || a.Value != 30.0 {
		t.Errorf("restored user:1.age = %v, %v; want 30, true", a.Value, ok)
	}

	a, ok = s2.Get("user:2", "name")
	if !ok || a.Value != "Bob" {
		t.Errorf("restored user:2.name = %v, %v; want Bob, true", a.Value, ok)
	}

	stats := s2.Stats()
	if stats.EntityCount != 2 {
		t.Errorf("restored EntityCount = %d, want 2", stats.EntityCount)
	}
}

func TestBackupPreservesTypes(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	s.Set("item:1", "active", true, "boolean")
	s.Set("item:1", "count", 42.0, "number")

	backupPath := filepath.Join(t.TempDir(), "backup.bak")
	s.Backup(backupPath)

	path2 := tempDB(t)
	s2, _ := New(path2)
	defer s2.Close()
	s2.Restore(backupPath)

	a, ok := s2.Get("item:1", "active")
	if !ok {
		t.Fatal("active not found")
	}
	if a.Type != "boolean" {
		t.Errorf("active type = %s, want boolean", a.Type)
	}

	a, ok = s2.Get("item:1", "count")
	if !ok {
		t.Fatal("count not found")
	}
	if a.Type != "number" {
		t.Errorf("count type = %s, want number", a.Type)
	}
}

func TestBackupSkipsDeleted(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	s.Set("user:1", "name", "Alice", "string")
	s.Set("user:2", "name", "Bob", "string")
	s.Delete("user:2", "name")

	backupPath := filepath.Join(t.TempDir(), "backup.bak")
	s.Backup(backupPath)

	path2 := tempDB(t)
	s2, _ := New(path2)
	defer s2.Close()
	s2.Restore(backupPath)

	_, ok := s2.Get("user:2", "name")
	if ok {
		t.Error("deleted entity should not be in backup")
	}

	a, ok := s2.Get("user:1", "name")
	if !ok || a.Value != "Alice" {
		t.Errorf("non-deleted entity should be in backup: %v, %v", a.Value, ok)
	}
}
