package store

import (
	"fmt"
	"testing"
)

func TestTransactionCommit(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	err := s.WithTransaction(func(tx *Transaction) error {
		if err := tx.Set("user:1", "name", "Alice", "string"); err != nil {
			return err
		}
		if err := tx.Set("user:1", "age", 30.0, "number"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	a, ok := s.Get("user:1", "name")
	if !ok || a.Value != "Alice" {
		t.Errorf("name = %v, %v; want Alice, true", a.Value, ok)
	}

	a, ok = s.Get("user:1", "age")
	if !ok || a.Value != 30.0 {
		t.Errorf("age = %v, %v; want 30, true", a.Value, ok)
	}
}

func TestTransactionRollback(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	// Insert initial data
	s.Set("user:1", "name", "Alice", "string")

	// Transaction that fails
	err := s.WithTransaction(func(tx *Transaction) error {
		if err := tx.Set("user:1", "name", "Bob", "string"); err != nil {
			return err
		}
		return fmt.Errorf("simulated error")
	})
	if err == nil {
		t.Fatal("expected error")
	}

	// Verify rollback - original value should remain
	a, ok := s.Get("user:1", "name")
	if !ok || a.Value != "Alice" {
		t.Errorf("after rollback: name = %v, %v; want Alice, true", a.Value, ok)
	}
}

func TestTransactionDelete(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	s.Set("user:1", "name", "Alice", "string")
	s.Set("user:1", "age", 30.0, "number")

	err := s.WithTransaction(func(tx *Transaction) error {
		return tx.Delete("user:1", "age")
	})
	if err != nil {
		t.Fatal(err)
	}

	_, ok := s.Get("user:1", "age")
	if ok {
		t.Error("age should be deleted")
	}

	a, ok := s.Get("user:1", "name")
	if !ok || a.Value != "Alice" {
		t.Errorf("name should still exist: %v, %v", a.Value, ok)
	}
}

func TestTransactionMultipleSets(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	err := s.WithTransaction(func(tx *Transaction) error {
		for i := 0; i < 10; i++ {
			if err := tx.Set("user:"+string(rune('0'+i)), "name", "User "+string(rune('0'+i)), "string"); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	stats := s.Stats()
	if stats.EntityCount != 10 {
		t.Errorf("EntityCount = %d, want 10", stats.EntityCount)
	}
}
