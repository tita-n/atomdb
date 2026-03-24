package store

import (
	"testing"
)

func TestUniqueConstraint(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	// Add unique constraint on email field
	s.AddConstraint(Constraint{
		Type:      ConstraintUnique,
		TypeName:  "user",
		FieldName: "email",
	})

	// First insert should work
	err := s.Set("user:1", "email", "alice@example.com", "string")
	if err != nil {
		t.Fatal(err)
	}

	// Second insert with same email should fail
	err = s.Set("user:2", "email", "alice@example.com", "string")
	if err == nil {
		t.Error("expected unique constraint violation")
	}

	// Different email should work
	err = s.Set("user:2", "email", "bob@example.com", "string")
	if err != nil {
		t.Fatal(err)
	}
}

func TestNotNullConstraint(t *testing.T) {
	path := tempDB(t)
	s, _ := New(path)
	defer s.Close()

	// Add not null constraint
	s.AddConstraint(Constraint{
		Type:      ConstraintNotNull,
		TypeName:  "user",
		FieldName: "name",
	})

	// Non-nil value should work
	err := s.Set("user:1", "name", "Alice", "string")
	if err != nil {
		t.Fatal(err)
	}
}

func TestConstraintManagerHasConstraint(t *testing.T) {
	cm := NewConstraintManager()

	cm.AddConstraint(Constraint{
		Type:      ConstraintUnique,
		TypeName:  "user",
		FieldName: "email",
	})

	if !cm.HasConstraint("user", "email", ConstraintUnique) {
		t.Error("should have unique constraint on user.email")
	}

	if cm.HasConstraint("user", "name", ConstraintUnique) {
		t.Error("should not have unique constraint on user.name")
	}
}

func TestConstraintManagerListConstraints(t *testing.T) {
	cm := NewConstraintManager()

	cm.AddConstraint(Constraint{Type: ConstraintUnique, TypeName: "user", FieldName: "email"})
	cm.AddConstraint(Constraint{Type: ConstraintNotNull, TypeName: "user", FieldName: "name"})
	cm.AddConstraint(Constraint{Type: ConstraintUnique, TypeName: "post", FieldName: "slug"})

	userConstraints := cm.ListConstraints("user")
	if len(userConstraints) != 2 {
		t.Errorf("user constraints = %d, want 2", len(userConstraints))
	}

	postConstraints := cm.ListConstraints("post")
	if len(postConstraints) != 1 {
		t.Errorf("post constraints = %d, want 1", len(postConstraints))
	}
}

func TestConstraintManagerTrackAndRemove(t *testing.T) {
	cm := NewConstraintManager()

	cm.AddConstraint(Constraint{
		Type:      ConstraintUnique,
		TypeName:  "user",
		FieldName: "email",
	})

	cm.TrackUnique("user", "email", "alice@example.com", "user:1")

	// Validate should fail for same value with different entity
	err := cm.Validate("user", "email", "alice@example.com", "user:2")
	if err == nil {
		t.Error("expected unique constraint violation")
	}

	// Validate should pass for same entity
	err = cm.Validate("user", "email", "alice@example.com", "user:1")
	if err != nil {
		t.Error("should allow same entity to keep same value")
	}

	// Remove and validate should pass
	cm.RemoveUnique("user", "email", "alice@example.com")
	err = cm.Validate("user", "email", "alice@example.com", "user:2")
	if err != nil {
		t.Error("should pass after removing unique tracking")
	}
}
