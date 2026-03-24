package store

import (
	"fmt"
	"sync"
)

// ConstraintType defines the type of constraint.
type ConstraintType int

const (
	ConstraintUnique ConstraintType = iota
	ConstraintNotNull
	ConstraintCheck
)

// Constraint represents a data integrity constraint.
type Constraint struct {
	Type      ConstraintType
	TypeName  string
	FieldName string
	CheckFn   func(value interface{}) bool // for CHECK constraints
}

// ConstraintManager tracks and enforces constraints.
type ConstraintManager struct {
	mu          sync.RWMutex
	constraints []Constraint
	uniqueIdx   map[string]map[string]string // typeName -> fieldValue -> entityID
}

// NewConstraintManager creates a new constraint manager.
func NewConstraintManager() *ConstraintManager {
	return &ConstraintManager{
		uniqueIdx: make(map[string]map[string]string),
	}
}

// AddConstraint registers a new constraint.
func (cm *ConstraintManager) AddConstraint(c Constraint) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.constraints = append(cm.constraints, c)

	// Initialize unique index if needed
	if c.Type == ConstraintUnique {
		key := c.TypeName + "." + c.FieldName
		if cm.uniqueIdx[key] == nil {
			cm.uniqueIdx[key] = make(map[string]string)
		}
	}
}

// RemoveConstraintsFor removes all constraints for a type.
func (cm *ConstraintManager) RemoveConstraintsFor(typeName string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var kept []Constraint
	for _, c := range cm.constraints {
		if c.TypeName != typeName {
			kept = append(kept, c)
		} else {
			key := c.TypeName + "." + c.FieldName
			delete(cm.uniqueIdx, key)
		}
	}
	cm.constraints = kept
}

// Validate checks if a value satisfies all constraints for a type/field.
func (cm *ConstraintManager) Validate(typeName, fieldName string, value interface{}, entityID string) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, c := range cm.constraints {
		if c.TypeName != typeName || c.FieldName != fieldName {
			continue
		}

		switch c.Type {
		case ConstraintUnique:
			key := typeName + "." + fieldName
			valStr := fmt.Sprintf("%v", value)
			if existing, ok := cm.uniqueIdx[key][valStr]; ok && existing != entityID {
				return fmt.Errorf("unique constraint violation: %s.%s = %v already exists on %s",
					typeName, fieldName, value, existing)
			}

		case ConstraintNotNull:
			if value == nil {
				return fmt.Errorf("not null constraint violation: %s.%s cannot be nil",
					typeName, fieldName)
			}

		case ConstraintCheck:
			if c.CheckFn != nil && !c.CheckFn(value) {
				return fmt.Errorf("check constraint violation: %s.%s = %v failed check",
					typeName, fieldName, value)
			}
		}
	}
	return nil
}

// TrackUnique records a unique value for an entity.
func (cm *ConstraintManager) TrackUnique(typeName, fieldName, value, entityID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := typeName + "." + fieldName
	if cm.uniqueIdx[key] == nil {
		cm.uniqueIdx[key] = make(map[string]string)
	}
	cm.uniqueIdx[key][value] = entityID
}

// RemoveUnique removes a unique value tracking.
func (cm *ConstraintManager) RemoveUnique(typeName, fieldName, value string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := typeName + "." + fieldName
	if cm.uniqueIdx[key] != nil {
		delete(cm.uniqueIdx[key], value)
	}
}

// HasConstraint checks if a constraint exists for a type/field.
func (cm *ConstraintManager) HasConstraint(typeName, fieldName string, ct ConstraintType) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, c := range cm.constraints {
		if c.TypeName == typeName && c.FieldName == fieldName && c.Type == ct {
			return true
		}
	}
	return false
}

// ListConstraints returns all constraints for a type.
func (cm *ConstraintManager) ListConstraints(typeName string) []Constraint {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var result []Constraint
	for _, c := range cm.constraints {
		if c.TypeName == typeName {
			result = append(result, c)
		}
	}
	return result
}
