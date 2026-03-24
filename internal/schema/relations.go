package schema

import (
	"fmt"
	"sync"
)

// Relation defines a relationship between two types.
type Relation struct {
	FromType      string
	FromField     string
	ToType        string
	ToField       string
	Cardinality   string // one-to-one, one-to-many, many-to-many
	CascadeDelete bool   // delete related entities when parent is deleted
}

// RelationManager tracks all relationships between types.
type RelationManager struct {
	mu        sync.RWMutex
	relations []Relation
}

// NewRelationManager creates a new relation manager.
func NewRelationManager() *RelationManager {
	return &RelationManager{}
}

// AddRelation registers a relationship.
func (rm *RelationManager) AddRelation(r Relation) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Validate cardinality
	switch r.Cardinality {
	case "one-to-one", "one-to-many", "many-to-many":
		// valid
	default:
		return fmt.Errorf("invalid cardinality %q, must be one-to-one, one-to-many, or many-to-many", r.Cardinality)
	}

	rm.relations = append(rm.relations, r)
	return nil
}

// RemoveRelation removes a relationship.
func (rm *RelationManager) RemoveRelation(fromType, fromField string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for i, r := range rm.relations {
		if r.FromType == fromType && r.FromField == fromField {
			rm.relations = append(rm.relations[:i], rm.relations[i+1:]...)
			return
		}
	}
}

// GetRelations returns all relations from a given type.
func (rm *RelationManager) GetRelations(fromType string) []Relation {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var result []Relation
	for _, r := range rm.relations {
		if r.FromType == fromType {
			result = append(result, r)
		}
	}
	return result
}

// GetIncomingRelations returns all relations pointing to a given type.
func (rm *RelationManager) GetIncomingRelations(toType string) []Relation {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var result []Relation
	for _, r := range rm.relations {
		if r.ToType == toType {
			result = append(result, r)
		}
	}
	return result
}

// FindRelation finds a specific relation.
func (rm *RelationManager) FindRelation(fromType, fromField string) (Relation, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for _, r := range rm.relations {
		if r.FromType == fromType && r.FromField == fromField {
			return r, true
		}
	}
	return Relation{}, false
}

// Validate checks if all relations are valid.
func (rm *RelationManager) Validate(schema *Schema) error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for _, r := range rm.relations {
		// Check from type exists
		if !schema.HasType(r.FromType) {
			return fmt.Errorf("relation source type %q does not exist", r.FromType)
		}
		// Check to type exists
		if !schema.HasType(r.ToType) {
			return fmt.Errorf("relation target type %q does not exist", r.ToType)
		}
		// Check from field exists
		td, _ := schema.GetType(r.FromType)
		found := false
		for _, f := range td.Fields {
			if f.Name == r.FromField {
				if f.Type != TypeRef {
					return fmt.Errorf("relation field %q.%q must be type ref", r.FromType, r.FromField)
				}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("relation field %q.%q does not exist", r.FromType, r.FromField)
		}
	}
	return nil
}

// JoinResult represents a joined row from multiple types.
type JoinResult map[string]interface{}

// Join performs a join between two entity sets on matching ref fields.
func Join(
	leftEntities []map[string]interface{},
	rightEntities []map[string]interface{},
	leftField string,
	rightIDField string,
) []JoinResult {
	// Build index on right side
	rightIndex := make(map[string][]map[string]interface{})
	for _, re := range rightEntities {
		id := fmt.Sprintf("%v", re[rightIDField])
		rightIndex[id] = append(rightIndex[id], re)
	}

	var results []JoinResult
	for _, le := range leftEntities {
		refVal := fmt.Sprintf("%v", le[leftField])
		matches, ok := rightIndex[refVal]
		if !ok {
			// Left join - include with nil right side
			row := make(JoinResult)
			for k, v := range le {
				row["left."+k] = v
			}
			results = append(results, row)
			continue
		}
		for _, re := range matches {
			row := make(JoinResult)
			for k, v := range le {
				row["left."+k] = v
			}
			for k, v := range re {
				row["right."+k] = v
			}
			results = append(results, row)
		}
	}
	return results
}
