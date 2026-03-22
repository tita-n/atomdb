package atom

import (
	"errors"
	"fmt"
	"time"
	"unicode"
)

const (
	MaxNameLength  = 1024
	MaxValueLength = 1048576
)

type Atom struct {
	Entity    string      `json:"entity"`
	Attribute string      `json:"attribute"`
	Value     interface{} `json:"value"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Version   int64       `json:"version"`
	NodeID    string      `json:"node_id,omitempty"`
}

func NewAtom(entity, attribute string, value interface{}, valueType string) (*Atom, error) {
	return NewAtomWithNode(entity, attribute, value, valueType, "")
}

func NewAtomWithNode(entity, attribute string, value interface{}, valueType, nodeID string) (*Atom, error) {
	if err := ValidateName(entity); err != nil {
		return nil, fmt.Errorf("invalid entity: %w", err)
	}
	if err := ValidateName(attribute); err != nil {
		return nil, fmt.Errorf("invalid attribute: %w", err)
	}
	if err := ValidateValue(value); err != nil {
		return nil, fmt.Errorf("invalid value: %w", err)
	}

	now := time.Now()
	return &Atom{
		Entity:    entity,
		Attribute: attribute,
		Value:     value,
		Type:      valueType,
		Timestamp: now,
		Version:   now.UnixNano(),
		NodeID:    nodeID,
	}, nil
}

func ValidateName(name string) error {
	if len(name) == 0 {
		return errors.New("name cannot be empty")
	}
	if len(name) > MaxNameLength {
		return fmt.Errorf("name exceeds maximum length of %d bytes", MaxNameLength)
	}
	for i, r := range name {
		if unicode.IsControl(r) && r != '\t' {
			return fmt.Errorf("name contains control character at position %d (U+%04X)", i, r)
		}
		if r == '\u2028' || r == '\u2029' {
			return fmt.Errorf("name contains Unicode line separator at position %d", i)
		}
	}
	return nil
}

func ValidateValue(value interface{}) error {
	if s, ok := value.(string); ok {
		if len(s) > MaxValueLength {
			return fmt.Errorf("string value exceeds maximum length of %d bytes", MaxValueLength)
		}
	}
	return nil
}

func (a *Atom) String() string {
	return fmt.Sprintf("%s.%s = %v (%s) [v%d @ %s]",
		a.Entity, a.Attribute, a.Value, a.Type, a.Version,
		a.Timestamp.Format(time.RFC3339Nano))
}
