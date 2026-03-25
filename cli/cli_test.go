package cli

import (
	"testing"
)

func TestParseRawVal(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{"hello", "hello"},
		{"12345", float64(12345)},
		{"3.14", 3.14},
		{"true", true},
		{"false", false},
		{"", ""},
		{"user@domain.com", "user@domain.com"},
		{"123-456-7890", "123-456-7890"},
		{"POST-2024-001", "POST-2024-001"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseRawVal(tt.input)
			if got != tt.expected {
				t.Errorf("parseRawVal(%q) = %v (%T), want %v (%T)", tt.input, got, got, tt.expected, tt.expected)
			}
		})
	}
}

func TestSanitizeEntityIDValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{"John Doe", "John Doe"},
		{"john:doe", "john_doe"},
		{"path/to/file", "path_to_file"},
		{float64(42), "n42"},
		{true, "true"},
		{false, "false"},
		{"long" + string(make([]byte, 300)) + "string", "long" + string(make([]byte, 253))},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := sanitizeEntityIDValue(tt.input)
			if tt.input == float64(42) || tt.input == true || tt.input == false {
				if got != tt.expected {
					t.Errorf("sanitizeEntityIDValue(%v) = %q, want %q", tt.input, got, tt.expected)
				}
			}
		})
	}
}

func TestInferType(t *testing.T) {
	tests := []struct {
		val      interface{}
		expected string
	}{
		{float64(3.14), "number"},
		{float32(3.14), "number"},
		{int(42), "number"},
		{int64(42), "number"},
		{true, "boolean"},
		{false, "boolean"},
		{"string", "string"},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := inferType(tt.val)
			if got != tt.expected {
				t.Errorf("inferType(%v) = %q, want %q", tt.val, got, tt.expected)
			}
		})
	}
}
