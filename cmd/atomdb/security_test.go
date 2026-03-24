package main

import (
	"testing"
)

func TestSanitizeError_ControlChars(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "normal message",
			input: "normal error message",
			want:  "normal error message",
		},
		{
			name:  "with newline stripped",
			input: "error\x0Awith newline",
			want:  "errorwith newline",
		},
		{
			name:  "with carriage return stripped",
			input: "error\x0Dwith crlf",
			want:  "errorwith crlf",
		},
		{
			name:  "with tab preserved",
			input: "error\x09with tab",
			want:  "error\x09with tab",
		},
		{
			name:  "with other control chars stripped",
			input: "error\x01\x02\x03chars",
			want:  "errorchars",
		},
		{
			name:  "ANSI escape code stripped",
			input: "error\x1b[31mmessage\x1b[0m",
			want:  "error[31mmessage[0m",
		},
		{
			name:  "with DEL char stripped",
			input: "error\x7Fchar",
			want:  "errorchar",
		},
		{
			name:  "truncation",
			input: "this is a very long error message that definitely exceeds two hundred characters and should be truncated because the maximum allowed is two hundred characters plus ellipsis which adds three more characters to the end of the string making it even longer than expected and should trigger the truncation mechanism correctly",
			want:  "this is a very long error message that definitely exceeds two hundred characters and should be truncated because the maximum allowed is two hundred characters plus ellipsis which adds three more chara...",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := &testError{msg: tc.input}
			got := sanitizeError(err)
			if got != tc.want {
				t.Errorf("sanitizeError() = %q, want %q", got, tc.want)
			}
		})
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestSanitizePath_ValidPaths(t *testing.T) {
	validPaths := []string{
		"data.db",
		"mydb.db",
		"path/to/data.db",
		"data/subdir/data.db",
		"underscore_data.db",
		"data-with-dashes.db",
		"data.with.dots.db",
		"./relative/path.db",
	}

	for _, p := range validPaths {
		_, err := sanitizePath(p)
		if err != nil {
			t.Errorf("sanitizePath(%q) returned error: %v", p, err)
		}
	}
}

func TestSanitizePath_InvalidPaths(t *testing.T) {
	invalidPaths := []struct {
		path string
	}{
		{"path/../escape"},
		{"../escape"},
		{"path/../../escape"},
		{"../other/path.db"},
	}

	for _, tc := range invalidPaths {
		t.Run(tc.path, func(t *testing.T) {
			_, err := sanitizePath(tc.path)
			if err == nil {
				t.Errorf("sanitizePath(%q) should have returned error", tc.path)
			}
		})
	}
}

func TestValidateDirPath(t *testing.T) {
	validPaths := []string{
		"data",
		"data/db",
		"subdir/data",
		".",
		"./relative",
	}

	for _, p := range validPaths {
		err := validateDirPath(p)
		if err != nil {
			t.Errorf("validateDirPath(%q) returned error: %v", p, err)
		}
	}
}
