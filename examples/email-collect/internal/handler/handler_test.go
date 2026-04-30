package handler

import (
	"testing"
	"github.com/Populurs/taskcore/model"
)

func TestUnique(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "no duplicates",
			input:    []string{"a@example.com", "b@example.com"},
			expected: []string{"a@example.com", "b@example.com"},
		},
		{
			name:     "with duplicates",
			input:    []string{"a@example.com", "b@example.com", "a@example.com"},
			expected: []string{"a@example.com", "b@example.com"},
		},
		{
			name:     "empty",
			input:    []string{},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := unique(tt.input)

			if len(result) != len(tt.expected) {
				t.Fatalf("length mismatch: got %d, want %d", len(result), len(tt.expected))
			}

			for i := range tt.expected {
				if result[i] != tt.expected[i] {
					t.Errorf("element mismatch at index %d: got %s, want %s", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestValidateEmail(t *testing.T) {
	tests := []struct {
		email    string
		expected bool
	}{
		{"test@example.com", true},
		{"test.user+tag@example.com", true},
		{"test@example.co.uk", true},
		{"invalid-email", false},
		{"test@", false},
		{"test.example.com", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.email, func(t *testing.T) {
			result := validateEmail(tt.email)

			if result != tt.expected {
				t.Errorf("validateEmail(%s) = %v, want %v", tt.email, result, tt.expected)
			}
		})
	}
}

func TestEmailTaskOptions(t *testing.T) {
	tests := []struct {
		name     string
		base     map[string]any
		module   map[string]any
		expected model.EmailTaskOptions
	}{
		{
			name: "default values",
			expected: model.EmailTaskOptions{
				MinConfidence: 0.8,
				MaxEmails:     0,
			},
		},
		{
			name: "base configuration",
			base: map[string]any{
				"min_confidence": 0.9,
				"max_emails":     100,
			},
			expected: model.EmailTaskOptions{
				MinConfidence: 0.9,
				MaxEmails:     100,
			},
		},
		{
			name: "module overrides base",
			base: map[string]any{
				"min_confidence": 0.9,
			},
			module: map[string]any{
				"min_confidence": 0.7,
			},
			expected: model.EmailTaskOptions{
				MinConfidence: 0.7,
				MaxEmails:     0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := model.ApplyEmailOptions(tt.base, tt.module)

			if result.MinConfidence != tt.expected.MinConfidence {
				t.Errorf("MinConfidence: got %f, want %f", result.MinConfidence, tt.expected.MinConfidence)
			}
			if result.MaxEmails != tt.expected.MaxEmails {
				t.Errorf("MaxEmails: got %d, want %d", result.MaxEmails, tt.expected.MaxEmails)
			}
		})
	}
}