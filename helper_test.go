package QueryHelper

import (
	"reflect"
	"testing"
)

func TestAddPrefix(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		input    map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name:   "Add prefix to each key",
			prefix: "pre_",
			input: map[string]interface{}{
				"key1": "value1",
				"key2": 2,
				"key3": true,
			},
			expected: map[string]interface{}{
				"pre_key1": "value1",
				"pre_key2": 2,
				"pre_key3": true,
			},
		},
		{
			name:     "Empty map input",
			prefix:   "pre_",
			input:    map[string]interface{}{},
			expected: map[string]interface{}{},
		},
		{
			name:   "No prefix added",
			prefix: "",
			input: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := AddPrefix(tt.prefix, tt.input)
			if !reflect.DeepEqual(output, tt.expected) {
				t.Errorf("AddPrefix() = %v, expected %v", output, tt.expected)
			}
		})
	}
}
