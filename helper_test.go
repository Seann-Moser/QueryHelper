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

func TestGetNonEmptyColumns(t *testing.T) {
	// Setup: Initialize the table with some columns
	table := &Table[any]{
		Columns: map[string]Column{
			"name":  {Name: "Name"},
			"age":   {Name: "Age"},
			"email": {Name: "Email"},
		},
	}

	// Input data containing keys that match some of the table's columns
	data := map[string]interface{}{
		"name":    "Alice",
		"age":     30,
		"address": "123 Main St", // 'address' is not in Columns
	}

	// Expected output: Only columns that are present in both data and table.Columns
	expectedColumns := []Column{
		{Name: "Name"},
		{Name: "Age"},
	}

	// Execution: Call the function under test
	resultColumns := table.GetNonEmptyColumns(data)

	// Verification: Check if the result matches the expected output
	if len(resultColumns) != len(expectedColumns) {
		t.Fatalf("Expected %d columns, got %d", len(expectedColumns), len(resultColumns))
	}

	// Convert slices to maps for easier comparison
	resultMap := make(map[string]Column)
	for _, col := range resultColumns {
		resultMap[col.Name] = col
	}

	expectedMap := make(map[string]Column)
	for _, col := range expectedColumns {
		expectedMap[col.Name] = col
	}

	if !reflect.DeepEqual(resultMap, expectedMap) {
		t.Errorf("Expected columns %v, got %v", expectedMap, resultMap)
	}
}

//func TestUpdateStatement(t *testing.T) {
//	// Scenario 1: Standard update with default updatable columns
//	table := &Table[any]{
//		Name: "my_schema.my_table",
//		Columns: map[string]Column{
//			"id": {
//				Name:           "id",
//				Primary:        true,
//				AutoGenerateID: false,
//				Update:         false,
//			},
//			"username": {
//				Name:           "username",
//				Primary:        false,
//				AutoGenerateID: false,
//				Update:         true,
//			},
//			"email": {
//				Name:           "email",
//				Primary:        false,
//				AutoGenerateID: false,
//				Update:         true,
//			},
//			"created_at": {
//				Name:           "created_at",
//				Primary:        false,
//				AutoGenerateID: false,
//				Update:         false,
//			},
//			"updated_at": {
//				Name:           "updated_at",
//				Primary:        false,
//				AutoGenerateID: false,
//				Update:         false,
//			},
//		},
//	}
//
//	expectedSQL := "UPDATE .my_schema.my_table SET username = :username ,email = :email WHERE id = :old_id"
//
//	// Call UpdateStatement without specifying updateColumns
//	sql := table.UpdateStatement()
//
//	if sql != expectedSQL {
//		t.Errorf("Scenario 1 Failed:\nExpected SQL:\n%s\nGot:\n%s", expectedSQL, sql)
//	}
//
//	// Scenario 2: Update with specific columns provided
//	updateColumns := []Column{
//		table.Columns["email"],
//	}
//
//	expectedSQL = "UPDATE .my_schema.my_table SET email = :email WHERE id = :old_id"
//
//	sql = table.UpdateStatement(updateColumns...)
//
//	if sql != expectedSQL {
//		t.Errorf("Scenario 2 Failed:\nExpected SQL:\n%s\nGot:\n%s", expectedSQL, sql)
//	}
//
//	// Scenario 3: No updatable columns
//	tableNoUpdate := &Table[any]{
//		Name: "my_schema.my_table2",
//		Columns: map[string]Column{
//			"id": {
//				Name:           "id",
//				Primary:        true,
//				AutoGenerateID: false,
//				Update:         false,
//			},
//			"created_at": {
//				Name:           "created_at",
//				Primary:        false,
//				AutoGenerateID: true,
//				Update:         false,
//			},
//			"updated_at": {
//				Name:           "updated_at",
//				Primary:        false,
//				AutoGenerateID: false,
//				Update:         false,
//			},
//		},
//	}
//
//	sql = tableNoUpdate.UpdateStatement()
//
//	if sql != "" {
//		t.Errorf("Scenario 3 Failed:\nExpected empty SQL, got:\n%s", sql)
//	}
//
//	// Scenario 4: No primary key or AutoGenerateID columns
//	tableNoWhere := &Table[any]{
//		Name: "my_schema.my_table3",
//		Columns: map[string]Column{
//			"username": {
//				Name:           "username",
//				Primary:        false,
//				AutoGenerateID: false,
//				Update:         true,
//			},
//			"email": {
//				Name:           "email",
//				Primary:        false,
//				AutoGenerateID: false,
//				Update:         true,
//			},
//		},
//	}
//
//	sql = tableNoWhere.UpdateStatement()
//
//	if sql != "" {
//		t.Errorf("Scenario 4 Failed:\nExpected empty SQL when no primary key or AutoGenerateID columns, got:\n%s", sql)
//	}
//}
