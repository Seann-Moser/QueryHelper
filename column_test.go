package QueryHelper

import (
	"testing"
)

func TestGetByteLength(t *testing.T) {
	tests := []struct {
		column   Column
		expected int
	}{
		// Fixed-size type with default charset
		{Column{Type: "INT", Charset: "utf8"}, 4},
		{Column{Type: "BIGINT", Charset: "utf8"}, 8},

		// Fixed-size type with utf8mb4 charset (multiplies length by 4)
		{Column{Type: "INT", Charset: "utf8mb4"}, 16},
		{Column{Type: "BIGINT", Charset: "utf8mb4"}, 32},

		// Variable-length type with specified length (CHAR)
		{Column{Type: "CHAR(1)", Charset: "utf8"}, 1},
		{Column{Type: "CHAR(1)", Charset: "utf8mb4"}, 4},

		// Variable-length type with specified length (VARCHAR)
		{Column{Type: "VARCHAR(128)", Charset: "utf8"}, 128},
		{Column{Type: "VARCHAR(128)", Charset: "utf8mb4"}, 512},

		// Variable-length type with no specified length (uses default 256)
		{Column{Type: "VARCHAR", Charset: "utf8"}, 256},
		{Column{Type: "VARCHAR", Charset: "utf8mb4"}, 1024},

		// Unrecognized type should return 0
		{Column{Type: "UNKNOWN", Charset: "utf8"}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.column.Type+"_"+tt.column.Charset, func(t *testing.T) {
			if got := tt.column.GetByteLength(); got != tt.expected {
				t.Errorf("GetByteLength() = %d, expected %d", got, tt.expected)
			}
		})
	}
}
