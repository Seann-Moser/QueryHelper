package QueryHelper

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	TableTypeTinyInt   = "TINYINT"
	TableTypeSmallInt  = "SMALLINT"
	TableTypeMediumInt = "MEDIUMINT"
	TableTypeInt       = "INT"
	TableTypeBigInt    = "BIGINT"
	TableTypeFloat     = "FLOAT"
	TableTypeDouble    = "DOUBLE"
	TableTypeDecimal   = "DECIMAL(10,2)"
	TableTypeChar      = "CHAR(1)"
	TableTypeVarChar   = "VARCHAR(256)"
	TableTypeText      = "TEXT"
	TableTypeBlob      = "BLOB"
	TableTypeJSON      = "JSON"
	TableTypeDate      = "DATE"
	TableTypeTime      = "TIME"
	TableTypeDateTime  = "DATETIME"
	TableTypeTimestamp = "TIMESTAMP"
	TableTypeYear      = "YEAR"
	TableTypeBool      = "BOOLEAN"

	SplitString = ";"
	EqualSplit  = "::"
)

// Define default byte sizes for types
var typeByteLength = map[string]int{
	"TINYINT":   1,
	"SMALLINT":  2,
	"MEDIUMINT": 3,
	"INT":       4,
	"BIGINT":    8,
	"FLOAT":     4,
	"DOUBLE":    8,
	"DECIMAL":   5,     // Approximate storage for DECIMAL(10,2); adjust as needed
	"CHAR":      1,     // CHAR(1) uses 1 byte per character
	"VARCHAR":   256,   // Default 256 bytes for VARCHAR(256)
	"TEXT":      65535, // Maximum size of TEXT in bytes
	"BLOB":      65535, // Maximum size of BLOB in bytes
	"JSON":      65535, // Approximate storage size for JSON
	"DATE":      3,
	"TIME":      3,
	"DATETIME":  8,
	"TIMESTAMP": 4,
	"YEAR":      1,
	"BOOLEAN":   1,
}

func GetColumnFromTag(name, data string, p reflect.Type) (*Column, error) {
	dataPoints := strings.Split(data, SplitString)
	con := map[string]interface{}{}
	con["select"] = true
	con["data_type"] = convertTypeToSql(name, p)
	con["name"] = ToSnakeCase(name)

	// Set 'null' to true for nullable types
	con["null"] = isNullableType(p)

	for _, row := range dataPoints {
		v := strings.Split(row, EqualSplit)
		key := strings.TrimSpace(v[0])
		value := ""
		if len(v) > 1 {
			value = strings.TrimSpace(v[1])
		}
		switch strings.ToLower(key) {
		case "where", "join_name", "data_type", "default", "where_join", "foreign_key", "foreign_table", "foreign_schema", "auto_generate_id_type", "group_by_modifier", "group_by_name", "charset":
			con[key] = value
		case "order_priority":
			v, err := strconv.ParseInt(value, 10, 64)
			if err == nil {
				con[key] = int(v)
			}
		default:
			if value != "" {
				t, err := strconv.ParseBool(value)
				if err == nil {
					con[key] = t
				}
			} else {
				con[key] = true
			}
		}
	}
	b, err := json.Marshal(con)
	if err != nil {
		return nil, err
	}
	config := &Column{}
	err = json.Unmarshal(b, config)
	if err != nil {
		return nil, err
	}
	return config, err
}

func isNullableType(v reflect.Type) bool {
	// Handle pointers as nullable types
	if v.Kind() == reflect.Ptr {
		return true
	}
	// Handle sql.Null* types as nullable
	if v == reflect.TypeOf(sql.NullString{}) ||
		v == reflect.TypeOf(sql.NullInt64{}) ||
		v == reflect.TypeOf(sql.NullFloat64{}) ||
		v == reflect.TypeOf(sql.NullBool{}) {
		return true
	}
	return false
}

func getType(myVar interface{}) string {
	if t := reflect.TypeOf(myVar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

func convertTypeToSql(name string, v reflect.Type) string {
	// Handle pointers by getting the element type
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// Handle sql.Null* types
	if v == reflect.TypeOf(sql.NullString{}) {
		return TableTypeVarChar
	}
	if v == reflect.TypeOf(sql.NullInt64{}) {
		return TableTypeBigInt
	}
	if v == reflect.TypeOf(sql.NullFloat64{}) {
		return TableTypeDouble
	}
	if v == reflect.TypeOf(sql.NullBool{}) {
		return TableTypeBool
	}

	// Handle time.Time
	if v == reflect.TypeOf(time.Time{}) {
		return TableTypeDateTime
	}
	if strings.Contains(name, "timestamp") {
		return TableTypeTimestamp
	}
	if strings.Contains(name, "date") {
		return TableTypeDateTime
	}

	// Handle slices and arrays (e.g., []byte)
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		if v.Elem().Kind() == reflect.Uint8 {
			return TableTypeBlob
		}
		return TableTypeText
	}

	switch v.Kind() {
	case reflect.Int8:
		return TableTypeTinyInt
	case reflect.Int16:
		return TableTypeSmallInt
	case reflect.Int32:
		return TableTypeInt
	case reflect.Int64:
		return TableTypeBigInt
	case reflect.Int:
		return TableTypeInt
	case reflect.Uint8:
		return TableTypeTinyInt + " UNSIGNED"
	case reflect.Uint16:
		return TableTypeSmallInt + " UNSIGNED"
	case reflect.Uint32:
		return TableTypeInt + " UNSIGNED"
	case reflect.Uint64:
		return TableTypeBigInt + " UNSIGNED"
	case reflect.Uint:
		return TableTypeInt + " UNSIGNED"
	case reflect.Float32:
		return TableTypeFloat
	case reflect.Float64:
		return TableTypeDouble
	case reflect.Bool:
		return TableTypeBool
	case reflect.String:
		return TableTypeVarChar
	case reflect.Struct:
		if v == reflect.TypeOf(time.Time{}) {
			return TableTypeDateTime
		}
		// Handle custom struct types as JSON
		return TableTypeJSON
	default:
		return TableTypeText
	}
}
