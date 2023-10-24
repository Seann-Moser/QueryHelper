package QueryHelper

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
)

const (
	TableTypeInt     = "int"
	TableTypeFloat   = "int"
	TableTypeVarChar = "varchar(256)"
	TableTypeText    = "text"
	TableTypeBool    = "tinyint(1)"
	TableTime        = "timestamp"

	SplitString = ";"
	EqualSplit  = "::"
)

func GetColumnFromTag(name, data string, p reflect.Type) (*Column, error) {
	dataPoints := strings.Split(data, SplitString)
	con := map[string]interface{}{}
	con["select"] = true
	con["data_type"] = convertTypeToSql(name, p)
	con["name"] = name
	for _, row := range dataPoints {
		v := strings.Split(row, EqualSplit)
		key := strings.TrimSpace(v[0])
		value := ""
		if len(v) > 1 {
			value = strings.TrimSpace(v[1])
		}
		switch strings.ToLower(key) {
		case "where", "join_name", "data_type", "default", "where_join", "foreign_key", "foreign_table", "auto_generate_id_type", "group_by_modifier":
			if key == "data_type" {
				con["data_type"] = value
			}
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

func getType(myVar interface{}) string {
	if t := reflect.TypeOf(myVar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

func convertTypeToSql(name string, v reflect.Type) string {
	if strings.Contains(name, "timestamp") {
		return "TIMESTAMP"
	}
	if strings.Contains(name, "date") {
		return "DATE"
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return TableTypeInt
	case reflect.String:
		return TableTypeVarChar
	case reflect.Float32, reflect.Float64:
		return TableTypeFloat
	case reflect.Bool:
		return TableTypeBool
	default:

		return TableTypeText
	}
}
