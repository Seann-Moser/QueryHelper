package generator

import (
	"encoding/json"
	"github.com/Seann-Moser/QueryHelper/table/dataset_table"
	"reflect"
	"strconv"
	"strings"
)

func (g *Generator) qConfigParser(name, data string, p reflect.Type) (*dataset_table.Config, error) {
	dataPoints := strings.Split(data, ",")
	con := map[string]interface{}{}
	con["select"] = true
	con["data_type"] = convertTypeToSql(name, p)
	for _, row := range dataPoints {
		v := strings.Split(row, ":")
		key := strings.TrimSpace(v[0])
		value := ""
		if len(v) > 1 {
			value = strings.TrimSpace(v[1])
		}
		switch strings.ToLower(key) {
		case "primary", "join", "select", "update", "skip", "null", "delete", "order_acs":
			if value != "" {
				t, err := strconv.ParseBool(value)
				if err == nil {
					con[key] = t
				}
			} else {
				con[key] = true
			}
		case "where", "join_name", "data_type", "default", "where_join", "foreign_key", "foreign_table", "order":
			if key == "data_type" {
				con["data_type"] = value
			}
			con[key] = strings.ReplaceAll(value, "{{comma}}", ";")
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
	config := &dataset_table.Config{}
	err = json.Unmarshal(b, config)
	if err != nil {
		return nil, err
	}
	return config, err
}

func convertTypeToSql(name string, v reflect.Type) string {
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
		if strings.Contains(name, "timestamp") {
			return "TIMESTAMP"
		}
		if strings.Contains(name, "date") {
			return "DATE"
		}
		return TableTypeText
	}
}
