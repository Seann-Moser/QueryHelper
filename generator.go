package QueryHelper

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"reflect"
	"strconv"
	"strings"
)

const (
	TableTypeInt     = "int"
	TableTypeFloat   = "int"
	TableTypeVarChar = "varchar(512)"
	TableTypeText    = "text"
	TableTypeBool    = "tinyint(1)"
	TableTime        = "timestamp"
)

func GenerateTableFromStruct(database string, s interface{}) (*Table, error) {
	var err error
	newTable := Table{
		Dataset:  database,
		Name:     getType(s),
		Elements: []*Elements{},
	}

	structType := reflect.TypeOf(s)
	for i := 0; i < structType.NumField(); i++ {
		e := Elements{}
		e.Name = structType.Field(i).Tag.Get("db")
		if e.Name == "" {
			e.Name = structType.Field(i).Name
		}
		if found := structType.Field(i).Tag.Get("where"); found != "" {
			e.Where = found
		}
		if found := structType.Field(i).Tag.Get("join_name"); found != "" {
			e.JoinName = found
		}
		if found := structType.Field(i).Tag.Get("data_type"); found != "" {
			e.Type = found
		} else {
			e.Type = convertTypeToSql(structType.Field(i).Type)
		}
		if found := structType.Field(i).Tag.Get("default"); found != "" {
			if e.Type == TableTypeText || e.Type == TableTypeVarChar {
				e.Default = fmt.Sprintf(`"%s"`, found)
			} else {
				e.Default = found
			}

		}
		if found := structType.Field(i).Tag.Get("can_be_null"); found != "" {
			e.CanBeNull, err = strconv.ParseBool(found)
			if err != nil {
				return nil, err
			}
		} else {
			e.CanBeNull = false
		}
		if found := structType.Field(i).Tag.Get("selectable"); found != "" {
			e.Selectable, err = strconv.ParseBool(found)
			if err != nil {
				return nil, err
			}
		} else {
			e.Selectable = true
		}
		if found := structType.Field(i).Tag.Get("joinable"); found != "" {
			e.Joinable, err = strconv.ParseBool(found)
			if err != nil {
				return nil, err
			}
		} else {
			e.Joinable = true
		}
		if found := structType.Field(i).Tag.Get("table"); found == "primary" {
			e.PrimaryKey = true
		} else {
			e.PrimaryKey = false
		}
		if found := structType.Field(i).Tag.Get("table"); found == "skip_insert" {
			e.SkipInsert = true
		} else {
			e.SkipInsert = false
		}
		if found := structType.Field(i).Tag.Get("can_update"); found != "" {
			e.CanUpdate, err = strconv.ParseBool(found)
			if err != nil {
				return nil, err
			}
			if e.PrimaryKey {
				e.CanUpdate = false
			}
		} else {
			e.CanUpdate = false
		}

		newTable.Elements = append(newTable.Elements, &e)
	}

	return &newTable, nil
}
func CreateMySqlTable(ctx context.Context, db *sqlx.DB, t *Table) error {
	createSchema := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", t.Dataset)
	_, err := db.ExecContext(ctx, createSchema)
	if err != nil {
		return fmt.Errorf("err: %v, schema: %s", err, createSchema)
	}
	PrimaryKeys := []string{}
	createString := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s(", t.Dataset, t.Name)
	for _, element := range t.Elements {
		elementString := fmt.Sprintf("\n\t%s %s", element.Name, element.Type)
		if !element.CanBeNull {
			elementString += " NOT NULL"
		}
		if element.Default != "" {
			elementString += fmt.Sprintf(" DEFAULT %s", element.Default)
		} else if element.Default == "" && element.CanBeNull {
			elementString += " DEFAULT NULL"
		}
		createString += elementString + ","
		if element.PrimaryKey {
			PrimaryKeys = append(PrimaryKeys, element.Name)
		}
	}
	if len(PrimaryKeys) == 0 {
		createString += fmt.Sprintf("\n\tPRIMARY KEY(%s)", t.Elements[0].Name)
	} else if len(PrimaryKeys) == 1 {
		createString += fmt.Sprintf("\n\tPRIMARY KEY(%s)", PrimaryKeys[0])
	} else {
		createString += fmt.Sprintf("\n\tCONSTRAINT PK_%s_%s PRIMARY KEY (%s)", t.Dataset, t.Name, strings.Join(PrimaryKeys, ","))

	}

	createString += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	if db != nil {
		_, err := db.ExecContext(ctx, createString)
		if err != nil {
			return fmt.Errorf("err: %v, table: %s", err, createString)
		}
	}
	return nil
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

func convertTypeToSql(v reflect.Type) string {
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
