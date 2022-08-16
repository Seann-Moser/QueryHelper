package table

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type Generator struct {
	db        *sqlx.DB
	logger    *zap.Logger
	dropTable bool
}

func NewGenerator(db *sqlx.DB, dropTable bool, logger *zap.Logger) *Generator {
	return &Generator{
		db:        db,
		logger:    logger,
		dropTable: dropTable,
	}
}

const (
	TableTypeInt     = "int"
	TableTypeFloat   = "int"
	TableTypeVarChar = "varchar(256)"
	TableTypeText    = "text"
	TableTypeBool    = "tinyint(1)"
	TableTime        = "timestamp"
)

func (g *Generator) qConfigParser(name, data string, p reflect.Type) (*Config, error) {
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
		case "primary", "join", "select", "update", "skip", "null":
			if value != "" {
				t, err := strconv.ParseBool(value)
				if err == nil {
					con[key] = t
				}
			} else {
				con[key] = true
			}
		case "where", "join_name", "data_type", "default", "where_join", "foreign_key", "foreign_table":
			if key == "data_type" {
				con["data_type"] = value
			}
			con[key] = strings.ReplaceAll(value, "{{comma}}", ",")
		}

	}
	b, err := json.Marshal(con)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	err = json.Unmarshal(b, config)
	if err != nil {
		return nil, err
	}
	return config, err
}
func (g *Generator) TableFromStruct(database string, s interface{}) (*Table, error) {
	var err error
	newTable := Table{
		Dataset:  database,
		Name:     getType(s),
		Elements: []*Config{},
	}

	structType := reflect.TypeOf(s)
	var setPrimary bool
	for i := 0; i < structType.NumField(); i++ {
		e := &Config{Select: true, Primary: false}
		name := structType.Field(i).Tag.Get("db")
		e.Name = name
		if e.Name == "" {
			e.Name = structType.Field(i).Name
		}
		if value := structType.Field(i).Tag.Get("q_config"); value != "" {
			// q_config:primary,update,select,join,join_name:name,data_type:var(128),skip,default:TIMESTAMP(),null
			e, err = g.qConfigParser(name, value, structType.Field(i).Type)
			if err != nil {
				g.logger.Error("failed parsing q_config", zap.String("q_config", value), zap.Error(err))
			}
			e.Name = name

		} else {
			e, err = g.qConfigParser(name, "", structType.Field(i).Type)
			e.Name = name
		}
		if e.Primary {
			setPrimary = true
		}
		newTable.Elements = append(newTable.Elements, e)
	}
	if !setPrimary {
		newTable.Elements[0].Primary = true
	}
	return &newTable, nil
}

func (g *Generator) CreateMySqlTable(ctx context.Context, t *Table) error {
	createSchema := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", t.Dataset)
	if g.db != nil {
		_, err := g.db.ExecContext(ctx, createSchema)
		if err != nil {
			return fmt.Errorf("err: %v, schema: %s", err, createSchema)
		}
	}
	var PrimaryKeys []string
	var FK []string
	var createString string
	if g.dropTable {
		createString = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;\n", t.Dataset, t.Name)
	}
	createString = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s(", t.Dataset, t.Name)

	for _, element := range t.Elements {
		elementString := fmt.Sprintf("\n\t%s %s", element.Name, element.Type)
		if !element.Null {
			elementString += " NOT NULL"
		}
		if element.Default != "" {
			elementString += fmt.Sprintf(" DEFAULT %s", element.Default)
		} else if element.Default == "" && element.Null {
			elementString += " DEFAULT NULL"
		}
		createString += elementString + ","
		if element.Primary {
			PrimaryKeys = append(PrimaryKeys, element.Name)
		}
		if len(element.ForeignKey) > 0 && len(element.ForeignTable) > 0 {
			FK = append(FK, fmt.Sprintf("\n\tFOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE on update cascade", element.Name, element.ForeignTable, element.ForeignKey))
		}
	}
	if len(PrimaryKeys) == 0 {
		createString += fmt.Sprintf("\n\tPRIMARY KEY(%s)", t.Elements[0].Name)
	} else if len(PrimaryKeys) == 1 {
		createString += fmt.Sprintf("\n\tPRIMARY KEY(%s)", PrimaryKeys[0])
	} else {
		createString += fmt.Sprintf("\n\tCONSTRAINT PK_%s_%s PRIMARY KEY (%s)", t.Dataset, t.Name, strings.Join(PrimaryKeys, ","))

	}
	if len(FK) > 0 {
		createString += "," + strings.Join(FK, ",")
	}
	createString += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	g.logger.Debug("creating table", zap.String("create_table", createString))
	if g.db != nil {
		_, err := g.db.ExecContext(ctx, createString)
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
