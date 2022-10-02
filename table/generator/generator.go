package generator

import (
	"context"
	"fmt"
	"github.com/Seann-Moser/QueryHelper/table/dataset_table"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type Generator struct {
	db        *sqlx.DB
	logger    *zap.Logger
	dropTable bool
}

func New(db *sqlx.DB, dropTable bool, logger *zap.Logger) *Generator {
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

func (g *Generator) Table(database string, s interface{}) (dataset_table.Tables, error) {
	var err error
	newTable := dataset_table.Table{
		Dataset:  database,
		Name:     getType(s),
		Elements: []*dataset_table.Config{},
	}

	structType := reflect.TypeOf(s)
	var setPrimary bool
	for i := 0; i < structType.NumField(); i++ {
		e := &dataset_table.Config{Select: true, Primary: false}
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
		if e.Name == "-" {
			g.logger.Debug("skipping", zap.String("field", structType.Field(i).Name))
			continue
		}
		newTable.Elements = append(newTable.Elements, e)
	}
	if !setPrimary {
		newTable.Elements[0].Primary = true
	}
	return &newTable, nil
}

func (g *Generator) MySqlTable(ctx context.Context, t dataset_table.Tables) error {
	createSchema := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", t.GetDataset())
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
		createString = fmt.Sprintf("DROP TABLE IF EXISTS %s;\n", t.FullTableName())
	}
	createString += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(", t.FullTableName())

	for _, element := range t.GetElements() {
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
		createString += fmt.Sprintf("\n\tPRIMARY KEY(%s)", t.GetElements()[0].Name)
	} else if len(PrimaryKeys) == 1 {
		createString += fmt.Sprintf("\n\tPRIMARY KEY(%s)", PrimaryKeys[0])
	} else {
		createString += fmt.Sprintf("\n\tCONSTRAINT PK_%s_%s PRIMARY KEY (%s)", t.GetDataset(), t.GetTableName(), strings.Join(PrimaryKeys, ","))

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
