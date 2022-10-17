package generator

import (
	"fmt"
	"github.com/Seann-Moser/QueryHelper/table/dataset_table"
	"reflect"
	"strings"

	"go.uber.org/zap"
)

type Generator struct {
	logger    *zap.Logger
	dropTable bool
}

func New(dropTable bool, logger *zap.Logger) *Generator {
	return &Generator{
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

func (g *Generator) Table(database string, s interface{}) (dataset_table.Table, error) {
	var err error
	newTable := dataset_table.DefaultTable{
		Dataset:  database,
		Name:     getType(s),
		Elements: []*dataset_table.Element{},
	}

	structType := reflect.TypeOf(s)
	var setPrimary bool
	for i := 0; i < structType.NumField(); i++ {
		e := &dataset_table.Element{Select: true, Primary: false}
		name := structType.Field(i).Tag.Get("db")
		e.Name = name
		if e.Name == "" {
			e.Name = structType.Field(i).Name
		}

		if value := structType.Field(i).Tag.Get("q_config"); value != "" {
			// q_config:primary,update,select,join,join_name:name,data_type:var(128),skip,default:TIMESTAMP(),null
			e, err = g.qConfigParser(name, value, structType.Field(i).Type)
			if err != nil {
				g.logger.Warn("failed parsing q_config", zap.String("q_config", value), zap.Error(err))
			}
			e.Name = name

		} else {
			e, err = g.qConfigParser(name, "", structType.Field(i).Type)
			if err != nil {
				g.logger.Warn("failed parsing q_config", zap.String("q_config", value), zap.Error(err))
			}
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

func (g *Generator) MySqlTable(t dataset_table.Table) (string, string) {
	createSchemaStatement := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n", t.GetDataset())
	var PrimaryKeys []string
	var FK []string
	createStatement := ""
	if g.dropTable {
		createStatement += fmt.Sprintf("DROP TABLE IF EXISTS %s;\n", t.FullTableName())
	}
	createStatement += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(", t.FullTableName())

	for _, element := range t.GetElements() {
		elementString := fmt.Sprintf("\n\t%s %s", element.Name, element.Type)
		if !element.Null {
			elementString += " NOT NULL"
		}
		switch element.Default {
		case "created_timestamp":
			elementString += " DEFAULT NOW()"
		case "updated_timestamp":
			elementString += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
		case "":
			if element.Null {
				elementString += " DEFAULT NULL"
			}
		default:
			elementString += fmt.Sprintf(" DEFAULT %s", element.Default)
		}
		createStatement += elementString + ","
		if element.Primary {
			PrimaryKeys = append(PrimaryKeys, element.Name)
		}
		if len(element.ForeignKey) > 0 && len(element.ForeignTable) > 0 {
			FK = append(FK, fmt.Sprintf("\n\tFOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE on update cascade", element.Name, element.ForeignTable, element.ForeignKey))
		}
	}
	if len(PrimaryKeys) == 0 {
		createStatement += fmt.Sprintf("\n\tPRIMARY KEY(%s)", t.GetElements()[0].Name)
	} else if len(PrimaryKeys) == 1 {
		createStatement += fmt.Sprintf("\n\tPRIMARY KEY(%s)", PrimaryKeys[0])
	} else {
		createStatement += fmt.Sprintf("\n\tCONSTRAINT PK_%s_%s PRIMARY KEY (%s)", t.GetDataset(), t.GetTableName(), strings.Join(PrimaryKeys, ","))

	}
	if len(FK) > 0 {
		createStatement += "," + strings.Join(FK, ",")
	}
	createStatement += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	return createSchemaStatement, createStatement
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}
