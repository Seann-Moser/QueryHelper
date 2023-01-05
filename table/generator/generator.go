package generator

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"

	"github.com/Seann-Moser/QueryHelper/table/dataset_table"

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

func (g *Generator) ColumnUpdater(ctx context.Context, db *sqlx.DB, t dataset_table.Table) error {
	cols, err := getColumns(ctx, db, t)
	if err != nil {
		return err
	}
	var addColumns []*dataset_table.Element
	var removeColumns []*dataset_table.Element
	colMap := map[string]*sql.ColumnType{}
	for _, c := range cols {
		colMap[c.Name()] = c
	}

	for _, e := range t.GetElements() {
		if _, found := colMap[e.Name]; !found {
			addColumns = append(addColumns, e)
		}
	}

	for _, c := range cols {
		if e := t.FindElementWithName(c.Name()); e == nil {
			removeColumns = append(removeColumns, e)
		}
	}

	alterTable := fmt.Sprintf("ALTER TABLE %s ;", t.FullTableName())

	if len(addColumns) > 0 {
		addStmt := generateColumnStatements(alterTable, "add", addColumns)
		g.logger.Debug("adding columns to table", zap.String("table", t.FullTableName()), zap.String("query", addStmt))
		_, err := db.ExecContext(ctx, addStmt)
		if err != nil {
			return err
		}
	}
	if len(removeColumns) > 0 {
		removeStmt := generateColumnStatements(alterTable, "remove", removeColumns)
		g.logger.Debug("removing columns from table", zap.String("table", t.FullTableName()), zap.String("query", removeStmt))
		_, err := db.ExecContext(ctx, removeStmt)
		if err != nil {
			return err
		}

	}
	t.SelectStatement("")
	return nil
}

func getColumns(ctx context.Context, db *sqlx.DB, t dataset_table.Table) ([]*sql.ColumnType, error) {
	rows, err := db.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM %s limit 1;", t.FullTableName()))
	if err != nil {
		return nil, err
	}
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	return cols, nil
}

func generateColumnStatements(alterTable, columnType string, e []*dataset_table.Element) string {
	output := []string{}
	for _, el := range e {
		output = append(output, generateColumnStmt(columnType, el))
	}
	return fmt.Sprintf("%s %s;", alterTable, strings.Join(output, ","))

}
func generateColumnStmt(columnType string, e *dataset_table.Element) string {
	switch strings.ToLower(columnType) {
	case "drop":
		return fmt.Sprintf("DROP COLUMN %s;", e.Name)
	case "add":
		return fmt.Sprintf("ADD %s", e.GetColumnDef())
	}
	return ""
}

func (g *Generator) MySqlTable(t dataset_table.Table) (string, string) {
	createSchemaStatement := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", t.GetDataset())
	var PrimaryKeys []string
	var FK []string
	createStatement := ""
	if g.dropTable {
		createStatement += fmt.Sprintf("DROP TABLE IF EXISTS %s;\n", t.FullTableName())
	}
	createStatement += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(", t.FullTableName())

	for _, element := range t.GetElements() {
		createStatement += element.GetColumnDef() + ","
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
	createStatement += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	return createSchemaStatement, createStatement
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}
