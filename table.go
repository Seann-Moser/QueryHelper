package main

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

const (
	TagConfigPrefix     = "qc"
	TagColumnNamePrefix = "db"
)

type Table[T any] struct {
	Dataset string             `json:"dataset"`
	Name    string             `json:"name"`
	Columns map[string]*Column `json:"columns"`
}

func NewTable[T any](databaseName string) (*Table[T], error) {
	var err error
	var s T
	newTable := Table[T]{
		Dataset: databaseName,
		Name:    getType(s),
		Columns: map[string]*Column{},
	}

	structType := reflect.TypeOf(s)
	var setPrimary bool
	for i := 0; i < structType.NumField(); i++ {
		var column *Column
		name := structType.Field(i).Tag.Get(TagColumnNamePrefix)
		if name == "" {
			name = structType.Field(i).Name
		}

		if value := structType.Field(i).Tag.Get(TagConfigPrefix); value != "" {
			column, err = GetColumnFromTag(column.Name, value, structType.Field(i).Type)
		} else {
			column, err = GetColumnFromTag(name, "", structType.Field(i).Type)
		}

		if err != nil {
			return nil, fmt.Errorf("failed parsing struct tag info(%s):%w",
				structType.Field(i).Tag.Get(TagConfigPrefix),
				err)
		}

		if column.Primary {
			setPrimary = true
		}
		if column.Name == "-" {
			continue
		}
		newTable.Columns[column.Name] = column
	}
	if !setPrimary {
		return nil, fmt.Errorf("no field was set as the primary key")
	}
	return &newTable, nil
}

func (t *Table[T]) CreateMySqlTableStatement(dropTable bool) ([]string, error) {
	createSchemaStatement := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", t.Dataset)
	var PrimaryKeys []string
	var FK []string
	createStatement := ""
	if dropTable {
		createStatement += fmt.Sprintf("DROP TABLE IF EXISTS %s;\n", t.FullTableName())
	}
	createStatement += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(", t.FullTableName())

	for _, column := range t.Columns {
		createStatement += column.GetDefinition() + ","
		if column.HasFK() {
			FK = append(FK, column.GetFK())
		}
		if column.Primary {
			PrimaryKeys = append(PrimaryKeys, column.Name)
		}
	}
	if len(PrimaryKeys) == 0 {
		return nil, fmt.Errorf("missing primary key")
	} else if len(PrimaryKeys) == 1 {
		createStatement += fmt.Sprintf("\n\tPRIMARY KEY(%s)", PrimaryKeys[0])
	} else {
		createStatement += fmt.Sprintf("\n\tCONSTRAINT PK_%s_%s PRIMARY KEY (%s)", t.Dataset, t.Name, strings.Join(PrimaryKeys, ","))

	}
	if len(FK) > 0 {
		createStatement += "," + strings.Join(FK, ",")
	}
	createStatement += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	return []string{createSchemaStatement, createStatement}, nil
}

func (t *Table[T]) FullTableName() string {
	return fmt.Sprintf("%s.%s", t.Dataset, t.Name)
}

func (t *Table[T]) FullColumnName(e *Column) string {
	return fmt.Sprintf("%s.%s", t.Name, e.Name)
}

func (t *Table[T]) WhereValues(whereElementsStr ...string) []string {
	var whereValues []string
	for _, i := range whereElementsStr {
		column, found := t.Columns[i]
		if !found {
			continue
		}

		tmp := column.Where
		if column.Where == "" {
			tmp = "="
		}
		var formatted string
		switch strings.TrimSpace(strings.ToLower(tmp)) {
		case "not in":
			fallthrough
		case "in":
			formatted = fmt.Sprintf("%s %s (:%s)", t.FullColumnName(column), tmp, column.Name)
		default:
			formatted = fmt.Sprintf("%s %s :%s", t.FullColumnName(column), tmp, column.Name)
		}
		if strings.Contains(formatted, ".") {
			whereValues = append(whereValues, formatted)
		}

	}
	return whereValues

}

func (t *Table[T]) GetColumns(ctx context.Context, db *sqlx.DB) ([]*sql.ColumnType, error) {
	if db == nil {
		return nil, nil
	}
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

func (t *Table[T]) GetUpdateStmt(add bool, columnList ...*Column) string {
	var output []string
	for _, column := range columnList {
		output = append(output, column.GetUpdateStmt(add))
	}
	return fmt.Sprintf("ALTER TABLE %s %s;", t.FullTableName(), strings.Join(output, ","))
}

func (t *Table[T]) UpdateTable(ctx context.Context, db *sqlx.DB) error {
	cols, err := t.GetColumns(ctx, db)
	if err != nil {
		return err
	}

	var addColumns []*Column
	var removeColumns []*Column
	colMap := map[string]*sql.ColumnType{}
	for _, c := range cols {
		colMap[c.Name()] = c
	}

	for _, e := range t.Columns {
		if _, found := colMap[e.Name]; !found {
			addColumns = append(addColumns, e)
		}
	}

	for _, c := range cols {
		if foundColumn, found := t.Columns[c.Name()]; found {
			removeColumns = append(removeColumns, foundColumn)
		}
	}

	if len(addColumns) > 0 {
		addStmt := t.GetUpdateStmt(true, addColumns...)
		if db != nil {
			_, err = db.ExecContext(ctx, addStmt)
			if err != nil {
				return err
			}
		}
	}
	if len(removeColumns) > 0 {
		removeStmt := t.GetUpdateStmt(false, removeColumns...)
		if db != nil {
			_, err = db.ExecContext(ctx, removeStmt)
			if err != nil {
				return err
			}
		}

	}
	if cols, err := t.GetColumns(ctx, db); err != nil || len(cols) != len(t.Columns) {
		return fmt.Errorf("update was not successful, columns are different than struct fields: %w", err)
	}
	return nil
}

func (t *Table[T]) NamedSelect(ctx context.Context, db *sqlx.DB, query string, args interface{}) ([]*T, error) {
	rows, err := db.NamedQueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, sql.ErrNoRows
	}
	var output []*T
	for rows.Next() {
		var tmp T
		err := rows.StructScan(&tmp)
		if err != nil {
			return nil, err
		}
		output = append(output, &tmp)
	}
	return output, nil
}

func (t *Table[T]) GetSelectableColumns(useAs bool) []string {
	var selectValues []string
	var suffix string
	for _, e := range t.Columns {
		if useAs {
			suffix = fmt.Sprintf("AS %s", e.Name)
		} else {
			suffix = ""
		}
		if e.Select {
			selectValues = append(selectValues, fmt.Sprintf("%s %s", t.FullColumnName(e), suffix))
		}
	}
	return selectValues
}

func (t *Table[T]) WhereStatement(conditional string, whereElementsStr ...string) string {
	whereValues := t.WhereValues(whereElementsStr...)
	if len(whereValues) == 0 {
		return ""
	}
	if conditional == "" {
		conditional = "AND"
	}
	return fmt.Sprintf("WHERE %s", strings.Join(whereValues, fmt.Sprintf(" %s ", conditional)))
}

func (t *Table[T]) OrderByStatement(orderBy ...string) string {
	var orderByValues []string
	for _, o := range orderBy {
		if v, found := t.Columns[o]; found {
			orderByValues = append(orderByValues, v.GetOrderStmt())
		}
	}
	if len(orderByValues) == 0 {
		return ""
	}
	return fmt.Sprintf("ORDER BY %s", strings.Join(orderByValues, ","))
}

func FindCommonColumns[T1 any, T2 any](t1 Table[T1], t2 Table[T2]) ([]string, []string) {
	joinArr := []string{}
	var whereValues []string
	addedWhereValues := map[string]bool{}
	for _, e := range t1.Columns {
		columnName := e.Name
		if e.JoinName != "" {
			columnName = e.JoinName
		}
		if !e.Join {
			continue
		}
		for _, e2 := range t2.Columns {
			e2ColName := e2.Name
			if e2.JoinName != "" {
				e2ColName = e2.JoinName
			}
			if strings.EqualFold(e2ColName, columnName) && e2.Join {
				joinArr = append(joinArr, fmt.Sprintf("%s = %s",
					t2.FullColumnName(e2),
					t1.FullColumnName(e),
				))
			} else {
				if _, found := addedWhereValues[e2.Name]; !found && len(e2.Where) > 0 && !e2.Join {
					addedWhereValues[e2.Name] = true
					whereValues = append(whereValues, fmt.Sprintf("%s %s :%s", t2.FullColumnName(e2), e2.Where, e2.Name))
				}
			}
		}
	}
	return joinArr, whereValues
}
