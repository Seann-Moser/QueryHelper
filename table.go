package main

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"
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
		field := structType.Field(i)
		name := field.Tag.Get(TagColumnNamePrefix)
		if name == "" {
			name = structType.Field(i).Name
		}

		if value := field.Tag.Get(TagConfigPrefix); value != "" {
			column, err = GetColumnFromTag(name, value, field.Type)
		} else {
			column, err = GetColumnFromTag(name, "", field.Type)
		}

		if err != nil {
			return nil, fmt.Errorf("failed parsing struct tag info(%s):%w",
				field.Tag.Get(TagConfigPrefix),
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

func (t *Table[T]) NamedSelect(ctx context.Context, db *sqlx.DB, query string, args ...interface{}) ([]*T, error) {
	rows, err := t.NamedQuery(ctx, db, query, args)
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

func (t *Table[T]) IsAutoGenerateID() bool {
	for _, e := range t.Columns {
		if e.AutoGenerateID {
			return true
		}
	}
	return false
}

func (t *Table[T]) GetGenerateID() []*Column {
	var output []*Column
	for _, e := range t.Columns {
		if e.AutoGenerateID {
			output = append(output, e)
		}
	}
	return output
}

func (t *Table[T]) GenerateID() map[string]string {
	m := map[string]string{}
	for _, e := range t.GetGenerateID() {
		uid := uuid.New().String()
		switch e.AutoGenerateIDType {
		case "hex":
			hasher := sha1.New()
			hasher.Write([]byte(uid))
			m[e.Name] = hex.EncodeToString(hasher.Sum(nil))
		case "base64":
			hasher := sha1.New()
			hasher.Write([]byte(uid))
			m[e.Name] = base64.URLEncoding.EncodeToString(hasher.Sum(nil))
		case "uuid":
			fallthrough
		default:
			m[e.Name] = uid
		}
	}
	return m
}

func (t *Table[T]) InsertStatement() string {
	var columnNames []string
	var values []string
	for _, e := range t.Columns {
		if e.Skip {
			continue
		}
		columnNames = append(columnNames, e.Name)
		values = append(values, ":"+e.Name)
	}
	if len(columnNames) == 0 {
		return ""
	}
	insert := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s);",
		t.FullTableName(),
		strings.Join(columnNames, ","), strings.Join(values, ","))
	return insert
}

func (t *Table[T]) UpdateStatement() string {
	var setValues []string
	var whereValues []string
	for _, e := range t.Columns {
		if e.Primary {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
		}
		if !e.Update {
			continue
		}
		setValues = append(setValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
	}
	if len(setValues) == 0 {
		return ""
	}
	update := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		t.FullTableName(),
		strings.Join(setValues, " ,"), strings.Join(whereValues, " AND "))
	return update
}

func (t *Table[T]) DeleteStatement() string {
	var whereValues []string
	for _, e := range t.Columns {
		if e.Primary {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
			continue
		}
		if e.Delete {
			return fmt.Sprintf("DELETE FROM %s WHERE %s = :%s", t.FullTableName(), e.Name, e.Name)
		}
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", t.FullTableName(), strings.Join(whereValues, " AND "))
}

func (t *Table[T]) CountStatement(conditional string, whereElementsStr ...string) string {
	wh := t.WhereStatement(conditional, whereElementsStr...)
	return fmt.Sprintf("SELECT COUNT(*) as count FROM %s %s", t.FullTableName(), wh)
}

func (t *Table[T]) NamedQuery(ctx context.Context, db *sqlx.DB, query string, args ...interface{}) (*sqlx.Rows, error) {
	a, err := combineStructs(args...)
	if err != nil {
		return nil, err
	}
	query = fixArrays(query, a)
	return db.NamedQueryContext(ctx, query, a)
}

func SelectJoinStmt[T any](baseTable Table[T], selectCol, whereElementsStr []string, joinTables ...Table[T]) string {
	validTables := []Table[T]{}
	var joinStmts []string
	whereValues := baseTable.WhereValues(whereElementsStr...)
	for _, currentTable := range joinTables {
		whereValues = append(whereValues, currentTable.WhereValues(whereElementsStr...)...)
		commonColumns, _ := FindCommonColumns(baseTable, currentTable)
		if len(commonColumns) == 0 {
			continue
		}
		validTables = append(validTables, currentTable)
		joinStmt := fmt.Sprintf(" JOIN %s ON %s", currentTable.FullTableName(), strings.Join(commonColumns, " AND "))
		joinStmts = append(joinStmts, joinStmt)
	}

	var selectValues []string
	dedupMap := map[string]bool{}
	for _, validTable := range validTables {
		if (len(selectCol) == 0) && len(selectValues) == 0 {
			selectValues = append(selectValues, validTable.GetSelectableColumns(true)...)
		} else {
			for _, e := range validTable.GetSelectableColumns(true) {
				for _, s := range selectCol {
					if _, found := dedupMap[e]; found {
						break
					}
					eleName := strings.TrimSpace(e[strings.Index(e, "AS")+2:])
					if _, found := dedupMap[eleName]; found {
						break
					}
					if strings.EqualFold(s, eleName) || strings.EqualFold(s, e) {
						selectValues = append(selectValues, e)
						dedupMap[eleName] = true
						dedupMap[e] = true
						break
					}
				}
			}

		}
	}
	var wv []string
	dedupWhereMap := map[string]bool{}
	for _, w := range whereValues {
		if _, found := dedupWhereMap[w]; found {
			continue
		}
		wv = append(wv, w)
		dedupWhereMap[w] = true
	}
	whereStmt := ""
	if len(wv) > 0 {
		whereStmt = fmt.Sprintf(" WHERE %s", strings.Join(wv, " AND "))
	}
	strings.Join(selectValues, ",")
	selectStmt := fmt.Sprintf("SELECT %s FROM %s %s %s", strings.Join(selectValues, ","), baseTable.FullTableName(), strings.Join(joinStmts, " "), whereStmt)
	return selectStmt
}

func (t *Table[T]) Insert(ctx context.Context, db *sqlx.DB, s T) (sql.Result, string, error) {
	if t.IsAutoGenerateID() {
		generateIds := t.GenerateID()
		args, err := combineStructs(generateIds, s)
		if err != nil {
			return nil, "", err
		}
		results, err := db.NamedExecContext(ctx, t.InsertStatement(), args)
		return results, generateIds[t.GetGenerateID()[0].Name], err
	}
	results, err := db.NamedExecContext(ctx, t.InsertStatement(), s)
	return results, "", err
}

func (t *Table[T]) Delete(ctx context.Context, db *sqlx.DB, s T) (sql.Result, error) {
	return db.NamedExecContext(ctx, t.DeleteStatement(), s)
}

func (t *Table[T]) Update(ctx context.Context, db *sqlx.DB, s T) (sql.Result, error) {
	return db.NamedExecContext(ctx, t.UpdateStatement(), s)
}

//func (t *Table[T]) SelectJoin(ctx context.Context, db *sqlx.DB, query string) {
//	query := SelectJoinStmt[interface{}](t,selectCol, whereStr, s...)
//	if err != nil {
//		return nil, err
//	}
//
//	rows, err := d.namedQuery(ctx, query, s...)
//}

func FindCommonColumns[T any](t1 Table[T], t2 Table[T]) ([]string, []string) {
	var joinArr []string
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
