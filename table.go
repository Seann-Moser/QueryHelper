package QueryHelper

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

const (
	TagConfigPrefix     = "qc"
	TagColumnNamePrefix = "db"
)

var (
	NoOverlappingColumnsErr = errors.New("error: no overlapping columns found")
	MissingPrimaryKeyErr    = errors.New("no field was set as the primary key")
)

type Table[T any] struct {
	Dataset string             `json:"dataset"`
	Name    string             `json:"name"`
	Columns map[string]*Column `json:"columns"`

	db *sqlx.DB
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
		column.Table = newTable.Name
		column.Dataset = databaseName

		if column.Primary {
			setPrimary = true
		}
		if column.Name == "-" {
			continue
		}
		newTable.Columns[column.Name] = column
	}
	if !setPrimary {
		return nil, MissingPrimaryKeyErr
	}
	return &newTable, nil
}

func (t *Table[T]) InitializeTable(ctx context.Context, db *sqlx.DB, dropTable, updateTable bool) error {
	stmts, err := t.CreateMySqlTableStatement(dropTable)
	if err != nil {
		return err
	}
	if db == nil {
		return nil
	}
	for _, stmt := range stmts {
		_, err = db.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
	}
	t.db = db
	if !updateTable {
		return nil
	}
	return t.UpdateTable(ctx, db)
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
		return nil, MissingPrimaryKeyErr
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
			formatted = fmt.Sprintf("%s %s (:%s)", column.FullName(), tmp, column.Name)
		default:
			formatted = fmt.Sprintf("%s %s :%s", column.FullName(), tmp, column.Name)
		}
		if strings.Contains(formatted, ".") {
			whereValues = append(whereValues, formatted)
		}

	}
	return whereValues

}

func (t *Table[T]) GetColumns(ctx context.Context, db *sqlx.DB) ([]*sql.ColumnType, error) {
	if db == nil {
		db = t.db
	}
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
	if db == nil {
		db = t.db
	}
	if db == nil {
		return nil
	}
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

func (t *Table[T]) Select(ctx context.Context, db *sqlx.DB, args ...interface{}) ([]*T, error) {
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(t.GetSelectableColumns(false), ","), t.FullTableName())

	values := WhereValues(t.Columns, false)
	if len(values) > 0 {
		query = fmt.Sprintf("%s WHERE %s", query, strings.Join(values, " AND "))
	}

	order := t.OrderByStatement()
	if len(order) > 0 {
		query = fmt.Sprintf("%s %s", query, order)
	}
	return t.NamedSelect(ctx, db, query, args...)
}

func (t *Table[T]) NamedSelect(ctx context.Context, db *sqlx.DB, query string, args ...interface{}) ([]*T, error) {
	if db == nil {
		db = t.db
	}
	if db == nil {
		return nil, nil
	}
	rows, err := t.NamedQuery(ctx, db, query, args...)
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
			selectValues = append(selectValues, fmt.Sprintf("%s %s", e.FullName(), suffix))
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

	var columns []*Column
	for _, o := range orderBy {
		if v, found := t.Columns[o]; found {
			columns = append(columns, v)
			//orderByValues = append(orderByValues, v.GetOrderStmt())
		}
	}
	if len(orderBy) == 0 {
		for _, column := range t.Columns {
			if column.Order {
				columns = append(columns, column)
			}
		}
	}
	if len(columns) == 0 {
		return ""
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].OrderPriority > columns[j].OrderPriority
	})
	for _, column := range columns {
		orderByValues = append(orderByValues, column.GetOrderStmt())
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
	if db == nil {
		db = t.db
	}
	if db == nil {
		return nil, nil
	}
	a, err := combineStructs(args...)
	if err != nil {
		return nil, err
	}
	query = fixArrays(query, a)
	return db.NamedQueryContext(ctx, query, a)
}

func (t *Table[T]) HasColumn(c *Column) (string, bool) {
	if !c.Join && !c.Select && c.WhereJoin == "" {
		return "", false
	}
	if c.Table == t.Name && c.Dataset == t.Dataset {
		return "", false
	}
	for _, column := range t.Columns {
		if c.JoinName == column.JoinName && (len(c.JoinName) > 0 && len(column.JoinName) > 0) {
			return c.JoinName, true
		}
		if c.JoinName == column.Name && len(c.JoinName) > 0 {
			return column.Name, true
		}
		if c.Name == column.Name {
			return column.Name, true
		}
		if c.Name == column.JoinName && len(column.JoinName) > 0 {
			return column.JoinName, true
		}
	}
	return "", false
}

func (t *Table[T]) GetCommonColumns(columns map[string]*Column) map[string]*Column {
	overlappingColumns := map[string]*Column{}
	for k, column := range columns {
		if _, found := t.HasColumn(column); found {
			overlappingColumns[k] = column
		}
	}
	return overlappingColumns
}

func (t *Table[T]) SelectJoinStmt(orderBy []string, tableColumns ...map[string]*Column) (string, error) {
	overlappingColumns := map[string]*Column{}
	allColumns := map[string]*Column{}
	for _, columns := range tableColumns {
		overlappingColumns = JoinMaps[*Column](overlappingColumns, t.GetCommonColumns(columns))
		allColumns = JoinMaps[*Column](allColumns, columns)
	}
	if len(overlappingColumns) == 0 {
		return "", NoOverlappingColumnsErr
	}
	joinStmt := t.generateJoinStmt(overlappingColumns)
	whereStmt := t.generateWhereStmt(allColumns)
	columns := t.GetSelectableColumns(false)
	selectStmt := fmt.Sprintf("SELECT %s FROM %s %s %s %s", strings.Join(columns, ","), t.FullTableName(), joinStmt, whereStmt, t.OrderByStatement(orderBy...))
	t.OrderByStatement()
	return selectStmt, nil
}

func (t *Table[T]) generateJoinStmt(columns map[string]*Column) string {
	if len(columns) == 0 {
		return ""
	}
	var joinStmts []string
	for _, column := range columns {
		name, found := t.HasColumn(column)
		if !column.Join || !found {
			continue
		}

		joinStmt := fmt.Sprintf(" JOIN %s ON %s.%s = %s.%s", column.FullTableName(), column.Table, column.Name, t.Name, name)
		joinStmts = append(joinStmts, joinStmt)
	}
	return strings.Join(joinStmts, " ")
}

func (t *Table[T]) generateWhereStmt(columns map[string]*Column) string {
	if len(columns) == 0 {
		return ""
	}
	stmts := WhereValues(columns, true)
	if len(stmts) == 0 {
		return ""
	}
	return fmt.Sprintf(" WHERE %s", strings.Join(stmts, " AND "))
}

func (t *Table[T]) Insert(ctx context.Context, db *sqlx.DB, s T) (sql.Result, string, error) {
	if db == nil {
		db = t.db
	}
	if db == nil {
		return nil, "", nil
	}
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
	if db == nil {
		db = t.db
	}
	if db == nil {
		return nil, nil
	}
	return db.NamedExecContext(ctx, t.DeleteStatement(), s)
}

func (t *Table[T]) Update(ctx context.Context, db *sqlx.DB, s T) (sql.Result, error) {
	if db == nil {
		db = t.db
	}
	if db == nil {
		return nil, nil
	}
	return db.NamedExecContext(ctx, t.UpdateStatement(), s)
}
