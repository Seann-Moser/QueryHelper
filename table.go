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
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type QueryType string

const (
	QueryTypeSQL      = "sql"
	QueryTypeFireBase = "firebase"
)
const (
	TagConfigPrefix     = "qc"
	TagColumnNamePrefix = "db"
)

var (
	NoOverlappingColumnsErr = errors.New("error: no overlapping columns found")
	MissingPrimaryKeyErr    = errors.New("no field was set as the primary key")
	matchFirstCap           = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap             = regexp.MustCompile("([a-z0-9])([A-Z])")
)

type Table[T any] struct {
	Dataset   string             `json:"dataset"`
	Name      string             `json:"name"`
	Columns   map[string]*Column `json:"columns"`
	QueryType QueryType          `json:"query_type"`
	db        DB
}

func NewTable[T any](databaseName string, queryType QueryType) (*Table[T], error) {
	var err error
	var s T
	newTable := Table[T]{
		Dataset:   databaseName,
		Name:      ToSnakeCase(getType(s)),
		Columns:   map[string]*Column{},
		QueryType: queryType,
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
		column.ColumnOrder = i
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

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func (t *Table[T]) GetColumn(name string) *Column {
	if column, found := t.Columns[ToSnakeCase(name)]; found {
		return column
	}
	return nil
}

func (t *Table[T]) InitializeTable(ctx context.Context, db DB, suffix ...string) error {
	if t.db == nil {
		t.db = db
	}
	if db == nil {
		return fmt.Errorf("no db set")
	}
	t.Name = strings.Join(append([]string{t.Name}, suffix...), "_")
	err := db.CreateTable(ctx, t.Dataset, t.Name, t.Columns)
	if err != nil {
		return err
	}
	return nil
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
			formatted = fmt.Sprintf("%s %s (:%s)", column.FullName(false), tmp, column.Name)
		default:
			formatted = fmt.Sprintf("%s %s :%s", column.FullName(false), tmp, column.Name)
		}
		if strings.Contains(formatted, ".") {
			whereValues = append(whereValues, formatted)
		}

	}
	return whereValues

}

func (t *Table[T]) Select(ctx context.Context, db DB, conditional string, groupBy bool, args ...interface{}) ([]*T, error) {
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(t.GetSelectableColumns(false, groupBy), ","), t.FullTableName())
	keys, err := getKeys(args...)
	if err != nil {
		return nil, err
	}

	if len(keys) > 0 {
		query = fmt.Sprintf("%s %s", query, t.WhereStatement(strings.ToUpper(conditional), keys...))
	}

	order := t.OrderByStatement(false)
	if len(order) > 0 {
		query = fmt.Sprintf("%s %s", query, order)
	}
	return t.NamedSelect(ctx, db, query, args...)
}

func (t *Table[T]) NamedSelect(ctx context.Context, db DB, query string, args ...interface{}) ([]*T, error) {
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

func (t *Table[T]) GetSelectableColumns(useAs bool, groupBy bool, names ...*Column) []string {
	var selectValues []string
	var suffix string
	if len(names) > 0 {
		for _, name := range names {
			if name == nil {
				continue
			}
			e, found := t.Columns[name.Name]
			if !found {
				continue
			}
			if useAs {
				suffix = fmt.Sprintf(" AS %s", e.Name)
			} else {
				suffix = ""
			}

			if e.Select {
				selectValues = append(selectValues, fmt.Sprintf("%s%s", e.FullName(groupBy), suffix))
			}
		}
		return selectValues
	}

	for _, e := range t.Columns {
		if useAs {
			suffix = fmt.Sprintf(" AS %s", e.Name)
		} else {
			suffix = ""
		}

		if e.Select {
			selectValues = append(selectValues, fmt.Sprintf("%s%s", e.FullName(groupBy), suffix))
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

func (t *Table[T]) OrderByStatement(groupBy bool, orderBy ...string) string {
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
		return columns[i].OrderPriority < columns[j].OrderPriority
	})
	for _, column := range columns {
		orderByValues = append(orderByValues, column.GetOrderStmt(groupBy))
	}

	return fmt.Sprintf("ORDER BY %s", strings.Join(orderByValues, ","))
}

func (t *Table[T]) OrderByColumns(groupBy bool, columns ...*Column) string {
	var orderByValues []string

	for _, column := range t.Columns {
		if column.Order {
			columns = append(columns, column)
		}
	}

	if len(columns) == 0 {
		return ""
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].OrderPriority < columns[j].OrderPriority
	})
	for _, column := range columns {
		orderByValues = append(orderByValues, column.GetOrderStmt(groupBy))
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

func (t *Table[T]) InsertStatement(amount int) string {
	var columnNames []string
	var values []string
	for _, e := range t.Columns {
		if e.Skip {
			continue
		}
		columnNames = append(columnNames, e.Name)
		values = append(values, e.Name)
	}
	if len(columnNames) == 0 {
		return ""
	}
	var rows []string
	for i := 0; i < amount; i++ {
		rows = append(rows, fmt.Sprintf("(%s)", strings.Join(ArrayWithPrefix(fmt.Sprintf(":%d_", i), values), ",")))
	}

	insert := fmt.Sprintf("INSERT INTO %s(%s) VALUES \n%s;",
		t.FullTableName(),
		strings.Join(columnNames, ","), strings.Join(rows, ",\n"))
	return insert
}

func ArrayWithPrefix(prefix string, list []string) []string {
	var rows []string
	for _, i := range list {
		rows = append(rows, fmt.Sprintf("%s%s", prefix, i))
	}
	return rows
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

func (t *Table[T]) NamedQuery(ctx context.Context, db DB, query string, args ...interface{}) (DBRow, error) {
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
	return db.QueryContext(ctx, query, a)
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
			return column.Name, true
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

func (t *Table[T]) SelectJoinStmt(JoinType string, orderBy []string, groupBy bool, tableColumns ...map[string]*Column) (string, error) {
	overlappingColumns := map[string]*Column{}
	allColumns := map[string]*Column{}
	for _, columns := range tableColumns {
		overlappingColumns = JoinMaps[*Column](overlappingColumns, t.GetCommonColumns(columns))
		allColumns = JoinMaps[*Column](allColumns, columns)
	}
	if len(overlappingColumns) == 0 {
		return "", NoOverlappingColumnsErr
	}
	joinStmt := t.generateJoinStmt(overlappingColumns, JoinType)
	whereStmt := t.generateWhereStmt(allColumns)
	columns := t.GetSelectableColumns(false, groupBy)
	selectStmt := fmt.Sprintf("SELECT %s FROM %s %s %s %s", strings.Join(columns, ","), t.FullTableName(), joinStmt, whereStmt, t.OrderByStatement(false, orderBy...))
	t.OrderByStatement(false)
	return selectStmt, nil
}

func (t *Table[T]) generateJoinStmt(columns map[string]*Column, JoinType string) string {
	if len(columns) == 0 {
		return ""
	}
	var joinStmts []string
	joinExp := "JOIN"
	switch strings.ToLower(JoinType) {
	case "left":
		joinExp = "LEFT JOIN"
	case "right":
		joinExp = "RIGHT JOIN"
	}
	for _, column := range columns {
		name, found := t.HasColumn(column)
		if !column.Join || !found {
			continue
		}

		joinStmt := fmt.Sprintf("%s %s ON %s.%s = %s.%s", joinExp, column.FullTableName(), column.Table, column.Name, t.Name, name)
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

func (t *Table[T]) Insert(ctx context.Context, db DB, s ...T) (string, error) {
	if db == nil {
		db = t.db
	}
	if db == nil {
		return "", nil
	}

	if t.IsAutoGenerateID() {
		generateIds := t.GenerateID()
		args := map[string]interface{}{}
		for rowIndex, i := range s {
			tmpArgs, err := combineStructs(generateIds, i)
			if err != nil {
				return "", err
			}
			tmpArgs = AddPrefix(fmt.Sprintf("%d_", rowIndex), tmpArgs)
			args, err = combineStructs(args, tmpArgs)
			if err != nil {
				return "", err
			}
		}

		err := db.ExecContext(ctx, t.InsertStatement(len(s)), args)
		return generateIds[t.GetGenerateID()[0].Name], err
	}
	args, err := combineStructsWithPrefix[T](s...)

	err = db.ExecContext(ctx, t.InsertStatement(len(s)), args)
	return "", err
}

func (t *Table[T]) InsertTx(ctx context.Context, db *sqlx.Tx, s ...T) (sql.Result, string, error) {
	if db == nil {
		return nil, "", nil
	}
	if t.IsAutoGenerateID() {
		generateIds := t.GenerateID()
		args, err := combineStructs(generateIds, s)
		if err != nil {
			return nil, "", err
		}
		results, err := db.NamedExecContext(ctx, t.InsertStatement(len(s)), args)
		return results, generateIds[t.GetGenerateID()[0].Name], err
	}
	results, err := db.NamedExecContext(ctx, t.InsertStatement(len(s)), s)
	return results, "", err
}

func (t *Table[T]) Delete(ctx context.Context, db DB, s T) error {
	if db == nil {
		db = t.db
	}
	if db == nil {
		return nil
	}
	return db.ExecContext(ctx, t.DeleteStatement(), s)
}

func (t *Table[T]) DeleteTx(ctx context.Context, db *sqlx.Tx, s T) (sql.Result, error) {
	if db == nil {
		return nil, nil
	}
	return db.NamedExecContext(ctx, t.DeleteStatement(), s)
}

func (t *Table[T]) Update(ctx context.Context, db DB, s T) error {
	if db == nil {
		db = t.db
	}
	if db == nil {
		return nil
	}
	return db.ExecContext(ctx, t.UpdateStatement(), s)
}

func (t *Table[T]) UpdateTx(ctx context.Context, db *sqlx.Tx, s T) (sql.Result, error) {
	if db == nil {
		return nil, nil
	}
	return db.NamedExecContext(ctx, t.UpdateStatement(), s)
}

func NamedQuery(ctx context.Context, db DB, query string, args ...interface{}) (DBRow, error) {
	if db == nil {
		return nil, nil
	}
	a, err := combineStructs(args...)
	if err != nil {
		return nil, err
	}
	query = fixArrays(query, a)
	return db.QueryContext(ctx, query, a)
}
