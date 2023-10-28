package QueryHelper

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type Query[T any] struct {
	SelectColumns []*Column
	FromTable     *Table[T]
	FromQuery     *Query[T]
	JoinStmt      []*JoinStmt
	WhereStmts    []*WhereStmt
	GroupByStmt   []*Column
	OrderByStmt   []*Column

	LimitCount int

	Query string
}

type JoinStmt struct {
	Columns  map[string]*Column
	JoinType string
}
type WhereStmt struct {
	LeftValue    *Column
	Conditional  string
	RightValue   interface{}
	Level        int
	JoinOperator string
}

func (w *WhereStmt) ToString() string {
	column := w.LeftValue

	tmp := column.Where
	if w.Conditional != "" {
		tmp = w.Conditional
	} else if tmp == "" {
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
		return formatted
	}
	return ""
}

func generateWhere(whereStatements []*WhereStmt) string {
	previousLevel := 0
	stmt := ""
	for i, w := range whereStatements {
		if where := w.ToString(); where != "" {

			if w.Level > previousLevel {
				stmt += fmt.Sprintf(" %s", generateList("(", w.Level-previousLevel))
			}
			if w.Level < previousLevel {
				stmt += fmt.Sprintf(" %s", generateList(")", previousLevel-w.Level))
			}

			if i > 0 {
				if w.JoinOperator == "" {
					w.JoinOperator = "AND"
				}
				stmt += " " + strings.ToUpper(w.JoinOperator)
			}

			stmt += fmt.Sprintf(" %s", w.ToString())
			previousLevel = w.Level

		}
	}
	if previousLevel > 0 {
		stmt += fmt.Sprintf(" %s", generateList(")", previousLevel))
	}
	return "WHERE " + stmt
}

func generateList(symbol string, count int) string {
	output := ""
	for i := 0; i < count; i++ {
		output += symbol
	}
	return output
}

func generateGroupBy(groupBy []*Column) string {
	var columns []string
	for _, c := range groupBy {
		columns = append(columns, c.FullName(false))
	}
	return "GROUP BY " + strings.Join(columns, ",")
}

func QueryTable[T any](table *Table[T]) *Query[T] {
	return &Query[T]{
		SelectColumns: []*Column{},
		FromTable:     table,
		FromQuery:     nil,
		JoinStmt:      make([]*JoinStmt, 0),
		WhereStmts:    make([]*WhereStmt, 0),
		GroupByStmt:   make([]*Column, 0),
		OrderByStmt:   make([]*Column, 0),
		Query:         "",
	}
}
func (q *Query[T]) Select(columns ...*Column) *Query[T] {
	q.SelectColumns = append(q.SelectColumns, columns...)
	return q
}

func (q *Query[T]) From(query *Query[T]) *Query[T] {
	q.FromQuery = query
	return q
}

func (q *Query[T]) Join(tableColumns map[string]*Column, joinType string) *Query[T] {
	q.JoinStmt = append(q.JoinStmt, &JoinStmt{
		Columns:  tableColumns,
		JoinType: joinType,
	})
	return q
}

func (q *Query[T]) Where(column *Column, conditional, joinOperator string, level int, value interface{}) *Query[T] {
	if level < 0 {
		level = 0
	}
	q.WhereStmts = append(q.WhereStmts, &WhereStmt{
		LeftValue:    column,
		Conditional:  conditional,
		RightValue:   value,
		Level:        level,
		JoinOperator: joinOperator,
	})

	return q
}

func (q *Query[T]) GroupBy(column ...*Column) *Query[T] {
	q.GroupByStmt = append(q.GroupByStmt, column...)
	return q
}

func (q *Query[T]) OrderBy(column ...*Column) *Query[T] {
	q.OrderByStmt = append(q.OrderByStmt, column...)
	return q
}

func (q *Query[T]) Limit(limit int) *Query[T] {
	q.LimitCount = limit
	return q
}

func (q *Query[T]) Build() *Query[T] {
	var isGroupBy = len(q.GroupByStmt) > 0
	var query string
	selectColumns := q.FromTable.GetSelectableColumns(isGroupBy, isGroupBy, q.SelectColumns...)

	if q.FromQuery != nil {
		q.FromQuery.Build()
		query = fmt.Sprintf("SELECT\n\t%s\nFROM\n\t(%s)", strings.Join(selectColumns, ",\n\t"), strings.ReplaceAll(q.FromQuery.Query, "\n", "\n\t"))

	} else {
		query = fmt.Sprintf("SELECT\n\t%s\nFROM\n\t%s", strings.Join(selectColumns, ",\n\t"), q.FromTable.FullTableName())

	}

	if len(q.JoinStmt) > 0 {
		for _, join := range q.JoinStmt {
			overlappingColumns := map[string]*Column{}
			overlappingColumns = JoinMaps[*Column](overlappingColumns, q.FromTable.GetCommonColumns(join.Columns))
			if len(overlappingColumns) == 0 {
				continue
			}
			query = fmt.Sprintf("%s\n%s", query, q.FromTable.generateJoinStmt(overlappingColumns, join.JoinType))
		}
	}

	if len(q.WhereStmts) > 0 {
		query = fmt.Sprintf("%s\n%s", query, generateWhere(q.WhereStmts))
	}

	if len(q.GroupByStmt) > 0 {
		query = fmt.Sprintf("%s\n%s", query, generateGroupBy(q.GroupByStmt))
	}

	if len(q.OrderByStmt) > 0 {
		query = fmt.Sprintf("%s\n%s", query, q.FromTable.OrderByColumns(q.OrderByStmt...))
	}

	if q.LimitCount > 0 {
		query = fmt.Sprintf("%s\nLIMIT %d;", query, q.LimitCount)
	}
	q.Query = query
	return q
}

func (q *Query[T]) Run(ctx context.Context, db DB, args ...interface{}) ([]*T, error) {
	if len(q.Query) == 0 {
		q.Build()
	}
	return q.FromTable.NamedSelect(ctx, db, q.Query, q.Args(args))
}

func (q *Query[T]) Args(args ...interface{}) map[string]interface{} {
	whereArgs := map[string]interface{}{}
	for _, where := range q.WhereStmts {
		if where.RightValue == nil {
			continue
		}
		whereArgs[where.LeftValue.Name] = where.RightValue
	}
	arg, err := combineStructs(append(args, whereArgs)...)
	if err != nil {
		return nil
	}
	return arg
}

func SelectQuery[T any, X any](ctx context.Context, db DB, q *Query[T], args ...interface{}) ([]*X, error) {
	if len(q.Query) == 0 {
		q.Build()
	}
	if db == nil {
		db = q.FromTable.db
	}
	rows, err := NamedQuery(ctx, db, q.Query, q.Args(args...))
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, sql.ErrNoRows
	}
	var output []*X
	for rows.Next() {
		var tmp X
		err := rows.StructScan(&tmp)
		if err != nil {
			return nil, err
		}
		output = append(output, &tmp)
	}
	return output, nil
}
