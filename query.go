package QueryHelper

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Query[T any] struct {
	SelectColumns []*Column
	FromTable     *Table[T]
	FromQuery     *Query[T]
	JoinStmt      []map[string]*Column
	WhereStmts    []*WhereStmt
	GroupByStmt   []*Column
	OrderByStmt   []*Column

	LimitCount int

	Query string
}

type WhereStmt struct {
	LeftValue   string
	Conditional string
	RightValue  interface{}
	Level       int
}

func generateWhere(whereStatments []*WhereStmt) string {

	return ""
}

func generateGroupBy(groupBy []*Column) string {

	return ""
}

func QueryTable[T any](table *Table[T]) *Query[T] {
	return &Query[T]{
		SelectColumns: []*Column{},
		FromTable:     table,
		FromQuery:     nil,
		JoinStmt:      make([]map[string]*Column, 0),
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

func (q *Query[T]) Join(tableColumns ...map[string]*Column) *Query[T] {
	q.JoinStmt = append(q.JoinStmt, tableColumns...)
	return q
}

func (q *Query[T]) Where(key string, conditional string, level int, value interface{}) *Query[T] {
	q.WhereStmts = append(q.WhereStmts, &WhereStmt{
		LeftValue:   key,
		Conditional: conditional,
		RightValue:  value,
		Level:       level,
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
	var query = ""
	selectColumns := q.FromTable.GetSelectableColumns(isGroupBy, isGroupBy, q.SelectColumns...)

	//whereStmt := q.FromTable.  (strings.ToUpper(conditional), keys...)
	// build sub query
	//gnereate group by

	//generate order by

	query = fmt.Sprintf(`SELECT %s FROM %s`, strings.Join(selectColumns, ","), q.FromTable.FullTableName())
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

func (q *Query[T]) Run(ctx context.Context, db *sqlx.DB, args ...interface{}) ([]*T, error) {
	if len(q.Query) == 0 {
		q.Build()
	}
	whereArgs := map[string]interface{}{}
	for _, where := range q.WhereStmts {
		if where.RightValue == nil {
			continue
		}
		whereArgs[where.LeftValue] = where.RightValue
	}
	return q.FromTable.NamedSelect(ctx, db, q.Query, append(args, whereArgs)...)
}
