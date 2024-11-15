package QueryHelper

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/Seann-Moser/go-serve/pkg/ctxLogger"
	"go.uber.org/zap"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Seann-Moser/ctx_cache"
	"go.opentelemetry.io/otel"
)

var QueryPrepare = map[string]string{}

type Query[T any] struct {
	Name                  string
	err                   error
	SelectColumns         []Column
	DistinctSelectColumns []Column
	FromTable             *Table[T]
	FromQuery             *Query[T]
	JoinStmt              []*JoinStmt
	WhereStmts            []*WhereStmt
	GroupByStmt           []Column
	OrderByStmt           []Column
	MapKeyColumns         []Column
	LimitCount            int
	NoLock                bool
	ReadPast              bool

	Cache         ctx_cache.Cache
	useCache      bool
	refreshCache  bool
	Query         string
	skipCache     bool
	CacheDuration time.Duration
	WhereColumns  map[string]int
	cansave       bool
	Pagination    struct {
		Limit              int
		Offset             int
		PreviousPageColumn Column
		PreviewColumnValue interface{}
	}

	tmpPrefix string
}

type JoinStmt struct {
	Columns  map[string]Column
	JoinType string
}

func GetQuery[T any](ctx context.Context) *Query[T] {
	table, err := GetTableCtx[T](ctx)
	if err != nil {
		return &Query[T]{err: err}
	}
	q := QueryTable[T](table)
	return q
}

func generateGroupBy(groupBy []Column) string {
	var columns []string
	for _, c := range groupBy {
		if c.Name == "" {
			return ""
		}
		if c.SelectAs == "" {
			if c.GroupByName != "" {
				columns = append(columns, c.GroupByName)
			} else {
				columns = append(columns, c.Name)
			}
		} else {
			columns = append(columns, c.SelectAs)
		}

	}
	return "GROUP BY " + strings.Join(columns, ",")
}

func QueryTable[T any](table *Table[T]) *Query[T] {
	return &Query[T]{
		Name:                  "",
		err:                   nil,
		SelectColumns:         []Column{},
		DistinctSelectColumns: []Column{},
		FromTable:             table,
		FromQuery:             nil,
		JoinStmt:              make([]*JoinStmt, 0),
		WhereStmts:            make([]*WhereStmt, 0),
		GroupByStmt:           make([]Column, 0),
		OrderByStmt:           make([]Column, 0),
		MapKeyColumns:         make([]Column, 0),
		WhereColumns:          map[string]int{},
		CacheDuration:         0,
		LimitCount:            0,
		Cache:                 nil,
		Query:                 "",
		skipCache:             false,
		cansave:               true,
	}
}

func (q *Query[T]) Select(columns ...Column) *Query[T] {
	for _, c := range columns {
		if c.Name == "" {
			continue
		}
		q.SelectColumns = append(q.SelectColumns, c)
	}
	return q
}

func (q *Query[T]) SkipCache() *Query[T] {
	q.skipCache = true
	return q
}

func (q *Query[T]) From(query *Query[T]) *Query[T] {
	q.FromQuery = query
	return q
}
func (q *Query[T]) hasSaved() bool {
	if q.Name == "" {
		return false
	}
	_, f := QueryPrepare[q.Name]
	return f
}

func (q *Query[T]) canSave() bool {
	if q.Name == "" && !q.cansave {
		return false
	}
	_, f := QueryPrepare[q.Name]
	return f
}

func (q *Query[T]) Column(name string) Column {
	if q.err != nil {
		return Column{}
	}
	if q.hasSaved() {
		return Column{}
	}
	c := q.FromTable.GetColumn(name)
	if c.Name == "" {
		q.err = fmt.Errorf("missing column from table(%s) %s", q.FromTable.FullTableName(), name)
	}
	return c
}

func (q *Query[T]) Join(tableColumns map[string]Column, joinType string) *Query[T] {
	q.JoinStmt = append(q.JoinStmt, &JoinStmt{
		Columns:  tableColumns,
		JoinType: joinType,
	})
	return q
}

func (q *Query[T]) JoinColumn(joinType string, tableColumns Column) *Query[T] {
	q.JoinStmt = append(q.JoinStmt, &JoinStmt{
		Columns:  map[string]Column{tableColumns.Name: tableColumns},
		JoinType: joinType,
	})
	return q
}

func (q *Query[T]) EnableNoLock() *Query[T] {
	q.NoLock = true
	q.ReadPast = false
	return q
}

func (q *Query[T]) EnableReadPast() *Query[T] {
	q.NoLock = false
	q.ReadPast = true
	return q
}

func (q *Query[T]) MapColumns(column ...Column) *Query[T] {
	if column == nil {
		return q
	}
	q.MapKeyColumns = append(q.MapKeyColumns, column...)
	return q
}
func (q *Query[T]) UniqueWhere(column Column, conditional, joinOperator string, level int, value interface{}, flip bool) *Query[T] {
	if level < 0 {
		level = 0
	}
	if column.Name == "" {
		return q
	}
	if strings.Contains(conditional, "in") {
		q.cansave = false
	}
	stmt := &WhereStmt{
		LeftValue:    column,
		Conditional:  conditional,
		RightValue:   value,
		Level:        level,
		JoinOperator: joinOperator,
		Flip:         flip,
	}
	if _, found := q.WhereColumns[column.FullTableName()]; !found {
		q.WhereColumns[column.FullTableName()] = 0
	} else {
		q.WhereColumns[column.FullTableName()]++
	}
	stmt.Index = q.WhereColumns[column.FullTableName()]
	q.WhereStmts = append(q.WhereStmts, stmt)
	return q
}

func (q *Query[T]) Where(column Column, conditional, joinOperator string, level int, value interface{}) *Query[T] {
	if level < 0 {
		level = 0
	}
	if strings.Contains(conditional, "in") {
		q.cansave = false
	}
	if q.hasSaved() {
		return q
	}
	if column.Name == "" {
		return q
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

func (q *Query[T]) W(column Column, conditional string, value interface{}) *Query[T] {
	if column.Name == "" {
		return q
	}
	if q.hasSaved() {
		return q
	}
	if strings.Contains(conditional, "in") {
		q.cansave = false
	}
	q.WhereStmts = append(q.WhereStmts, &WhereStmt{
		LeftValue:    column,
		Conditional:  conditional,
		RightValue:   value,
		Level:        0,
		JoinOperator: "AND",
	})

	return q
}

func (q *Query[T]) Page(limit int, offset int) *Query[T] {
	q.Pagination.Limit = limit
	q.Pagination.Offset = offset
	return q
}

func (q *Query[T]) SetPageFromRequest(currentPage uint, itemsPerPage uint) *Query[T] {
	if currentPage < 1 {
		currentPage = 1
	}
	q.Pagination.Limit = int(itemsPerPage)
	q.Pagination.Offset = int(currentPage-1) * int(itemsPerPage)
	return q
}

func (q *Query[T]) GroupBy(column ...Column) *Query[T] {
	for _, c := range column {
		if c.Name == "" {
			continue
		}
		if len(q.SelectColumns) > 0 {
			for _, selectColumn := range q.SelectColumns {
				if strings.EqualFold(c.Name, selectColumn.Name) {
					q.GroupByStmt = append(q.GroupByStmt, selectColumn)
					break
				}
			}
		} else {
			q.GroupByStmt = append(q.GroupByStmt, c)
		}

	}
	return q
}

func (q *Query[T]) OrderBy(column ...Column) *Query[T] {
	for _, c := range column {
		if c.Name == "" {
			continue
		}
		q.OrderByStmt = append(q.OrderByStmt, c)
	}
	return q
}

func (q *Query[T]) SetCacheDuration(duration time.Duration) *Query[T] {
	q.CacheDuration = duration
	return q
}

func (q *Query[T]) Limit(limit int) *Query[T] {
	q.LimitCount = limit
	return q
}

func (q *Query[T]) SetCache(cache ctx_cache.Cache) *Query[T] {
	q.Cache = cache
	return q
}

func (q *Query[T]) UseCache() *Query[T] {
	q.useCache = true
	return q
}

func (q *Query[T]) Refresh(refresh bool) *Query[T] {
	q.refreshCache = refresh
	return q
}

func (q *Query[T]) Build() *Query[T] {
	switch q.FromTable.QueryType {
	case QueryTypeFireBase:
	case QueryTypeSQL:
		fallthrough
	default:

		query := q.buildSqlQuery()
		return query
	}
	return q
}
func (q *Query[T]) SetName(name string) *Query[T] {
	q.Name = name
	return q
}

func (q *Query[T]) getName() string {
	if len(q.Name) != 0 {
		return q.Name
	}
	args := []string{
		q.FromTable.Name,
	}
	if len(q.WhereStmts) > 0 {
		args = append(args, "where")
	}
	for _, w := range q.WhereStmts {
		args = append(args, w.LeftValue.Name)
	}
	if len(q.GroupByStmt) > 0 {
		args = append(args, "group by")
	}
	for _, w := range q.GroupByStmt {
		args = append(args, w.Name)
	}

	return strings.ToLower(strings.Join(args, "_"))
}

func (q *Query[T]) RunMap(ctx context.Context, db DB, args ...interface{}) (map[string]*T, error) {
	rows, err := q.Run(ctx, db, args)
	if err != nil {
		return nil, err
	}
	if len(q.MapKeyColumns) == 0 {
		q.MapKeyColumns = append(q.MapKeyColumns, q.FromTable.GetPrimary()...)
	}
	m := map[string]*T{}

	for _, row := range rows {
		// pointer to struct - addressable
		ps := reflect.ValueOf(row)
		// struct
		s := ps.Elem()
		for _, column := range q.MapKeyColumns {
			if s.Kind() == reflect.Struct {
				f := s.FieldByName(column.Name)
				if f.IsValid() {
					m[f.String()] = row
				}
			}
		}
	}
	return m, err
}

func (q *Query[T]) RunSingle(ctx context.Context, db DB, args ...interface{}) (*T, error) {
	rows, err := q.Limit(1).Run(ctx, db, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, sql.ErrNoRows
	}
	return rows[0], nil
}

func (q *Query[T]) RunCtx(ctx context.Context) ([]*T, error) {
	return q.Run(ctx, nil)
}

func (q *Query[T]) Prefix(group string) *Query[T] {
	q.tmpPrefix = group
	return q
}

func (q *Query[T]) Run(ctx context.Context, db DB, args ...interface{}) ([]*T, error) {
	if q.err != nil {
		return nil, q.err
	}
	if len(q.Query) == 0 {
		q.Build()
	}
	ctx = CtxWithQueryTag(ctx, q.getName())
	cacheKey := q.GetCacheKey(args...)

	if q.useCache || q.Cache != nil {
		tracer := otel.GetTracerProvider()
		ctx, span := tracer.Tracer("query-ctx").Start(ctx, fmt.Sprintf("%s-%s", q.Name, q.FromTable.FullTableName()))
		defer span.End()
		return ctx_cache.GetSetCheck[[]*T](ctx, q.CacheDuration, q.FromTable.FullTableName()+q.tmpPrefix, cacheKey, q.refreshCache, func(ctx context.Context, data *[]*T) bool {
			if data == nil {
				return false
			}
			return len(*data) > 0
		},

			func(ctx context.Context) ([]*T, error) {
				if q.NoLock || q.ReadPast {
					return q.FromTable.UseNoLock().NamedSelect(ctx, db, q.Query, q.Args(args))
				}
				return q.FromTable.NamedSelect(ctx, db, q.Query, q.Args(args))
			})
	}

	tracer := otel.GetTracerProvider()
	ctx, span := tracer.Tracer("query").Start(ctx, fmt.Sprintf("%s-%s", q.Name, q.FromTable.FullTableName()))
	defer span.End()
	var data []*T
	var err error
	if q.NoLock || q.ReadPast {
		data, err = q.FromTable.UseNoLock().NamedSelect(ctx, db, q.Query, q.Args(args))
	} else {
		data, err = q.FromTable.NamedSelect(ctx, db, q.Query, q.Args(args))
	}

	if err != nil {
		ctxLogger.Error(ctx, "query error", zap.Error(err), zap.String("query", q.Query))
		span.RecordError(err)
		return nil, err
	}
	return data, nil
}

func (q *Query[T]) Args(args ...interface{}) map[string]interface{} {
	whereArgs := map[string]interface{}{}
	for _, where := range q.WhereStmts {
		if k, arg := where.GetArg(); arg != nil || k != "" {
			whereArgs[k] = arg
		}
	}
	arg, err := combineStructs(append(args, whereArgs)...)
	if err != nil {
		return nil
	}
	return arg
}

func (q *Query[T]) GetCacheKey(args ...interface{}) string {
	var keys []string
	argsData := q.Args(args...)

	keys = append(keys, q.FromTable.FullTableName())
	keys = append(keys, strconv.FormatBool(q.NoLock))
	keys = append(keys, strconv.FormatBool(q.ReadPast))
	for _, k := range q.SelectColumns {
		keys = append(keys, k.Name)
	}
	for _, k := range q.WhereStmts {
		keys = append(keys, k.ToString())
	}
	for _, k := range q.GroupByStmt {
		keys = append(keys, k.FullTableName())
	}
	for _, k := range q.OrderByStmt {
		keys = append(keys, k.FullTableName())
	}
	keys = append(keys, strconv.Itoa(q.Pagination.Offset))
	keys = append(keys, strconv.Itoa(q.Pagination.Limit))
	for k, v := range argsData {
		keys = append(keys, fmt.Sprintf("%s:%s", k, safeString(v)))
	}

	return GetMD5Hash(strings.Join(keys, ""))
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

type TotalRows struct {
	Total int `json:"total" db:"total"`
}

func (q *Query[T]) TotalRows(ctx context.Context, distintColumns *Column) (int, error) {
	var query string
	if q.err != nil {
		return -1, q.err
	}
	if distintColumns == nil {
		query = fmt.Sprintf("SELECT\n\tcount(*) as total\nFROM\n\t%s", q.FromTable.FullTableName())
	} else {
		query = fmt.Sprintf("SELECT\n\tcount(%s) as total\nFROM\n\t%s", distintColumns.FullName(false, false), q.FromTable.FullTableName())
	}

	if len(q.JoinStmt) > 0 {
		for _, join := range q.JoinStmt {
			overlappingColumns := map[string]Column{}
			overlappingColumns = JoinMaps[Column](overlappingColumns, q.FromTable.GetCommonColumns(join.Columns))
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
		query = fmt.Sprintf("%s\n%s", query, q.FromTable.OrderByColumns(len(q.GroupByStmt) > 0, q.OrderByStmt...))
	}
	cacheKey := q.GetCacheKey() + "_total"
	if q.useCache {
		tracer := otel.GetTracerProvider()
		ctx, span := tracer.Tracer("query-ctx").Start(ctx, fmt.Sprintf("%s-%s", q.Name, q.FromTable.FullTableName()))
		defer span.End()
		return ctx_cache.GetSet[int](ctx, q.CacheDuration, q.FromTable.FullTableName(), cacheKey, q.refreshCache, func(ctx context.Context) (int, error) {
			db, err := q.FromTable.NamedQuery(ctx, nil, query, q.Args())
			if err != nil {
				return -1, err
			}
			t := TotalRows{}
			if db.Next() {
				err = db.StructScan(&t)
				if err != nil {
					return -1, err
				}
			}
			return t.Total, nil
		})
	}
	db, err := q.FromTable.NamedQuery(ctx, nil, query, q.Args())
	if err != nil {
		return -1, err
	}
	t := TotalRows{}
	if db.Next() {
		err = db.StructScan(&t)
		if err != nil {
			return -1, err
		}
	}
	return t.Total, nil
}

func (q *Query[T]) buildSqlQuery() *Query[T] {
	if q.err != nil {
		return q
	}
	if q.hasSaved() {
		if v, found := QueryPrepare[q.Name]; found {
			q.Query = v
			return q
		}
	}
	var isGroupBy = len(q.GroupByStmt) > 0
	var query string
	selectColumns := q.FromTable.GetSelectableColumns(isGroupBy, q.SelectColumns...)
	withSelect := ""
	if q.FromQuery != nil {
		q.FromQuery.Build()
		query = fmt.Sprintf("SELECT\n\t%s\nFROM\n\t(%s) %s", strings.Join(selectColumns, ",\n\t"), strings.ReplaceAll(q.FromQuery.Query, "\n", "\n\t"), withSelect)

	} else {
		query = fmt.Sprintf("SELECT\n\t%s\nFROM\n\t%s%s", strings.Join(selectColumns, ",\n\t"), q.FromTable.FullTableName(), withSelect)

	}

	if len(q.JoinStmt) > 0 {
		for _, join := range q.JoinStmt {
			overlappingColumns := map[string]Column{}
			overlappingColumns = JoinMaps[Column](overlappingColumns, q.FromTable.GetCommonColumns(join.Columns))
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
		query = fmt.Sprintf("%s\n%s", query, q.FromTable.OrderByColumns(len(q.GroupByStmt) > 0, q.OrderByStmt...))
	}

	if q.LimitCount > 0 && q.Pagination.Limit == 0 {
		query = fmt.Sprintf("%s\nLIMIT %d;", query, q.LimitCount)
	}

	if q.Pagination.Limit > 0 {
		query = fmt.Sprintf("%s\nLIMIT %d OFFSET %d;", query, q.Pagination.Limit, q.Pagination.Offset)
	}
	q.Query = query
	if q.canSave() {
		QueryPrepare[q.Name] = query
	}
	return q
}

func SelectQuery[T any, X any](ctx context.Context, db DB, q *Query[T], options *DBOptions, args ...interface{}) ([]*X, error) {
	if len(q.Query) == 0 {
		q.Build()
	}
	if db == nil {
		db = q.FromTable.db
	}
	if q.useCache {
		cacheKey := q.GetCacheKey(args...)
		tracer := otel.GetTracerProvider()
		ctx, span := tracer.Tracer("select-query-ctx").Start(ctx, fmt.Sprintf("%s-%s", q.Name, q.FromTable.FullTableName()))
		defer span.End()

		return ctx_cache.GetSet[[]*X](ctx, q.CacheDuration, q.FromTable.FullTableName()+q.tmpPrefix, cacheKey, q.refreshCache, func(ctx context.Context) ([]*X, error) {
			rows, err := NamedQuery(ctx, db, q.Query, options, q.Args(args...))
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
		})
	}
	rows, err := NamedQuery(ctx, db, q.Query, options, q.Args(args...))
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
