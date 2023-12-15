package QueryHelper

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/Seann-Moser/ctx_cache"
	"sort"
	"strings"
	"sync"
)

type CacheMonitor struct {
	TableCache map[string]map[string]string
	Cache      ctx_cache.Cache
	signal     chan string
}

var syncMutex = &sync.RWMutex{}
var TableCache map[string]map[string]string = map[string]map[string]string{}
var tableUpdateSignal chan string = make(chan string)

func NewCacheMonitor(ctx context.Context) *CacheMonitor {
	cm := &CacheMonitor{
		TableCache: TableCache,
		Cache:      ctx_cache.GetCacheFromContext(ctx),
		signal:     tableUpdateSignal,
	}
	return cm
}

func (cm *CacheMonitor) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-cm.signal:
				if !ok {
					return
				}
				syncMutex.Lock()
				for _, keys := range cm.TableCache[v] {
					_ = cm.Cache.DeleteKey(ctx, keys)
				}
				delete(cm.TableCache, v)
				syncMutex.Unlock()
			}
		}
	}()

}

type Query[T any] struct {
	Name          string
	err           error
	SelectColumns []*Column
	FromTable     *Table[T]
	FromQuery     *Query[T]
	JoinStmt      []*JoinStmt
	WhereStmts    []*WhereStmt
	GroupByStmt   []*Column
	OrderByStmt   []*Column

	LimitCount int

	Cache     ctx_cache.Cache
	Query     string
	skipCache bool
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

func GetQuery[T any](ctx context.Context) *Query[T] {
	table, err := GetTableCtx[T](ctx)
	if err != nil {
		return &Query[T]{err: err}
	}
	q := QueryTable[T](table)
	return q
}

func (w *WhereStmt) ToString() string {
	column := w.LeftValue
	if column == nil {
		return ""
	}
	tmp := column.Where
	if w.Conditional != "" {
		tmp = w.Conditional
	} else if tmp == "" {
		tmp = "="
	}
	var formatted string
	switch strings.TrimSpace(strings.ToLower(tmp)) {
	case "is not":
		if w.RightValue == nil {
			formatted = fmt.Sprintf("%s %s null", column.FullName(false), tmp)
		}
	case "is":
		if w.RightValue == nil {
			formatted = fmt.Sprintf("%s %s null", column.FullName(false), tmp)
		}
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

			if w.Level < previousLevel {
				stmt += fmt.Sprintf(" %s", generateList(")", previousLevel-w.Level))
			}
			if i > 0 {
				if w.JoinOperator == "" {
					w.JoinOperator = "AND"
				}
				stmt += " " + strings.ToUpper(w.JoinOperator)
			}

			if w.Level > previousLevel {
				stmt += fmt.Sprintf(" %s", generateList("(", w.Level-previousLevel))
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
		if c == nil {
			return ""
		}
		if c.GroupByName != "" {
			columns = append(columns, c.GroupByName)
		} else if c.ForceGroupByValue {
			columns = append(columns, c.FullName(true))
		} else {
			columns = append(columns, c.FullName(false))
		}

	}
	return "GROUP BY " + strings.Join(columns, ",")
}

func QueryTable[T any](table *Table[T]) *Query[T] {
	return &Query[T]{
		SelectColumns: []*Column{},
		Name:          "",
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
	for _, c := range columns {
		if c == nil {
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

func (q *Query[T]) Column(name string) *Column {
	if q.err != nil {
		return nil
	}
	c := q.FromTable.GetColumn(name)
	if c == nil {
		q.err = fmt.Errorf("missing column from table(%s) %s", q.FromTable.FullTableName(), name)
	}
	return c
}

func (q *Query[T]) Join(tableColumns map[string]*Column, joinType string) *Query[T] {
	q.JoinStmt = append(q.JoinStmt, &JoinStmt{
		Columns:  tableColumns,
		JoinType: joinType,
	})
	return q
}

func (q *Query[T]) JoinColumn(joinType string, tableColumns *Column) *Query[T] {
	q.JoinStmt = append(q.JoinStmt, &JoinStmt{
		Columns:  map[string]*Column{tableColumns.Name: tableColumns},
		JoinType: joinType,
	})
	return q
}

func (q *Query[T]) Where(column *Column, conditional, joinOperator string, level int, value interface{}) *Query[T] {
	if level < 0 {
		level = 0
	}
	if column == nil {
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

func (q *Query[T]) GroupBy(column ...*Column) *Query[T] {
	for _, c := range column {
		if c == nil {
			continue
		}
		q.GroupByStmt = append(q.GroupByStmt, c)
	}
	return q
}

func (q *Query[T]) OrderBy(column ...*Column) *Query[T] {
	for _, c := range column {
		if c == nil {
			continue
		}
		q.OrderByStmt = append(q.OrderByStmt, c)
	}
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

func (q *Query[T]) Build() *Query[T] {
	switch q.FromTable.QueryType {
	case QueryTypeFireBase:
	case QueryTypeSQL:
		fallthrough
	default:
		return q.buildSqlQuery()
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
func (q *Query[T]) Run(ctx context.Context, db DB, args ...interface{}) ([]*T, error) {
	if q.err != nil {
		return nil, q.err
	}
	if len(q.Query) == 0 {
		q.Build()
	}
	ctx = CtxWithQueryTag(ctx, q.getName())
	cacheKey := q.GetCacheKey(args)
	syncMutex.RLock()
	if _, found := TableCache[q.FromTable.FullTableName()]; !found {
		TableCache[q.FromTable.FullTableName()] = map[string]string{}
	}
	TableCache[q.FromTable.FullTableName()][cacheKey] = ""
	syncMutex.RUnlock()
	if q.Cache != nil {
		data, err := ctx_cache.GetFromCache[[]*T](ctx, q.Cache, cacheKey)
		if err == nil && len(*data) > 0 {
			return *data, nil
		}
	}

	data, err := q.FromTable.NamedSelect(ctx, db, q.Query, q.Args(args))
	if err != nil {
		return nil, err
	}

	if q.Cache != nil {
		_ = ctx_cache.SetFromCache[[]*T](ctx, q.Cache, cacheKey, data)
	}

	return data, nil
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

func (q *Query[T]) GetCacheKey(args ...interface{}) string {
	var key []string
	for _, k := range q.SelectColumns {
		key = append(key, k.Name)
	}
	for _, k := range q.WhereStmts {
		key = append(key, k.ToString())
	}
	for _, k := range q.GroupByStmt {
		key = append(key, k.Name)
	}
	for _, k := range q.OrderByStmt {
		key = append(key, k.Name)
	}
	argsData := q.Args(args)
	keys := make([]string, 0, len(argsData))

	for k := range argsData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		keys = append(keys, safeString(argsData[k]))
	}

	return GetMD5Hash(strings.Join(key, ""))
}
func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}
func (q *Query[T]) buildSqlQuery() *Query[T] {
	if q.err != nil {
		return q
	}
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
		query = fmt.Sprintf("%s\n%s", query, q.FromTable.OrderByColumns(len(q.GroupByStmt) > 0, q.OrderByStmt...))
	}

	if q.LimitCount > 0 {
		query = fmt.Sprintf("%s\nLIMIT %d;", query, q.LimitCount)
	}
	q.Query = query
	return q
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
