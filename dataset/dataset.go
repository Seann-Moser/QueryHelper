package dataset

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/Seann-Moser/QueryHelper/table/dataset_table"
	"github.com/Seann-Moser/QueryHelper/table/generator"
)

type Dataset struct {
	Name            string
	structsToTables []interface{}
	Tables          map[string]dataset_table.Table
	ctx             context.Context
	DB              *sqlx.DB
	logger          *zap.Logger
	generator       *generator.Generator
	dryRun          bool
	createTable     bool

	cache Cache
}

func New(ctx context.Context, name string, createTable, dropTable bool, logger *zap.Logger, db *sqlx.DB, cache Cache, structsToTables ...interface{}) (*Dataset, error) {
	d := Dataset{
		Name:            name,
		structsToTables: structsToTables,
		Tables:          map[string]dataset_table.Table{},
		ctx:             ctx,
		DB:              db,
		logger:          logger,
		generator:       generator.New(dropTable, logger),
		dryRun:          db == nil,
		createTable:     createTable,
		cache:           cache,
	}
	for _, i := range d.structsToTables {
		err := d.AddTable(i)
		if err != nil {
			return nil, err
		}
	}

	return &d, nil
}

func (d *Dataset) AddTable(s interface{}) error {
	ts, err := d.generator.Table(d.Name, s)
	if err != nil {
		return err
	}
	d.logger.Info("add_table",
		zap.String("table", ts.FullTableName()), zap.Int("total_elements", len(ts.GetElements())))
	d.Tables[getType(s)] = ts
	if d.createTable {
		return d.CreateTable(ts)
	}
	return nil
}
func (d *Dataset) CreateTable(t dataset_table.Table) error {
	schema, sqlStmt := d.generator.MySqlTable(t)
	_, err := d.execQuery(d.ctx, schema)
	if err != nil {
		return err
	}
	_, err = d.execQuery(d.ctx, sqlStmt)
	return err
}

func (d *Dataset) GetTable(s interface{}) dataset_table.Table {
	if v, found := d.Tables[getType(s)]; found {
		return v
	}
	return nil
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

func (d *Dataset) Insert(ctx context.Context, s interface{}) (sql.Result, string, error) {
	if v, found := d.Tables[getType(s)]; found {
		if v.IsAutoGenerateID() {
			generateIds := v.GenerateID()
			args, err := combineStructs(generateIds, s)
			if err != nil {
				return nil, "", err
			}
			results, err := d.execQuery(ctx, v.InsertStatement(), args)
			return results, generateIds[v.GetGenerateID()[0].Name], err
		}
		results, err := d.execQuery(ctx, v.InsertStatement(), s)
		return results, "", err
	}
	return nil, "", fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Update(ctx context.Context, s interface{}) (sql.Result, error) {
	if v, found := d.Tables[getType(s)]; found {
		d.logger.Debug("update", zap.String("query", v.UpdateStatement()))
		return d.execQuery(ctx, v.UpdateStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Delete(ctx context.Context, s interface{}) (sql.Result, error) {
	if v, found := d.Tables[getType(s)]; found {
		d.logger.Debug("delete", zap.String("query", v.DeleteStatement()))
		return d.execQuery(ctx, v.DeleteStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Count(ctx context.Context, s interface{}, conditional string, whereStmt ...string) (int, error) {
	if v, found := d.Tables[getType(s)]; found {
		d.logger.Debug("count", zap.String("query", v.CountStatement(conditional, whereStmt...)))
		rows, err := d.namedQuery(ctx, v.CountStatement(conditional, whereStmt...), s)
		if err != nil {
			return 0, err
		}

		var i int
		err = rows.Scan(&i)
		if err != nil {
			return 0, err
		}
		return i, nil
	}
	return -1, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) DeleteAllReferences(ctx context.Context, s interface{}) (sql.Result, error) {
	var err error
	for _, v := range d.Tables {
		query := v.DeleteStatement()

		_, e := d.execQuery(ctx, query, s)
		if err != nil && !strings.Contains(e.Error(), "could not find") {
			err = multierr.Combine(err, e)

		}
	}
	return nil, err
}

func (d *Dataset) Select(ctx context.Context, s interface{}, whereStmts ...string) (*sqlx.Rows, error) {
	stmt, err := d.SelectStatement(s, whereStmts...)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	args := map[string]interface{}{}
	err = json.Unmarshal(b, &args)
	if err != nil {
		return nil, err
	}
	table, found := d.Tables[getType(s)]
	if found {
		rs, err := d.cache.Get(args, table)
		if err == nil && rs != nil {
			return rs, nil
		}
	}
	rows, err := d.namedQuery(ctx, stmt, args)
	d.cache.Set(rows, args, table)
	return rows, err

}

func (d *Dataset) SelectStatement(s interface{}, whereStmts ...string) (string, error) {
	if v, found := d.Tables[getType(s)]; found {
		selectStatement := v.SelectStatement(whereStmts...)
		return selectStatement, nil
	}
	return "", fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) SelectJoin(ctx context.Context, selectCol, whereStr []string, s ...interface{}) (*sqlx.Rows, error) {
	if v, found := d.Tables[getType(s[0])]; found {
		var tables []dataset_table.Table
		for _, t := range s {
			t := getType(t)
			if v, found := d.Tables[t]; found {
				tables = append(tables, v)
			}
		}
		args, err := combineStructs(s[0:]...)
		if err != nil {
			return nil, err
		}
		rs, err := d.cache.Get(args, tables...)
		if err == nil && rs != nil {
			return rs, nil
		}

		query := v.SelectJoin(selectCol, whereStr, tables[1:]...)
		rows, err := d.namedQuery(ctx, query, interface{}(args))
		d.cache.Set(rows, args, tables...)
		return rows, err
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) execQuery(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	d.logger.Debug("running named query", zap.String("query", query), zap.Any("args", args))
	if d.dryRun {
		return nil, nil
	}
	return d.DB.NamedExecContext(ctx, query, args)
}

func (d *Dataset) namedQuery(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	d.logger.Debug("running named query", zap.String("query", query), zap.Any("args", args))
	if d.dryRun {
		return nil, nil
	}
	return d.DB.NamedQueryContext(ctx, query, interface{}(args))
}

func combineStructs(i ...interface{}) (map[string]interface{}, error) {
	output := map[string]interface{}{}
	for _, s := range i {
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		t := map[string]interface{}{}
		err = json.Unmarshal(b, &t)
		if err != nil {
			return nil, err
		}
		output = joinMaps(output, t)
	}
	return output, nil
}
func joinMaps(m ...map[string]interface{}) map[string]interface{} {
	output := map[string]interface{}{}
	for _, currentMap := range m {
		for k, v := range currentMap {
			if _, found := output[k]; !found {
				output[k] = v
			}
		}
	}
	return output
}
