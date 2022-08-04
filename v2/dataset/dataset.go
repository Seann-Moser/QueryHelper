package dataset

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
	"github.com/patrickmn/go-cache"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/Seann-Moser/QueryHelper/v2/table"
)

type Dataset struct {
	Name            string
	structsToTables []interface{}
	tables          map[string]*table.Table
	ctx             context.Context
	DB              *sqlx.DB
	cache           *cache.Cache
	logger          *zap.Logger
	generator       *table.Generator
}

func NewDataset(ctx context.Context, name string, logger *zap.Logger, db *sqlx.DB, structsToTables ...interface{}) (*Dataset, error) {
	d := Dataset{
		Name:            name,
		structsToTables: structsToTables,
		tables:          map[string]*table.Table{},
		ctx:             ctx,
		DB:              db,
		logger:          logger,
		generator:       table.NewGenerator(db, false, logger),
	}
	for _, i := range d.structsToTables {
		err := d.addTable(i)
		if err != nil {
			return nil, err
		}
	}

	return &d, nil
}

func (d *Dataset) addTable(s interface{}) error {
	ts, err := d.generator.TableFromStruct(d.Name, s)
	if err != nil {
		return err
	}
	d.logger.Debug("add_table",
		zap.String("table", ts.FullTableName()), zap.Int("total_elements", len(ts.Elements)))
	d.tables[getType(s)] = ts
	return d.generator.CreateMySqlTable(d.ctx, ts)
}

func (d *Dataset) GetTable(s interface{}) *table.Table {
	if v, found := d.tables[getType(s)]; found {
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

func (d *Dataset) Insert(ctx context.Context, s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		d.logger.Debug("insert", zap.String("query", v.InsertStatement()))
		return d.DB.NamedExecContext(ctx, v.InsertStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Update(ctx context.Context, s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		d.logger.Debug("update", zap.String("query", v.UpdateStatement()))
		return d.DB.NamedExecContext(ctx, v.UpdateStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Delete(ctx context.Context, s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		d.logger.Debug("delete", zap.String("query", v.DeleteStatement()))
		return d.DB.NamedExecContext(ctx, v.DeleteStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Count(ctx context.Context, s interface{}, conditional string, whereStmt ...string) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		d.logger.Debug("count", zap.String("query", v.Count(conditional, whereStmt...)))
		return d.DB.NamedExecContext(ctx, v.Count(conditional, whereStmt...), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) DeleteAllReferences(ctx context.Context, s interface{}) (sql.Result, error) {
	var err error
	for _, v := range d.tables {
		query := v.DeleteStatement()
		d.logger.Debug("delete", zap.String("query", query))
		_, e := d.DB.NamedExecContext(ctx, query, s)
		err = multierr.Combine(err, e)
	}
	return nil, err
}

func (d *Dataset) SelectJoin(ctx context.Context, selectCol, whereStr []string, s ...interface{}) (*sqlx.Rows, error) {
	if v, found := d.tables[getType(s[0])]; found {
		var tables []*table.Table
		for _, t := range s {
			if v, found := d.tables[getType(t)]; found {
				tables = append(tables, v)
			}
		}
		args, err := combineStructs(s[0:]...)
		if err != nil {
			return nil, err
		}
		query := v.SelectJoin(selectCol, whereStr, tables[1:]...)
		d.logger.Debug("select_join", zap.String("query", query))
		return d.DB.NamedQueryContext(ctx, query, interface{}(args))
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
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
