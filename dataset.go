package QueryHelper

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/patrickmn/go-cache"
	"go.uber.org/multierr"
	"time"
)

type Dataset struct {
	Name            string
	structsToTables []interface{}
	tables          map[string]*Table
	ctx             context.Context
	DB              *sqlx.DB
	cache           *cache.Cache
	Debug           bool
}

func NewDataset(ctx context.Context, name string, db *sqlx.DB, structsToTables ...interface{}) (*Dataset, error) {
	d := Dataset{
		Name:            name,
		structsToTables: structsToTables,
		tables:          map[string]*Table{},
		ctx:             ctx,
		DB:              db,
	}
	for _, i := range d.structsToTables {
		err := d.addTable(i)
		if err != nil {
			return nil, err
		}
	}

	return &d, nil
}
func NewDatasetWithCache(ctx context.Context, name string, db *sqlx.DB, structsToTables ...interface{}) (*Dataset, error) {
	d := Dataset{
		Name:            name,
		structsToTables: structsToTables,
		tables:          map[string]*Table{},
		ctx:             ctx,
		DB:              db,
		cache:           cache.New(2*time.Minute, 5*time.Minute),
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
	table, err := GenerateTableFromStruct(d.Name, s)
	if err != nil {
		return err
	}
	d.tables[getType(s)] = table
	return CreateMySqlTable(d.ctx, d.DB, table)
}

func (d *Dataset) GetTable(s interface{}) *Table {
	if v, found := d.tables[getType(s)]; found {
		return v
	}
	return nil
}

func (d *Dataset) Select(ctx context.Context, s interface{}, whereStmts ...string) (*sqlx.Rows, error) {
	if v, found := d.tables[getType(s)]; found {
		selectStatement := v.GenerateNamedSelectStatement()
		if len(whereStmts) > 0 {
			selectStatement = v.GenerateNamedSelectStatementWithCustomWhere(whereStmts...)
		}
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		key := getCacheKey(b, selectStatement)
		if v := d.GetCache(key); v != nil {
			return v, nil
		}

		t := map[string]interface{}{}
		err = json.Unmarshal(b, &t)
		if err != nil {
			return nil, err
		}
		if d.Debug {
			fmt.Printf("select statement: %s\n", selectStatement)
		}
		rows, err := d.DB.NamedQueryContext(ctx, selectStatement, t)
		if err != nil {
			return nil, err
		}
		if d.cache != nil {
			d.cache.Set(key, rows, cache.DefaultExpiration)
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) GetCache(key string) *sqlx.Rows {
	if d.cache != nil {
		if v, found := d.cache.Get(key); found {
			if d.Debug {
				fmt.Printf("loading from cache:%s\n", key)
			}
			switch t := v.(type) {
			case sqlx.Rows:
				return &t
			case *sqlx.Rows:
				return t
			}
		}
	}
	return nil
}

func getCacheKey(data []byte, selectStmt string) string {
	h := sha1.New()
	h.Write(append(data, []byte(selectStmt)...))
	sha1Hash := hex.EncodeToString(h.Sum(nil))
	return sha1Hash
}

func (d *Dataset) Insert(ctx context.Context, s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		return d.DB.NamedExecContext(ctx, v.GenerateNamedInsertStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Update(ctx context.Context, s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		return d.DB.NamedExecContext(ctx, v.GenerateNamedUpdateStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Delete(ctx context.Context, s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		return d.DB.NamedExecContext(ctx, v.GenerateNamedDeleteStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) DeleteAllReferences(ctx context.Context, s interface{}) (sql.Result, error) {
	var err error
	for _, v := range d.tables {
		if d.Debug {
			fmt.Printf("delete: %s\n", v.GenerateNamedDeleteStatement())
		}
		_, e := d.DB.NamedExecContext(ctx, v.GenerateNamedDeleteStatement(), s)
		err = multierr.Combine(err, e)
	}
	return nil, err
}
func (d *Dataset) SelectJoin(ctx context.Context, s ...interface{}) (*sqlx.Rows, error) {
	if v, found := d.tables[getType(s[0])]; found {
		var tables []*Table
		for _, t := range s {
			if v, found := d.tables[getType(t)]; found {
				tables = append(tables, v)
			}
		}
		args, err := CombineStructs(s[0:]...)
		if err != nil {
			return nil, err
		}
		query := v.GenerateNamedSelectJoinStatement(tables[1:]...)
		if d.Debug {
			fmt.Printf("joinedSelect: %s\n", query)
		}
		return d.DB.NamedQueryContext(ctx, query, interface{}(args))
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) SelectJoinCustomWhere(ctx context.Context, whereStr []string, s ...interface{}) (*sqlx.Rows, error) {
	if v, found := d.tables[getType(s[0])]; found {
		var tables []*Table
		for _, t := range s {
			if v, found := d.tables[getType(t)]; found {
				tables = append(tables, v)
			}
		}
		args, err := CombineStructs(s[0:]...)
		if err != nil {
			return nil, err
		}
		query := v.GenerateNamedSelectJoinStatementWithCustomWhere(whereStr, tables[1:]...)
		if d.Debug {
			fmt.Printf("joinedSelect: %s\n", query)
		}
		return d.DB.NamedQueryContext(ctx, query, interface{}(args))
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) SelectJoinDatasets(ctx context.Context, d2 *Dataset, s ...interface{}) (*sqlx.Rows, error) {
	if v, found := d.tables[getType(s[0])]; found {
		var tables []*Table
		for _, t := range s {
			if v, found := d.tables[getType(t)]; found {
				tables = append(tables, v)
			} else if v, found := d2.tables[getType(t)]; found {
				tables = append(tables, v)
			}
		}
		args, err := CombineStructs(s[0:]...)
		if err != nil {
			return nil, err
		}
		query := v.GenerateNamedSelectJoinStatement(tables[1:]...)
		if d.Debug {
			fmt.Printf("joinedSelect: %s\n", query)
		}
		return d.DB.NamedQueryContext(ctx, query, interface{}(args))
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}
