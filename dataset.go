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
}

func NewDataset(ctx context.Context, name string, db *sqlx.DB, structsToTables ...interface{}) (*Dataset, error) {
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

func (d *Dataset) Select(s interface{}) (*sqlx.Rows, error) {
	if v, found := d.tables[getType(s)]; found {
		selectStatement := v.GenerateNamedSelectStatement()
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		key := getCacheKey(b, selectStatement)
		if v, found := d.cache.Get(key); found {
			switch t := v.(type) {
			case sqlx.Rows:
				return &t, nil
			case *sqlx.Rows:
				return t, nil
			}
		}

		t := map[string]interface{}{}
		err = json.Unmarshal(b, &t)
		if err != nil {
			return nil, err
		}
		rows, err := d.DB.NamedQueryContext(d.ctx, selectStatement, t)
		if err != nil {
			return nil, err
		}
		d.cache.Set(key, rows, cache.DefaultExpiration)
		return rows, nil
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}
func getCacheKey(data []byte, selectStmt string) string {
	h := sha1.New()
	h.Write(append(data, []byte(selectStmt)...))
	sha1Hash := hex.EncodeToString(h.Sum(nil))
	return sha1Hash
}

func (d *Dataset) Insert(s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		return d.DB.NamedExecContext(d.ctx, v.GenerateNamedInsertStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Update(s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		return d.DB.NamedExecContext(d.ctx, v.GenerateNamedUpdateStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) Delete(s interface{}) (sql.Result, error) {
	if v, found := d.tables[getType(s)]; found {
		return d.DB.NamedExecContext(d.ctx, v.GenerateNamedDeleteStatement(), s)
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) DeleteAllReferences(s interface{}) (sql.Result, error) {
	var err error
	for _, v := range d.tables {
		_, e := d.DB.NamedExecContext(d.ctx, v.GenerateNamedUpdateStatement(), s)
		err = multierr.Combine(err, e)
	}
	return nil, err
}
func (d *Dataset) SelectJoin(s ...interface{}) (*sqlx.Rows, error) {
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
		return d.DB.NamedQueryContext(d.ctx, v.GenerateNamedSelectJoinStatement(tables[1:]...), interface{}(args))
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}

func (d *Dataset) SelectJoinDatasets(d2 *Dataset, s ...interface{}) (*sqlx.Rows, error) {
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
		return d.DB.NamedQueryContext(d.ctx, v.GenerateNamedSelectJoinStatement(tables[1:]...), interface{}(args))
	}
	return nil, fmt.Errorf("unable to find insert for type: %s", getType(s))
}
