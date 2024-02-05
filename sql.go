package QueryHelper

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"os"
	"sort"
	"strings"
)

var _ DB = &SqlDB{}

type SqlDB struct {
	sql *sqlx.DB
}

func NewSql(db *sqlx.DB) *SqlDB {
	return &SqlDB{
		sql: db,
	}
}

func (s *SqlDB) Ping(ctx context.Context) error {
	return s.sql.PingContext(ctx)
}

func (s *SqlDB) Close() {
	_ = s.sql.Close()
}

func (s *SqlDB) CreateTable(ctx context.Context, dataset, table string, columns map[string]*Column) error {
	createSchemaStatement := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dataset)
	var PrimaryKeys []string
	var FK []string
	createStatement := ""
	createStatement += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s(", dataset, table)

	var c []*Column

	for _, column := range columns {
		c = append(c, column)
	}

	sort.Slice(c, func(i, j int) bool {
		return c[i].ColumnOrder < c[j].ColumnOrder
	})

	for _, column := range c {
		createStatement += column.GetDefinition() + ","
		if column.HasFK() {
			FK = append(FK, column.GetFK())
		}
		if column.Primary {
			PrimaryKeys = append(PrimaryKeys, column.Name)
		}
	}
	if len(PrimaryKeys) == 0 {
		return MissingPrimaryKeyErr
	} else if len(PrimaryKeys) == 1 {
		createStatement += fmt.Sprintf("\n\tPRIMARY KEY(%s)", PrimaryKeys[0])
	} else {
		createStatement += fmt.Sprintf("\n\tCONSTRAINT PK_%s_%s PRIMARY KEY (%s)", dataset, table, strings.Join(PrimaryKeys, ","))

	}
	if len(FK) > 0 {
		createStatement += "," + strings.Join(FK, ",")
	}
	createStatement += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"

	for _, stmt := range []string{createSchemaStatement, createStatement} {
		_, err := s.sql.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SqlDB) QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error) {
	defer func() { //catch or finally
		if err := recover(); err != nil { //catch
			fmt.Fprintf(os.Stderr, "Exception: %v\n", err)
			os.Exit(1)
		}
	}()
	return s.sql.NamedQueryContext(ctx, query, args)
}

func (s *SqlDB) ExecContext(ctx context.Context, query string, args interface{}) error {
	defer func() { //catch or finally
		if err := recover(); err != nil { //catch
			fmt.Fprintf(os.Stderr, "Exception: %v\n", err)
			os.Exit(1)
		}
	}()
	_, err := s.sql.NamedExecContext(ctx, query, args)
	return err
}
