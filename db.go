package QueryHelper

import "context"

type DB interface {
	Ping(ctx context.Context) error
	CreateTable(ctx context.Context, dataset, table string, columns map[string]*Column) error
	QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error)
	ExecContext(ctx context.Context, query string, args interface{}) error
	Close()
}

type DBRow interface {
	Next() bool
	StructScan(i interface{}) error
}
