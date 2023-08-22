package QueryHelper

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

var (
	ErrTableNotInCtx = errors.New("table is missing from context")
)

type tableCtxName string

func AddTableCtx[T any](ctx context.Context, db *sqlx.DB, dataset string, dropTable, updateColumns bool) (context.Context, error) {
	table, err := NewTable[T](dataset)
	if err != nil {
		return nil, nil
	}

	err = table.InitializeTable(ctx, db, dropTable, updateColumns)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, tableCtxName(table.Name), table)
	return ctx, nil
}

func GetTableCtx[T any](ctx context.Context) (*Table[T], error) {
	var s T
	name := getType(s)

	value := ctx.Value(tableCtxName(name))
	if value == nil {
		return nil, ErrTableNotInCtx
	}
	return value.(*Table[T]), nil
}
