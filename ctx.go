package QueryHelper

import (
	"context"
	"errors"
	"strings"
)

var (
	ErrTableNotInCtx = errors.New("table is missing from context")
)

type tableCtxName string

func AddTableCtx[T any](ctx context.Context, db DB, dataset string, queryType QueryType, suffix ...string) (context.Context, error) {
	table, err := NewTable[T](dataset, queryType)
	if err != nil {
		return ctx, err
	}
	err = table.InitializeTable(ctx, db, suffix...)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, tableCtxName(table.Name), table)
	return ctx, nil
}

func GetTableCtx[T any](ctx context.Context, suffix ...string) (*Table[T], error) {
	var s T
	name := ToSnakeCase(getType(s))

	value := ctx.Value(tableCtxName(strings.Join(append([]string{name}, suffix...), "_")))
	if value == nil {
		return nil, ErrTableNotInCtx
	}
	return value.(*Table[T]), nil
}

func WithTableContext(baseCtx context.Context, tableCtx context.Context, names ...string) (context.Context, error) {
	for _, name := range names {
		value := tableCtx.Value(tableCtxName(name))
		if value == nil {
			return nil, ErrTableNotInCtx
		}
		baseCtx = context.WithValue(baseCtx, tableCtxName(name), value)

	}
	return baseCtx, nil
}
