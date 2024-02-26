package QueryHelper

import (
	"context"
	"errors"
	"fmt"
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

func InsertCtx[T any](ctx context.Context, data *T, suffix ...string) (string, error) {
	table, err := GetTableCtx[T](ctx, suffix...)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", fmt.Errorf("no data provided")
	}
	id, err := table.Insert(ctx, nil, *data)
	if err != nil {
		return "", err
	}
	return id, nil
}

func DeleteAllCtx[T any](ctx context.Context, data []*T, suffix ...string) error {
	table, err := GetTableCtx[T](ctx, suffix...)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("no data provided")
	}
	for _, d := range data {
		err = table.Delete(ctx, nil, *d)
		if err != nil {
			return err
		}
	}
	return nil
}

func DeleteCtx[T any](ctx context.Context, data *T, suffix ...string) error {
	table, err := GetTableCtx[T](ctx, suffix...)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("no data provided")
	}
	return table.Delete(ctx, nil, *data)
}

func UpdateCtx[T any](ctx context.Context, data *T, suffix ...string) error {
	table, err := GetTableCtx[T](ctx, suffix...)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("no data provided")
	}
	return table.Update(ctx, nil, *data)
}

func ListCtx[T any](ctx context.Context, stmt ...*WhereStmt) ([]*T, error) {
	q := GetQuery[T](ctx)
	q.WhereStmts = append(q.WhereStmts, stmt...)
	return q.Run(ctx, nil)
}

func GetIDCtx[T any](ctx context.Context, id string) (*T, error) {
	q := GetQuery[T](ctx)
	q.Where(q.Column("id"), "=", "AND", 0, id)
	return q.RunSingle(ctx, nil)
}

func GetColumn[T any](ctx context.Context, name string) Column {
	return GetQuery[T](ctx).Column(name)
}
