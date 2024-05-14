package QueryHelper

import (
	"context"
	"fmt"
)

type DB interface {
	Ping(ctx context.Context) error
	CreateTable(ctx context.Context, dataset, table string, columns map[string]Column) error
	QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error)
	ExecContext(ctx context.Context, query string, args interface{}) error
	Close()
}

type DBRow interface {
	Next() bool
	StructScan(i interface{}) error
}

type MockDB struct {
	tables   map[string]*mockTable
	mockData map[string]map[string]*mockData
}

type mockTable struct {
	name    string
	dataset string
	columns map[string]Column
}

type mockData struct {
	name    string
	dataset string
	columns map[string]Column
}

func NewMockDB() *MockDB {
	return &MockDB{
		tables: map[string]*mockTable{},
	}
}
func (m MockDB) Ping(ctx context.Context) error {
	return nil
}

func (m MockDB) CreateTable(ctx context.Context, dataset, table string, columns map[string]Column) error {
	m.tables[fmt.Sprintf("%s.%s", dataset, table)] = &mockTable{
		name:    table,
		dataset: dataset,
		columns: columns,
	}
	return nil
}

func (m MockDB) QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error) {
	// todo new row
	return nil, nil
}

func (m MockDB) ExecContext(ctx context.Context, query string, args interface{}) error {
	// todo new row
	return nil
}

func (m MockDB) Close() {
}

var _ DB = MockDB{}
