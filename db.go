package QueryHelper

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/xwb1989/sqlparser"
	"regexp"
	"strings"
)

var (
	// List of reserved keywords
	reservedKeywords = []string{
		"ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "BACKUP", "BETWEEN",
		"CASE", "CHECK", "COLUMN", "CONSTRAINT", "CREATE", "DATABASE", "DEFAULT",
		"DELETE", "DESC", "DISTINCT", "DROP", "EXEC", "EXISTS", "FOREIGN", "FROM",
		"FULL", "GROUP", "HAVING", "IN", "INDEX", "INNER", "INSERT", "IS", "JOIN",
		"KEY", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "OR", "ORDER", "OUTER",
		"PRIMARY", "PROCEDURE", "RIGHT", "ROWNUM", "SELECT", "SET", "TABLE", "TOP",
		"TRUNCATE", "UNION", "UNIQUE", "UPDATE", "VALUES", "VIEW", "WHERE",
	}

	// Regex patterns for invalid column names
	patterns = []string{
		`^\d+.*`,     // Starts with a number
		`.*\s+.*`,    // Contains space
		`.*[-\.@].*`, // Contains dash, dot, or at-sign
	}
)

type DB interface {
	Ping(ctx context.Context) error
	CreateTable(ctx context.Context, dataset, table string, columns map[string]Column) error
	QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error)
	ExecContext(ctx context.Context, query string, args interface{}) error
	RawQueryContext(ctx context.Context, query string, args interface{}) (DBRow, error)
	Close()
	GetDataset(ds string) string
}

type DBRow interface {
	Next() bool
	StructScan(i interface{}) error
	Scan(i ...any) error
	Close() error
}

var _ DBRow = &sqlx.Rows{}

type MockDB struct {
	tables   map[string]*mockTable
	mockData map[string]map[string]*mockData
	prefix   string
}

func (m MockDB) RawQueryContext(ctx context.Context, query string, args interface{}) (DBRow, error) {
	return nil, nil
}

func (m MockDB) GetDataset(ds string) string {
	if len(m.prefix) > 0 {
		return m.prefix + ds
	}
	return ds
}

type mockTable struct {
	name    string
	dataset string
	columns map[string]Column
}

type mockData struct {
	//name    string
	//dataset string
	//columns map[string]Column
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
	for _, col := range columns {
		if !isValidColumnName(col.Name) {
			return fmt.Errorf("column name %s is not valid", col.Name)
		}
	}
	return nil
}

func (m MockDB) QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error) {
	if valid, err := isSQLValid(query); err != nil && !valid {
		return nil, fmt.Errorf("invalid query %s: %v", query, err)
	}
	return nil, nil
}

func (m MockDB) ExecContext(ctx context.Context, query string, args interface{}) error {
	if valid, err := isSQLValid(query); err != nil && !valid {
		return fmt.Errorf("invalid query %s: %v", query, err)
	}
	return nil
}

func (m MockDB) Close() {
}

var _ DB = MockDB{}

func isSQLValid(sql string) (bool, error) {
	_, err := sqlparser.Parse(sql)
	if err != nil {
		return false, err
	}
	return true, nil
}

func isValidColumnName(columnName string) bool {
	for _, keyword := range reservedKeywords {
		if strings.ToUpper(columnName) == keyword {
			return false
		}
	}

	for _, pattern := range patterns {
		matched, _ := regexp.MatchString(pattern, columnName)
		if matched {
			return false
		}
	}
	return true
}
