package QueryHelper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Seann-Moser/go-serve/pkg/ctxLogger"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"os"
	"sort"
	"strings"
)

var _ DB = &SqlDB{}

type SqlDB struct {
	sql           *sqlx.DB
	updateColumns bool
	tablePrefix   string
}

func Flags() *pflag.FlagSet {
	fs := pflag.NewFlagSet("sql-db", pflag.ExitOnError)
	fs.Bool("sql-db-update-columns", false, "")
	fs.String("sql-db-prefix", "", "")
	return fs
}
func NewSql(db *sqlx.DB) *SqlDB {
	return &SqlDB{
		sql:           db,
		updateColumns: viper.GetBool("sql-db-update-columns"),
		tablePrefix:   viper.GetString("sql-db-prefix"),
	}
}

func (s *SqlDB) Ping(ctx context.Context) error {
	return s.sql.PingContext(ctx)
}

func (s *SqlDB) Close() {
	_ = s.sql.Close()
}

func (s *SqlDB) GetDataset(ds string) string {
	return fmt.Sprintf("%s%s", s.tablePrefix, ds)
}

func (s *SqlDB) CreateTable(ctx context.Context, dataset, table string, columns map[string]Column) error {
	createSchemaStatement := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dataset)
	var PrimaryKeys []string
	var FK []string
	createStatement := ""
	createStatement += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s(", dataset, table)

	var c []Column

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
	if s.updateColumns {
		return s.ColumnUpdater(ctx, dataset, table, columns)
	}
	return nil
}

func (s *SqlDB) QueryContext(ctx context.Context, query string, options *DBOptions, args interface{}) (DBRow, error) {
	if options == nil || !(options.NoLock || options.ReadPast) {
		return s.sql.NamedQueryContext(ctx, query, args)
	}
	tx, err := s.sql.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	_, err = tx.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("error setting transaction: %w", err)
	}

	rows, err := tx.NamedQuery(query, args)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing query: %w", err)
	}

	return rows, nil
}

func (s *SqlDB) RawQueryContext(ctx context.Context, query string, options *DBOptions, args ...interface{}) (DBRow, error) {
	defer func() { //catch or finally
		if err := recover(); err != nil { //catch
			fmt.Fprintf(os.Stderr, "Exception: %v\n", err)
			os.Exit(1)
		}
	}()
	if options == nil || !(options.NoLock || options.ReadPast) {
		return s.sql.QueryxContext(ctx, query, args...)
	}
	tx, err := s.sql.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	_, err = tx.Exec("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("error setting transaction: %w", err)
	}

	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing query: %w", err)
	}

	return rows, nil
}

func (s *SqlDB) ExecContext(ctx context.Context, query string, args interface{}) error {
	defer func() { //catch or finally
		if err := recover(); err != nil { //catch
			fmt.Fprintf(os.Stderr, "Exception: %v\n", err)
			os.Exit(1)
		}
	}()
	ctxLogger.Debug(ctx, "starting transaction")
	tx, err := s.sql.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	ctxLogger.Debug(ctx, "started transaction")
	_, err = tx.NamedExecContext(ctx, query, args)
	if err != nil {
		ctxLogger.Debug(ctx, "rolled back transaction")
		_ = tx.Rollback()
		return err
	}
	ctxLogger.Debug(ctx, "commit transaction")
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing query: %w", err)
	}
	ctxLogger.Debug(ctx, "finished transaction")

	return nil
}

func (s *SqlDB) ColumnUpdater(ctx context.Context, dataset, table string, columns map[string]Column) error {
	cols, err := getColumns(ctx, s.sql, dataset, table)
	if err != nil {
		return err
	}
	var addColumns []*Column
	var removeColumns []*sql.ColumnType
	colMap := map[string]*sql.ColumnType{}
	for _, c := range cols {
		colMap[c.Name()] = c
	}

	for _, e := range columns {
		if _, found := colMap[e.Name]; !found {
			addColumns = append(addColumns, &e)
		}
	}

	for _, c := range cols {
		if _, found := columns[c.Name()]; !found {
			removeColumns = append(removeColumns, c)
		}
	}

	alterTable := fmt.Sprintf("ALTER TABLE %s.%s ", dataset, table)

	if len(addColumns) > 0 {
		addStmt := generateColumnStatements(alterTable, "add", addColumns)
		ctxLogger.Debug(ctx, "adding columns to table", zap.String("query", addStmt))
		_, err := s.sql.ExecContext(ctx, addStmt)
		if err != nil {
			return err
		}
	}
	if len(removeColumns) > 0 {
		removeStmt := generateColumnTypeStatements(alterTable, "remove", removeColumns)
		ctxLogger.Debug(ctx, "removing columns from table", zap.String("table", table), zap.String("query", removeStmt))
		_, err := s.sql.ExecContext(ctx, removeStmt)
		if err != nil {
			return err
		}

	}
	return nil
}

func getColumns(ctx context.Context, db *sqlx.DB, dataset, table string) ([]*sql.ColumnType, error) {
	if db == nil {
		return nil, nil
	}
	rows, err := db.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s limit 1;", dataset, table))
	if err != nil {
		return nil, err
	}
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	return cols, nil
}

func generateColumnTypeStatements(alterTable, columnType string, e []*sql.ColumnType) string {
	output := []string{}
	for _, el := range e {
		output = append(output, generateColumnTypeStmt(columnType, el))
	}
	return fmt.Sprintf("%s %s;", alterTable, strings.Join(output, ","))

}

func generateColumnStatements(alterTable, columnType string, e []*Column) string {
	output := []string{}
	for _, el := range e {
		output = append(output, generateColumnStmt(columnType, el))
	}
	return fmt.Sprintf("%s %s;", alterTable, strings.Join(output, ","))

}
func generateColumnStmt(columnType string, e *Column) string {
	switch strings.ToLower(columnType) {
	case "drop":
		return fmt.Sprintf("DROP COLUMN %s;", e.Name)
	case "add":
		return fmt.Sprintf("ADD %s", e.GetDefinition())
	}
	return ""
}

func generateColumnTypeStmt(columnType string, e *sql.ColumnType) string {
	switch strings.ToLower(columnType) {
	case "drop":
		return fmt.Sprintf("DROP COLUMN %s", e.Name())
	case "add":
		return fmt.Sprintf("ADD %s", e.Name())
	}
	return ""
}

type IndexInfo struct {
	IndexName  string `db:"INDEX_NAME" json:"index_name"`
	ColumnName string `db:"COLUMN_NAME" json:"column_name"`
	NonUnique  int    `db:"NON_UNIQUE" json:"non_unique"`
	SeqInIndex int    `db:"SEQ_IN_INDEX" json:"seq_in_index"`
}

type ColumnInfo struct {
	ColumnName    string `db:"COLUMN_NAME" json:"column_name"`
	ColumnType    string `db:"COLUMN_TYPE" json:"column_type"`
	IsNullable    string `db:"IS_NULLABLE" json:"is_nullable"`
	ColumnKey     string `db:"COLUMN_KEY" json:"column_key"`
	ColumnDefault string `db:"COLUMN_DEFAULT" json:"column_default"`
	Extra         string `db:"EXTRA" json:"extra"`
}

func (s *SqlDB) GetTableDefinition(database string, tableName string) ([]ColumnInfo, error) {
	query := `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA
			  FROM information_schema.columns
			  WHERE table_schema = ? AND table_name = ?`

	var columns []ColumnInfo
	err := s.sql.Select(&columns, query, database, tableName)
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (s *SqlDB) GetTableIndexes(database, tableName string) ([]IndexInfo, error) {
	query := `SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX
			  FROM information_schema.statistics
			  WHERE table_schema = ? AND table_name = ?`

	var indexes []IndexInfo
	err := s.sql.Select(&indexes, query, database, tableName)
	if err != nil {
		return nil, err
	}

	return indexes, nil
}
