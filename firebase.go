package QueryHelper

import (
	"context"
)

var _ DB = &FirebaseDB{}

type FirebaseDB struct {
	//client *firebase.Client
}

func (f FirebaseDB) GetTableIndexes(database, tableName string) ([]IndexInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (f FirebaseDB) GetTableDefinition(database string, tableName string) ([]ColumnInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (f FirebaseDB) RawQueryContext(ctx context.Context, query string, options *DBOptions, args ...interface{}) (DBRow, error) {
	//TODO implement me
	panic("implement me")
}

func (f FirebaseDB) GetDataset(ds string) string {
	//TODO implement me
	panic("implement me")
}

func NewFirebaseDB() *FirebaseDB {
	return &FirebaseDB{}
}

func (f FirebaseDB) Close() {
	//TODO implement me

}

func (f FirebaseDB) Ping(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (f FirebaseDB) CreateTable(ctx context.Context, dataset, table string, columns map[string]Column) error {
	//TODO implement me
	panic("implement me")
}

func (f FirebaseDB) QueryContext(ctx context.Context, query string, options *DBOptions, args interface{}) (DBRow, error) {
	//TODO implement me
	panic("implement me")
}

func (f FirebaseDB) ExecContext(ctx context.Context, query string, args interface{}) error {
	//TODO implement me
	panic("implement me")
}
