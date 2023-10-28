package QueryHelper

import (
	"context"
	firebase "firebase.google.com/go/v4/db"
)

var _ DB = &FirebaseDB{}

type FirebaseDB struct {
	client *firebase.Client
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

func (f FirebaseDB) CreateTable(ctx context.Context, dataset, table string, columns map[string]*Column) error {
	//TODO implement me
	panic("implement me")
}

func (f FirebaseDB) QueryContext(ctx context.Context, query string, args interface{}) (DBRow, error) {
	//TODO implement me
	panic("implement me")
}

func (f FirebaseDB) ExecContext(ctx context.Context, query string, args interface{}) error {
	//TODO implement me
	panic("implement me")
}
