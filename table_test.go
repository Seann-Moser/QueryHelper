package QueryHelper

import (
	"context"
	"testing"
)

type FullTestStruct struct {
	ID               string `json:"chapter_id" db:"chapter_id" qc:"where:=;delete;auto_generate_id;auto_generate_id_type:base64,join"`
	Public           bool   `json:"public" db:"public" qc:"default:true;primary"`
	BookID           string `json:"book_id" db:"book_id" qc:"primary"`
	Number           int    `json:"chapter_number" db:"chapter_number" qc:"primary;update;order"`
	Language         string `json:"language" db:"language" qc:"primary;update,order;order_priority:1"`
	Image            string `json:"cover_image" db:"cover_image" qc:"update"`
	UpdatedTimestamp string `db:"updated_timestamp" json:"updated_timestamp" qc:"skip;default:updated_timestamp" `
	CreatedTimestamp string `db:"created_timestamp" json:"created_timestamp" qc:"skip;default:created_timestamp"`
}

type GuestRequests struct {
	ID        string `db:"id" json:"id" qc:"primary;where::=;join;join_name::book_id"`
	URLPath   string `db:"url_path" json:"url_path" qc:"primary;where:=;join;where_join::="`
	SubDomain string `db:"sub_domain" json:"sub_domain" qc:"primary;join"`
	Hits      int    `db:"value" json:"value" qc:"update"`
	Day       string `db:"day" json:"day" qc:"skip;date_type:DATE;update,default:NOW()"`
}

func TestNewTable(t *testing.T) {
	table, err := NewTable[FullTestStruct]("test")
	if err != nil {
		t.Fatal(err)
	}
	stmts, err := table.CreateMySqlTableStatement(false)
	for _, stmt := range stmts {
		println(stmt)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestTableJoin(t *testing.T) {
	fullTable, err := NewTable[FullTestStruct]("test")
	if err != nil {
		t.Fatal(err)
	}

	GuestRequestsTable, err := NewTable[GuestRequests]("requests")
	if err != nil {
		t.Fatal(err)
	}

	sql, err := fullTable.SelectJoinStmt("", nil, GuestRequestsTable.Columns)
	if err != nil {
		t.Fatal(err)
	}
	println(sql)
}

func TestTableCtx(t *testing.T) {
	ctx, err := AddTableCtx[FullTestStruct](context.Background(), nil, "test", false, true)
	if err != nil {
		t.Fatal(err)
	}
	table, err := GetTableCtx[FullTestStruct](ctx)
	if err != nil {
		t.Fatal(err)
	}
	println(table.Select(ctx, nil))
}
