package QueryHelper

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

type FullTestStruct struct {
	ID               string     `json:"chapter_id" db:"chapter_id" qc:"where:=;delete;auto_generate_id;auto_generate_id_type:base64,join"`
	Public           bool       `json:"public" db:"public" qc:"default:true;primary"`
	BookID           string     `json:"book_id" db:"book_id" qc:"primary"`
	Number           int        `json:"chapter_number" db:"chapter_number" qc:"primary;update;order;group_by_modifier::count"`
	Language         string     `json:"language" db:"language" qc:"primary;update,order;order_priority:1"`
	Image            string     `json:"cover_image" db:"cover_image" qc:"update"`
	UpdatedTimestamp string     `db:"updated_timestamp" json:"updated_timestamp" qc:"skip;default:updated_timestamp" `
	CreatedTimestamp string     `db:"created_timestamp" json:"created_timestamp" qc:"skip;default:created_timestamp"`
	TestNull         NullString `db:"null_string" json:"test_null" qc:"null"`
}

type GuestRequests struct {
	ID        string `db:"id" json:"id" qc:"primary;where::=;join;join_name::book_id"`
	URLPath   string `db:"url_path" json:"url_path" qc:"primary;where:=;join;where_join::="`
	SubDomain string `db:"sub_domain" json:"sub_domain" qc:"primary;join"`
	Hits      int    `db:"value" json:"value" qc:"update"`
	Day       string `db:"day" json:"day" qc:"skip;date_type:DATE;update,default:NOW()"`
}

type Question struct {
	ID          string `json:"id"   db:"id" qc:"primary;join;join_name::question_id;auto_generate_id"`
	OptionsHash string `json:"options_hash" db:"options_hash" qc:"update;primary;data_type::varchar(512)"`

	Question string `json:"question"  db:"question" qc:"update"`
	Status   int    `json:"status"  db:"status" qc:"update"`

	CreatedTimestamp string `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp"`
	UpdatedTimestamp string `json:"updated_timestamp" db:"updated_timestamp" qc:"skip;default::updated_timestamp"`
}

type SurveyQuestions struct {
	SurveyID   string `json:"survey_id" db:"survey_id" qc:"primary;join;foreign_key::id;foreign_table::survey"`
	QuestionID string `json:"question_id" db:"question_id" qc:"primary;join;foreign_key::id;foreign_table::question"`
	Number     int    `json:"number" db:"number"`
}

type Log struct {
	ID               string `json:"id" db:"id" qc:"primary;join;join_name::audit_id;auto_generate_id;group_by_modifier::count"`
	AccountID        string `json:"account_id" db:"account_id" qc:"primary;join;join_name::account_id"`
	UserID           string `json:"user_id" db:"user_id" qc:"primary;data_type::varchar(512);join;join_name::user_id"`
	Service          string `json:"service" db:"service"`
	LogType          string `json:"log_type" db:"log_type"`
	Data             string `json:"data" db:"data" qc:"data_type::text"`
	CreatedTimestamp string `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp;group_by_modifier::DATE(*);group_by_name::created_date"`
}

type AuditLog struct {
	ID               string   `json:"id" db:"id" qc:"primary;join;join_name::audit_id;auto_generate_id;group_by_modifier::count"`
	AccountID        string   `json:"account_id" db:"account_id" qc:"primary;join;join_name::account_id"`
	UserID           string   `json:"user_id" db:"user_id" qc:"primary;data_type::varchar(512);join;join_name::user_id"`
	Service          string   `json:"service" db:"service"`
	Role             string   `json:"role" db:"role"`
	Path             string   `json:"path" db:"path"`
	Method           string   `json:"method" db:"method"`
	Latency          int64    `json:"latency" db:"latency" qc:"data_type::bigint"`
	StatusCode       int64    `json:"status_code" db:"status_code"`
	LogType          string   `json:"log_type" db:"log_type"`
	Answsers         []string `json:"answsers" db:"anwsers"`
	Data             string   `json:"data" db:"data" qc:"data_type::text"`
	CreatedTimestamp string   `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp;group_by_modifier::DATE(*);group_by_name::created_date"`
}

func TestNewTable(t *testing.T) {
	table, err := NewTable[FullTestStruct]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatal(err)
	}
	query := QueryTable[FullTestStruct](table).Select().Where(table.GetColumn("chapter_id"), "in", "", 0, nil).Build()
	println(query.Query)

	query = QueryTable[FullTestStruct](table).Select(table.GetColumn("book_id"), table.GetColumn("number")).Where(table.GetColumn("chapter_id"), "in", "", 2, nil).Where(table.GetColumn("language"), "=", "", 1, nil).GroupBy(table.GetColumn("book_id")).Build()

	_, _ = table.Insert(context.Background(), nil, FullTestStruct{}, FullTestStruct{}, FullTestStruct{})
	println(query.Query)

}

func TestTableJoin2(t *testing.T) {
	fullTable, err := NewTable[Question]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}

	GuestRequestsTable, err := NewTable[SurveyQuestions]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}
	_ = GuestRequestsTable.InitializeTable(context.Background(), MockDB{
		tables:   make(map[string]*mockTable),
		mockData: make(map[string]map[string]*mockData),
		prefix:   "qa_",
	})
	sql, err := fullTable.SelectJoinStmt("", nil, false, GuestRequestsTable.GetColumns())
	if err != nil {
		t.Fatal(err)
	}
	println(sql)

	questionsQuery := QueryTable[Question](fullTable).
		Join(GuestRequestsTable.GetColumns(), "").
		Where(GuestRequestsTable.GetColumn("survey_id"), "=", "AND", 0, "").
		Build()
	println(questionsQuery.Query)
}

func TestTableJoin(t *testing.T) {
	fullTable, err := NewTable[FullTestStruct]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}

	GuestRequestsTable, err := NewTable[GuestRequests]("requests", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}

	sql, err := fullTable.SelectJoinStmt("", nil, false, GuestRequestsTable.GetColumns())
	if err != nil {
		t.Fatal(err)
	}
	println(sql)

	query := QueryTable[FullTestStruct](fullTable).
		//Select(fullTable.GetColumn("book_id"), fullTable.GetColumn("number")).
		Join(GuestRequestsTable.GetColumns(), "LEFT").
		Where(fullTable.GetColumn("chapter_id"), "in", "", 2, nil).
		Where(fullTable.GetColumn("language"), "=", "", 1, nil).
		GroupBy(fullTable.GetColumn("book_id")).
		Build()
	println(query.Query)
}

func TestTableCtx(t *testing.T) {
	fullTable, err := NewTable[Log]("audit", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}

	auditQuery := QueryTable[Log](fullTable).
		Select(
			fullTable.GetColumn("id"),
			fullTable.GetColumn("account_id"),
			fullTable.GetColumn("log_type"),
			fullTable.GetColumn("created_timestamp"),
		).
		Where(fullTable.GetColumn("account_id"), "=", "AND", 0, "").
		GroupBy(fullTable.GetColumn("account_id"), fullTable.GetColumn("created_timestamp")).
		OrderBy(fullTable.GetColumn("created_timestamp"))
	auditQuery.Build()
	println(auditQuery.Query)

	auditQuery = QueryTable[Log](fullTable).
		Where(fullTable.GetColumn("account_id"), "=", "AND", 0, "").
		OrderBy(fullTable.GetColumn("created_timestamp")).Build()
	println(auditQuery.Query)
}

type Permissions struct {
	ID               string `json:"id" db:"id" qc:"primary;join;join_name::permission_id;auto_generate_id;"`
	Name             string `json:"name" db:"name" qc:"update"`
	System           string `json:"system" db:"system" qc:"update"`
	Path             string `json:"path" db:"path" qc:"primary;data_type::varchar(512);update"`
	Methods          string `json:"methods" db:"methods" qc:"primary;update"`
	UpdatedTimestamp string `json:"updated_timestamp" db:"updated_timestamp" qc:"skip;default::updated_timestamp"`
	CreatedTimestamp string `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp"`
	Public           bool   `json:"public" db:"public" qc:"default::false;update"`

	HandlerFunc http.HandlerFunc `db:"-" json:"-"`
}
type RolePermissions struct {
	RoleID           string `json:"role_id" db:"role_id" qc:"primary;join;foreign_key::id;foreign_table::role"`
	PermissionID     string `json:"permission_id" db:"permission_id" qc:"primary;join;foreign_key::id;foreign_table::permissions"`
	CreatedTimestamp string `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp"`
}

func TestTableWhereGroupingCtx(t *testing.T) {

	permissionsTable, err := NewTable[Permissions]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}

	rolePermissionsTable, err := NewTable[RolePermissions]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}

	permisisonsQuery := QueryTable[Permissions](permissionsTable).
		Join(rolePermissionsTable.GetColumns(), "LEFT").
		Where(rolePermissionsTable.GetColumn("role_id"), "in", "AND", 0, strings.Join([]string{}, ",")).
		Where(permissionsTable.GetColumn("service"), "in", "AND", 0, strings.Join([]string{"", "", "default"}, ",")).
		Where(permissionsTable.GetColumn("path"), "=", "AND", 0, "").
		Where(permissionsTable.GetColumn("public"), "=", "AND", 1, true).
		Where(rolePermissionsTable.GetColumn("role_id"), "is not", "OR", 1, nil).
		Build()
	println(permisisonsQuery.Query)

}

func TestQuery_GroupBy(t *testing.T) {
	auditTable, err := NewTable[AuditLog]("audit_log", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}
	err = NewMockDB().CreateTable(context.Background(), "test", auditTable.Name, auditTable.GetColumns())
	if err != nil {
		t.Fatal(err)
	}
	auditQuery := QueryTable[AuditLog](auditTable).
		Select(
			auditTable.GetColumn("id"),
			auditTable.GetColumn("account_id"),
			auditTable.GetColumn("log_type"),
			auditTable.GetColumn("created_timestamp"),
		).
		Where(auditTable.GetColumn("account_id"), "=", "AND", 0, "test").
		GroupBy(auditTable.GetColumn("account_id"), auditTable.GetColumn("created_timestamp"))

	auditQuery.Where(auditTable.GetColumn("created_timestamp"), ">=", "AND", 0, time.Now().Format("2006-01-02T15:04:05"))
	auditQuery.Build()
	_, err = auditQuery.Run(context.Background(), NewMockDB())
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	println(auditQuery.Query)

}

func TestTable_Prefix(t *testing.T) {
	auditTable, err := NewTable[AuditLog]("audit_log", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}
	print(auditTable.tmpPrefix)

	if auditTable.Prefix("hello").tmpPrefix != "hello" {
		t.Fatal("prefix failed")
	}
	if auditTable.tmpPrefix != "" {
		t.Fatal("prefix failed")
	}

}
