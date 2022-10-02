package generator

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"strings"
	"testing"
)

type TestCase struct {
	Name                 string
	Struct               interface{}
	ExpectedSql          string
	ExpectedAutoGenerate bool
	AutoGeneratedKeys    []string
}
type GuestRequests struct {
	ID        string `db:"id" json:"id" q_config:"primary,where:="`
	URLPath   string `db:"url_path" json:"url_path" q_config:"primary,where:=,join"`
	SubDomain string `db:"sub_domain" json:"sub_domain" q_config:"primary,join"`
	Hits      int    `db:"value" json:"value" q_config:"update"`
	Day       string `db:"day" json:"day" q_config:"skip,date_type:DATE,update,default:NOW()"`
}

type FullTestStruct struct {
	ID               string `json:"chapter_id" db:"chapter_id" q_config:"where:=,delete,auto_generate_id,auto_generate_id_type:base64"`
	Public           bool   `json:"public" db:"public" q_config:"default:true"`
	BookID           string `json:"book_id" db:"book_id" q_config:"primary"`
	Number           int    `json:"chapter_number" db:"chapter_number" q_config:"primary,update"`
	Language         string `json:"language" db:"language" q_config:"primary,update"`
	Image            string `json:"cover_image" db:"cover_image" q_config:"update"`
	UpdatedTimestamp string `db:"updated_timestamp" json:"updated_timestamp" q_config:"skip,default:updated_timestamp" `
	CreatedTimestamp string `db:"created_timestamp" json:"created_timestamp" q_config:"skip,default:created_timestamp"`
}

func TestGenerator_CreateMySqlTable(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	gen := New(logger)
	tcs := []*TestCase{
		{
			Name:   "GuestRequest",
			Struct: GuestRequests{},
			ExpectedSql: `CREATE SCHEMA IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.GuestRequests(
	id varchar(256) NOT NULL,
	url_path varchar(256) NOT NULL,
	sub_domain varchar(256) NOT NULL,
	value int NOT NULL,
	day varchar(256) NOT NULL DEFAULT NOW(),
	CONSTRAINT PK_test_test PRIMARY KEY (id,url_path,sub_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`,
		},
		{
			Name:                 "FullTest",
			Struct:               FullTestStruct{},
			ExpectedAutoGenerate: true,
			AutoGeneratedKeys:    []string{"chapter_id"},
			ExpectedSql: `CREATE SCHEMA IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.FullTestStruct(
	chapter_id varchar(256) NOT NULL,
	public tinyint(1) NOT NULL DEFAULT true,
	book_id varchar(256) NOT NULL,
	chapter_number int NOT NULL,
	language varchar(256) NOT NULL,
	cover_image varchar(256) NOT NULL,
	updated_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	created_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
	CONSTRAINT PK_test_test PRIMARY KEY (book_id,chapter_number,language)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			table, err := gen.Table("test", tc.Struct)
			if err != nil {
				t.Error(err)
			}
			sqlStmt := gen.MySqlTable(table)
			assert.Equal(t, tc.ExpectedSql, sqlStmt)
			assert.Equal(t, tc.ExpectedAutoGenerate, table.IsAutoGenerateID())
			if table.IsAutoGenerateID() {
				generatedMap := table.GenerateID()
				assert.NotEmpty(t, tc.AutoGeneratedKeys)
				for _, autoKey := range tc.AutoGeneratedKeys {
					found := false
					for k, v := range generatedMap {
						if strings.EqualFold(k, autoKey) {
							found = true
							println(v)
							break
						}
					}
					if !found {
						t.Errorf("missing auto generated key: %s", autoKey)
					}
				}
			}
		})
	}
}
