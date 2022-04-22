package QueryHelper

import (
	"github.com/tj/assert"
	"testing"
)

type Test struct {
	UserID      string `db:"user_id" join_name:"id"`
	Name        string `db:"name" default:"jon smith" table:"primary"`
	UserName    string `db:"user_name" update:"true" can_be_null:"false" can_update:"true"`
	CreatedDate string `db:"created_date" default:"NOW()" data_type:"timestamp" table:"skip_insert"`
	Password    string `db:"password" selectable:"false" where:"="`
	UpdatedDate string `db:"updated_date" default:"" data_type:"timestamp" table:"skip_insert" can_be_null:"true" can_update:"true"`
	Active      bool   `db:"active" default:"true" can_update:"true" where:"="`
	TestStruct  Test2
}

type Test2 struct {
	TestID string `db:"test_id" join_name:"id"`
	Name   string `db:"name"  table:"primary" where:"=" joinable:"false"`
	Active bool   `db:"active" default:"true" can_update:"true" joinable:"false" where:"="`
}

func TestTable_GenerateNamedSelectJoinStatement(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	newTable2, err := GenerateTableFromStruct("default_dbs", Test2{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "SELECT Test.user_id, Test.name, Test.user_name, Test.created_date, Test.updated_date, Test.active FROM default_db.Test  JOIN default_dbs.Test2 ON Test2.test_id = Test.user_id  WHERE Test2.name = :name AND Test2.active = :active AND Test.password = :password AND Test.active = :active", newTable.GenerateNamedSelectJoinStatement(newTable2))
}

func TestTable_GenerateNamedSelectStatement(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "SELECT user_id, name, user_name, created_date, updated_date, active FROM default_db.Test WHERE password = :password AND active = :active", newTable.GenerateNamedSelectStatement())
}
func TestTable_GenerateNamedUpdateStatement(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "UPDATE default_db.Test SET user_name = :user_name ,updated_date = :updated_date ,active = :active WHERE name = :name", newTable.GenerateNamedUpdateStatement())
}
func TestTable_GenerateNamedInsertStatement(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	//err = CreateMySqlTable(ctx,db,newTable)
	assert.Equal(t, "INSERT INTO default_db.Test(user_id,name,user_name,password,active) VALUES(:user_id,:name,:user_name,:password,:active);", newTable.GenerateNamedInsertStatement())

}
func TestGenerateTableFromStruct(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, &Table{
		Dataset: "default_db",
		Name:    "Test",
		Elements: []*Elements{
			{
				Name:       "user_id",
				Type:       TableTypeVarChar,
				Default:    "",
				PrimaryKey: false,
				SkipInsert: false,
				CanUpdate:  false,
				CanBeNull:  false,
				Selectable: true,
				JoinName:   "id",
				Joinable:   true,
			},
			{
				Name:       "name",
				Type:       TableTypeVarChar,
				Default:    "\"jon smith\"",
				PrimaryKey: true,
				SkipInsert: false,
				CanUpdate:  false,
				CanBeNull:  false,
				Selectable: true,
				Joinable:   true,
			},
			{
				Name:       "user_name",
				Type:       TableTypeVarChar,
				Default:    "",
				PrimaryKey: false,
				SkipInsert: false,
				CanUpdate:  true,
				CanBeNull:  false,
				Selectable: true,
				Joinable:   true,
			},

			{
				Name:       "created_date",
				Type:       TableTime,
				Default:    "NOW()",
				PrimaryKey: false,
				SkipInsert: true,
				CanUpdate:  false,
				Selectable: true,
				CanBeNull:  false,
				Joinable:   true,
			},
			{
				Name:       "password",
				Type:       TableTypeVarChar,
				Default:    "",
				PrimaryKey: false,
				SkipInsert: false,
				CanUpdate:  false,
				CanBeNull:  false,
				Selectable: false,
				Where:      "=",
				Joinable:   true,
			},
			{
				Name:       "updated_date",
				Type:       TableTime,
				Default:    "",
				PrimaryKey: false,
				SkipInsert: true,
				CanUpdate:  true,
				CanBeNull:  true,
				Selectable: true,
				Joinable:   true,
			},
			{
				Name:       "active",
				Type:       TableTypeBool,
				Default:    "true",
				PrimaryKey: false,
				SkipInsert: false,
				CanUpdate:  true,
				CanBeNull:  false,
				Where:      "=",
				Selectable: true,
				Joinable:   true,
			},
		},
	}, newTable)
}
