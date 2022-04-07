package QueryHelper

import (
	"github.com/tj/assert"
	"testing"
)

type Test struct {
	Name        string `db:"name" default:"jon smith" table:"primary"`
	UserName    string `db:"user_name" update:"true" can_be_null:"false" can_update:"true"`
	CreatedDate string `db:"created_date" default:"NOW()" data_type:"timestamp" table:"skip_insert"`
	Password    string `db:"password" selectable:"false" where:"="`
	UpdatedDate string `db:"updated_date" default:"" data_type:"timestamp" table:"skip_insert" can_be_null:"true" can_update:"true"`
	Active      bool   `db:"active" default:"true" can_update:"true" where:"="`
}

func TestTable_GenerateNamedSelectStatement(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "SELECT name, user_name, created_date, updated_date, active FROM default_db.Test WHERE password = :password AND active = :active", newTable.GenerateNamedSelectStatement())
}
func TestTable_GenerateNamedUpdateStatement(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "UPDATE default_db.Test SET user_name = :user_name ,updated_date = :updated_date WHERE name = :name", newTable.GenerateNamedUpdateStatement())
}
func TestTable_GenerateNamedInsertStatement(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	//err = CreateMySqlTable(ctx,db,newTable)
	assert.Equal(t, "INSERT INTO default_db.Test(name,user_name) VALUES(:name,:user_name);", newTable.GenerateNamedInsertStatement())

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
				Name:       "name",
				Type:       TableTypeVarChar,
				Default:    "\"jon smith\"",
				PrimaryKey: true,
				SkipInsert: false,
				CanUpdate:  false,
				CanBeNull:  false,
			},
			{
				Name:       "user_name",
				Type:       TableTypeVarChar,
				Default:    "",
				PrimaryKey: false,
				SkipInsert: false,
				CanUpdate:  true,
				CanBeNull:  false,
			},
			{
				Name:       "created_date",
				Type:       TableTime,
				Default:    "NOW()",
				PrimaryKey: false,
				SkipInsert: true,
				CanUpdate:  false,
				CanBeNull:  false,
			},
			{
				Name:       "updated_date",
				Type:       TableTime,
				Default:    "",
				PrimaryKey: false,
				SkipInsert: true,
				CanUpdate:  true,
				CanBeNull:  true,
			},
		},
	}, newTable)
}
