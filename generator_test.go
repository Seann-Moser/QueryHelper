package QueryHelper

import (
	"github.com/tj/assert"
	"testing"
)

type Test struct {
	Name        string `db:"name" default:"jon smith" table:"primary"`
	UserName    string `db:"user_name" update:"true" null:"true"`
	CreatedDate string `db:"created_date" default:"NOW()" data_type:"timestamp" table:"skip_insert"`
}

func TestTable_GenerateNamedUpdateStatement(t *testing.T) {
	newTable, err := GenerateTableFromStruct("default_db", Test{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "UPDATE default_db.Test SET user_name = :user_name WHERE name = :name", newTable.GenerateNamedUpdateStatement())
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
				SkipUpdate: true,
				NotNull:    true,
			},
			{
				Name:       "user_name",
				Type:       TableTypeVarChar,
				Default:    "",
				PrimaryKey: false,
				SkipInsert: false,
				SkipUpdate: false,
				NotNull:    true,
			},
			{
				Name:       "created_date",
				Type:       TableTime,
				Default:    "NOW()",
				PrimaryKey: false,
				SkipInsert: true,
				SkipUpdate: true,
				NotNull:    true,
			},
		},
	}, newTable)
}
