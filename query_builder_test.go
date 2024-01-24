package QueryHelper

import (
	"context"
	"testing"
)

type Resource struct {
	ID           string `json:"id" db:"id" qc:"primary;join;join_name::resource_id;"` // ID ("resource.*")
	Description  string `json:"description" db:"description" qc:"data_type::varchar(512);update"`
	ResourceType string `json:"resource_type" db:"resource_type" qc:"update"` // ResourceType "url"
	Data         string `json:"data" db:"data" qc:"update;text"`
	Public       bool   `json:"public" db:"public" qc:"default::false;update"`

	UpdatedTimestamp string `json:"updated_timestamp" db:"updated_timestamp" qc:"skip;default::updated_timestamp"`
	CreatedTimestamp string `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp"`
}

func TestQuery_Build(t *testing.T) {
	table, err := NewTable[Resource]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}
	q := QueryTable[Resource](table)
	for _, permissions := range []string{"test_id", "test_id2"} {
		q.UniqueWhere(q.Column("id"), "REGEXP", "OR", 1, permissions)
	}
	q.Build()
	println(q.Query)
	args := q.Args(nil)
	table.Insert(context.Background(), nil, Resource{}, Resource{})

	println(args)
}
