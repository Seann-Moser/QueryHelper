package QueryHelper

import (
	"fmt"
	"github.com/google/uuid"
	"testing"
)

type Resource struct {
	ID           string `json:"id" db:"id" qc:"primary;join;join_name::resource_id;group_by_modifier::count"` // ID ("resource.*")
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
	q.Select(q.Column("id").Wrap("distinct %s").As("did"), q.Column("data"))
	for _, permissions := range []string{"test_id", "test_id2"} {
		q.UniqueWhere(q.Column("id"), "REGEXP", "OR", 1, permissions, true)
	}
	q.Where(q.Column("id"), "=", "and", 0, nil)
	q.GroupBy(q.Column("data"), q.Column("id"))
	q.Build()
	println(q.Query)
	//	args := q.Args(nil)
	//table.Insert(context.Background(), nil, Resource{}, Resource{})

	//println(args)
}

type Answer struct {
	AccountID  string `json:"account_id" db:"account_id"`
	ID         string `json:"id" db:"id" qc:"primary;join;join_name::answer_id;auto_generate_id;group_by_modifier::count"`
	QuestionID string `json:"question_id" db:"question_id" qc:"primary;join;join_name::question_id;where"`
	SurveyID   string `json:"survey_id" db:"survey_id" qc:"primary;join;"`
	UID        string `json:"uid" db:"uid" qc:"primary;group_by_modifier::count"`

	FloatValue float64 `json:"float_value" db:"float_value" qc:"update"`
	IntValue   int     `json:"int_value" db:"int_value" qc:"update"`
	Value      string  `json:"value" db:"value" qc:"update;data_value::text"`

	Status           int    `json:"status" db:"status" qc:"update"`
	CreatedTimestamp string `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp"`
}

func TestQuery_BuildGroupBy(t *testing.T) {
	answerTable, err := NewTable[Answer]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}
	//q := QueryTable[Answer](answerTable)
	//q.Select(q.Column("uid").Wrap("distinct %s").As("id"), q.Column("survey_id")).
	//	Where(q.Column("survey_id"), "in", "AND", 0, strings.Join([]string{}, ",")).
	//	///		SetCache(ctx_cache.GetCacheFromContext(ctx)).
	//	GroupBy(q.Column("survey_id")).
	//	Build()
	//
	//qs := QueryTable[Answer](answerTable).
	//	Where(answerTable.GetColumn("uid"), "=", "AND", 1, "uid").
	//	Where(answerTable.GetColumn("question_id"), "=", "AND", 1, "q_id").
	//	Where(answerTable.GetColumn("survey_id"), "=", "AND", 1, "s_id").
	//	Build()
	//
	//println(qs.Query)
	qt := QueryTable[Answer](answerTable)
	qt.Select(qt.Column("uid").Wrap("count(distinct %s)").As("id")).SetName("test-anwser").
		Where(qt.Column("survey_id"), "=", "AND", 0, "test_id").
		Build()
	println(qt.Query)

	qttest := QueryTable[Answer](answerTable)
	qt.Select(qt.Column("uid").Wrap("count(distinct %s)").As("id")).SetName("test-anwser").
		Where(qt.Column("survey_id"), "=", "AND", 0, "test_id").
		Build()
	println(qttest.Query)
	//	args := q.Args(nil)
	//table.Insert(context.Background(), nil, Resource{}, Resource{})
	//
	//println(args)
}

func TestQuery_Upsert(t *testing.T) {
	answerTable, err := NewTable[Answer]("test", QueryTypeSQL)
	if err != nil {
		t.Fatal(err)
	}
	upsert := answerTable.UpsertStatement(10)
	println(upsert)

}

var table = []struct {
	input int
}{
	{input: 1},
	{input: 100},
}

func BenchmarkCombineStructs(b *testing.B) {
	for _, v := range table {
		anwers := make([]Answer, v.input)
		var output []interface{}
		for _, a := range anwers {
			a.UID = uuid.New().String()
			a.QuestionID = uuid.New().String()
			output = append(output, a)
		}
		b.Run(fmt.Sprintf("combining data_%d", v.input), func(b *testing.B) {
			_, _ = combineStructs(output...)
		})
	}

}

func BenchmarkCombineStructsInsert(b *testing.B) {
	answerTable, err := NewTable[Answer]("test", QueryTypeSQL)
	if err != nil {
		b.Fatal(err)
	}
	for _, v := range table {
		anwers := make([]Answer, v.input)
		for _, a := range anwers {
			a.UID = uuid.New().String()
			a.QuestionID = uuid.New().String()
		}
		b.Run(fmt.Sprintf("combining data_%d", v.input), func(b *testing.B) {
			generateIds := answerTable.GenerateID()
			args := map[string]interface{}{}
			for rowIndex, i := range anwers {
				tmpArgs, err := combineStructs(generateIds, i)
				if err != nil {
					return
				}
				tmpArgs = AddPrefix(fmt.Sprintf("%d_", rowIndex), tmpArgs)
				args, err = combineStructs(args, tmpArgs)
				if err != nil {
					return
				}
			}
		})
	}

}
