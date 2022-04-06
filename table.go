package QueryHelper

import (
	"fmt"
	"strings"
)

type Table struct {
	Dataset  string
	Name     string
	Elements []*Elements
}
type Elements struct {
	Name       string
	Type       string
	Default    string
	PrimaryKey bool
	SkipInsert bool
	SkipUpdate bool
	NotNull    bool
}

func (t *Table) GenerateNamedInsertStatement() string {
	var columnNames []string
	var values []string
	for _, e := range t.Elements {
		if e.SkipInsert {
			continue
		}
		columnNames = append(columnNames, e.Name)
		values = append(values, ":"+e.Name)
	}
	if len(columnNames) == 0 {
		return ""
	}
	insert := fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES(%s);",
		t.Dataset,
		t.Name,
		strings.Join(columnNames, ","), strings.Join(values, ","))
	return insert
}
func (t *Table) GenerateNamedUpdateStatement() string {
	var setValues []string
	var whereValues []string

	for _, e := range t.Elements {
		if e.PrimaryKey {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
			continue
		}
		if e.SkipUpdate {
			continue
		}
		setValues = append(setValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))

	}
	if len(setValues) == 0 {
		return ""
	}
	update := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		t.Dataset,
		t.Name,
		strings.Join(setValues, " ,"), strings.Join(whereValues, " AND "))
	return update
}
