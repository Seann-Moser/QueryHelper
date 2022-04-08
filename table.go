package QueryHelper

import (
	"encoding/json"
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
	CanUpdate  bool
	CanBeNull  bool
	Selectable bool
	Where      string
	JoinName   string
	Joinable   bool
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
		if !e.CanUpdate {
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

func (t *Table) GenerateNamedSelectStatement() string {
	var selectValues []string
	var whereValues []string
	for _, e := range t.Elements {
		if len(e.Where) > 0 {
			whereValues = append(whereValues, fmt.Sprintf("%s %s :%s", e.Name, e.Where, e.Name))
		}
		if e.Selectable {
			selectValues = append(selectValues, e.Name)
		}
	}
	whereStmt := ""
	if len(whereValues) > 0 {
		whereStmt = fmt.Sprintf(" WHERE %s", strings.Join(whereValues, " AND "))
	}
	selectStmt := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(selectValues, ", "), t.Dataset, t.Name)
	selectStmt += whereStmt
	return selectStmt
}

func (t *Table) GenerateNamedSelectJoinStatement(joinTables ...*Table) string {
	validTables := []*Table{t}
	joinStmts := []string{}
	var whereValues []string
	for _, currentTable := range joinTables {
		commonElements, wv := t.FindCommonElementName(currentTable)
		if len(commonElements) == 0 {
			continue
		}
		whereValues = append(whereValues, wv...)
		validTables = append(validTables, currentTable)
		joinStmt := fmt.Sprintf(" JOIN %s ON %s", currentTable.FullTableName(), strings.Join(commonElements, " AND "))
		joinStmts = append(joinStmts, joinStmt)
	}
	var selectValues []string
	for _, validTable := range validTables {
		selectValues = append(selectValues, validTable.GetSelectableElements(true)...)
	}

	for _, e := range t.Elements {
		if len(e.Where) > 0 {
			whereValues = append(whereValues, fmt.Sprintf("%s %s :%s", t.FullElementName(e), e.Where, e.Name))
		}
	}
	whereStmt := ""
	if len(whereValues) > 0 {
		whereStmt = fmt.Sprintf(" WHERE %s", strings.Join(whereValues, " AND "))
	}
	selectStmt := fmt.Sprintf("SELECT %s FROM %s %s %s", strings.Join(t.GetSelectableElements(true), ", "), t.FullTableName(), strings.Join(joinStmts, " "), whereStmt)
	return selectStmt
}

func (t *Table) GetSelectableElements(fullNames bool) []string {
	var selectValues []string
	for _, e := range t.Elements {
		if e.Selectable {
			if fullNames {
				selectValues = append(selectValues, t.FullElementName(e))
			} else {
				selectValues = append(selectValues, e.Name)
			}

		}
	}
	return selectValues
}
func (t *Table) FullTableName() string {
	return fmt.Sprintf("%s.%s", t.Dataset, t.Name)
}

func (t *Table) FullElementName(e *Elements) string {
	return fmt.Sprintf("%s.%s", t.Name, e.Name)
}
func CombineStructs(i ...interface{}) (map[string]interface{}, error) {
	output := map[string]interface{}{}
	for _, s := range i {
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		t := map[string]interface{}{}
		err = json.Unmarshal(b, &t)
		if err != nil {
			return nil, err
		}
		output = joinMaps(output, t)
	}
	return output, nil
}
func joinMaps(m ...map[string]interface{}) map[string]interface{} {
	output := map[string]interface{}{}
	for _, currentMap := range m {
		for k, v := range currentMap {
			if _, found := output[k]; !found {
				output[k] = v
			}
		}
	}
	return output
}
func (t *Table) FindCommonElementName(e2List *Table) ([]string, []string) {
	joinArr := []string{}
	var whereValues []string
	for _, e := range t.Elements {
		columnName := e.Name
		if e.JoinName != "" {
			columnName = e.JoinName
		}
		if !e.Joinable {
			continue
		}
		for _, e2 := range e2List.Elements {
			if !e2.Joinable {
				continue
			}
			if e2.Name == columnName || e2.JoinName == columnName {
				joinArr = append(joinArr, fmt.Sprintf("%s = %s",
					e2List.FullElementName(e2),
					t.FullElementName(e),
				))
			} else {
				if len(e.Where) > 0 {
					whereValues = append(whereValues, fmt.Sprintf("%s %s :%s", t.FullElementName(e), e.Where, e.Name))
				}
			}
		}
	}
	return joinArr, whereValues
}
