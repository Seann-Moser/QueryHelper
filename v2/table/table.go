package table

import (
	"fmt"
	"strings"
)

type Table struct {
	Dataset  string    `json:"dataset"`
	Name     string    `json:"name"`
	Elements []*Config `json:"elements"`
}
type Config struct {
	Name         string
	Type         string `json:"type"`
	Default      string `json:"default"`
	Primary      bool   `json:"primary_key"`
	ForeignKey   string `json:"foreign_key"`
	ForeignTable string `json:"foreign_table"`
	Skip         bool   `json:"skip"`
	Update       bool   `json:"update"`
	Null         bool   `json:"null"`
	Select       bool   `json:"select"`
	Where        string `json:"where"`
	WhereJoin    string `json:"where_join"`
	JoinName     string `json:"join_name"`
	Join         bool   `json:"join"`
	Delete       bool   `json:"delete"`
}

func (t *Table) SelectableColumns(fullNames bool) string {
	var data []string
	for _, v := range t.Elements {
		if !(v.Select || v.Primary) {
			continue
		}
		if fullNames {
			data = append(data, t.FullElementName(v))
		} else {
			data = append(data, v.Name)
		}
	}
	return strings.Join(data, ",")
}
func (t *Table) FullTableName() string {
	return fmt.Sprintf("%s.%s", t.Dataset, t.Name)
}

func (t *Table) FullElementName(e *Config) string {
	return fmt.Sprintf("%s.%s", t.Name, e.Name)
}

func (t *Table) WhereStatement(conditional string, whereElementsStr ...string) string {
	var whereValues []string
	for _, i := range whereElementsStr {
		element := t.FindElementWithName(i)
		if element != nil {
			tmp := element.Where
			if element.Where == "" {
				tmp = "="
			}
			whereValues = append(whereValues, fmt.Sprintf("%s %s :%s", element.Name, tmp, element.Name))
		}
	}
	if len(whereValues) == 0 {
		return ""
	}
	if conditional == "" {
		conditional = "AND"
	}
	return fmt.Sprintf("WHERE %s", strings.Join(whereValues, fmt.Sprintf(" %s ", conditional)))

}

func (t *Table) FindElementWithName(name string) *Config {
	for _, e := range t.Elements {
		if strings.EqualFold(e.Name, name) {
			return e
		}
	}
	return nil
}

func (t *Table) FindCommonElementName(e2List *Table) ([]string, []string) {
	joinArr := []string{}
	var whereValues []string
	addedWhereValues := map[string]bool{}
	for _, e := range t.Elements {
		columnName := e.Name
		if e.JoinName != "" {
			columnName = e.JoinName
		}
		if !e.Join {
			continue
		}
		for _, e2 := range e2List.Elements {
			if (e2.Name == columnName || e2.JoinName == columnName) && e2.Join {
				joinArr = append(joinArr, fmt.Sprintf("%s = %s",
					e2List.FullElementName(e2),
					t.FullElementName(e),
				))
			} else {
				if _, found := addedWhereValues[e2.Name]; !found && len(e2.Where) > 0 && !e2.Join {
					addedWhereValues[e2.Name] = true
					whereValues = append(whereValues, fmt.Sprintf("%s %s :%s", e2List.FullElementName(e2), e2.Where, e2.Name))
				}
			}
		}
	}
	return joinArr, whereValues
}

func (t *Table) GetSelectableElements(fullNames bool) []string {
	var selectValues []string
	for _, e := range t.Elements {
		if e.Select {
			if fullNames {
				selectValues = append(selectValues, fmt.Sprintf("%s AS %s", t.FullElementName(e), e.Name)) //
			} else {
				selectValues = append(selectValues, e.Name)
			}

		}
	}
	return selectValues
}
