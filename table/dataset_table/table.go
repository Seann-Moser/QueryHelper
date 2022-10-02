package dataset_table

import (
	"fmt"
	"strings"
)

type Info interface {
	FullTableName() string
	GetElements() []*Element
	GetDataset() string
	GetTableName() string
	FullElementName(e *Element) string
	WhereStatement(conditional string, whereElementsStr ...string) string
	FindElementWithName(name string) *Element
	GetSelectableElements(fullNames bool) []string
	FindCommonElementName(e2List Tables) ([]string, []string)
}

func (t *Table) GetDataset() string {
	return t.Dataset
}
func (t *Table) GetTableName() string {
	return t.Dataset
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
func (t *Table) GetElements() []*Element {
	return t.Elements
}
func (t *Table) FullTableName() string {
	return fmt.Sprintf("%s.%s", t.Dataset, t.Name)
}

func (t *Table) FullElementName(e *Element) string {
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
			formatted := fmt.Sprintf("%s %s :%s", t.FullElementName(element), tmp, element.Name)
			if strings.Contains(formatted, ".") {
				whereValues = append(whereValues, formatted)
			}
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

func (t *Table) FindElementWithName(name string) *Element {
	for _, e := range t.Elements {
		if strings.EqualFold(e.Name, name) {
			return e
		}
	}
	return nil
}

func (t *Table) FindCommonElementName(e2List Tables) ([]string, []string) {
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
		for _, e2 := range e2List.GetElements() {
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
