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
	WhereValues(whereElementsStr ...string) []string
	WhereStatement(conditional string, whereElementsStr ...string) string
	FindElementWithName(name string) *Element
	GetSelectableElements(fullNames bool) []string
	FindCommonElementName(e2List Table) ([]string, []string)
}

func (t *DefaultTable) GetDataset() string {
	return t.Dataset
}
func (t *DefaultTable) GetTableName() string {
	return t.Dataset
}
func (t *DefaultTable) SelectableColumns(fullNames bool) string {
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
func (t *DefaultTable) GetElements() []*Element {
	return t.Elements
}
func (t *DefaultTable) FullTableName() string {
	return fmt.Sprintf("%s.%s", t.Dataset, t.Name)
}

func (t *DefaultTable) FullElementName(e *Element) string {
	return fmt.Sprintf("%s.%s", t.Name, e.Name)
}

func (t *DefaultTable) WhereValues(whereElementsStr ...string) []string {
	var whereValues []string
	for _, i := range whereElementsStr {
		element := t.FindElementWithName(i)
		if element != nil {
			tmp := element.Where
			if element.Where == "" {
				tmp = "="
			}
			var formatted string
			switch strings.TrimSpace(strings.ToLower(tmp)) {
			case "not in":
				fallthrough
			case "in":
				formatted = fmt.Sprintf("%s %s (:%s)", t.FullElementName(element), tmp, element.Name)
			default:
				formatted = fmt.Sprintf("%s %s :%s", t.FullElementName(element), tmp, element.Name)
			}
			if strings.Contains(formatted, ".") {
				whereValues = append(whereValues, formatted)
			}
		}
	}
	return whereValues

}
func (t *DefaultTable) WhereStatement(conditional string, whereElementsStr ...string) string {
	whereValues := t.WhereValues(whereElementsStr...)
	if len(whereValues) == 0 {
		return ""
	}
	if conditional == "" {
		conditional = "AND"
	}
	return fmt.Sprintf("WHERE %s", strings.Join(whereValues, fmt.Sprintf(" %s ", conditional)))

}

func (t *DefaultTable) FindElementWithName(name string) *Element {
	for _, e := range t.Elements {
		if strings.EqualFold(e.Name, name) {
			return e
		}
	}
	return nil
}

func (t *DefaultTable) FindCommonElementName(e2List Table) ([]string, []string) {
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
			e2ColName := e2.Name
			if e2.JoinName != "" {
				e2ColName = e2.JoinName
			}
			if strings.EqualFold(e2ColName, columnName) && e2.Join {
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

func (t *DefaultTable) GetSelectableElements(fullNames bool) []string {
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
