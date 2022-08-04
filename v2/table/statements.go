package table

import (
	"fmt"
	"strings"
)

func (t *Table) InsertStatement() string {
	var columnNames []string
	var values []string
	for _, e := range t.Elements {
		if e.Skip {
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

func (t *Table) UpdateStatement() string {
	var setValues []string
	var whereValues []string
	for _, e := range t.Elements {
		if e.Primary {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
			continue
		}
		if !e.Update {
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

func (t *Table) DeleteStatement() string {
	var whereValues []string
	for _, e := range t.Elements {
		if e.Primary {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
			continue
		}
		if e.Delete {
			return fmt.Sprintf("DELETE FROM %s WHERE %s = :%s", t.FullTableName(), e.Name, e.Name)
		}
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", t.FullTableName(), strings.Join(whereValues, " AND "))
}

func (t *Table) Count(conditional string, whereElementsStr ...string) string {
	wh := t.WhereStatement(conditional, whereElementsStr...)
	return fmt.Sprintf("SELECT COUNT(*) as count FROM %s %s", t.FullTableName(), wh)
}

func (t *Table) SelectJoin(selectCol, whereElementsStr []string, joinTables ...*Table) string {
	validTables := []*Table{t}
	joinStmts := []string{}
	var whereValues []string
	for _, currentTable := range joinTables {
		commonElements, wv := t.FindCommonElementName(currentTable)
		if len(commonElements) == 0 {
			continue
		}
		if whereElementsStr == nil || len(whereElementsStr) == 0 {
			whereValues = append(whereValues, wv...)
		} else {
			for _, i := range whereElementsStr {
				element := currentTable.FindElementWithName(i)
				if element != nil {
					tmp := element.Where
					if element.Where == "" {
						tmp = "="
					}
					whereValues = append(whereValues, fmt.Sprintf("%s %s :%s", currentTable.FullElementName(element), tmp, element.Name))
				}
			}
		}
		validTables = append(validTables, currentTable)
		joinStmt := fmt.Sprintf(" JOIN %s ON %s", currentTable.FullTableName(), strings.Join(commonElements, " AND "))
		joinStmts = append(joinStmts, joinStmt)
	}

	var selectValues []string
	var dedupMap map[string]bool
	dedupMap = map[string]bool{}
	for _, validTable := range validTables {
		if selectCol == nil || len(selectCol) == 0 {
			selectValues = append(selectValues, validTable.GetSelectableElements(true)...)
		} else {
			for _, e := range validTable.GetSelectableElements(true) {
				for _, s := range selectCol {
					eleName := strings.TrimSpace(e[strings.Index(e, "AS")+2:])
					if _, found := dedupMap[eleName]; found {
						break
					}
					if strings.EqualFold(s, eleName) {
						selectValues = append(selectValues, e)
						dedupMap[eleName] = true
						break
					}
				}
			}

		}
	}

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

	whereStmt := ""
	if len(whereValues) > 0 {
		whereStmt = fmt.Sprintf(" WHERE %s", strings.Join(whereValues, " AND "))
	}
	strings.Join(selectValues, ",")
	selectStmt := fmt.Sprintf("SELECT %s FROM %s %s %s", strings.Join(selectValues, ","), t.FullTableName(), strings.Join(joinStmts, " "), whereStmt)
	return selectStmt
}
