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
func (t *Table) SelectStatement(where ...string) string {
	var selectValues = t.GetSelectableElements(false)
	whereStmt := ""
	if len(where) > 0 {
		whereStmt = t.WhereStatement("AND", where...)
	}
	selectStmt := fmt.Sprintf("SELECT %s FROM %s.%s ", strings.Join(selectValues, ", "), t.Dataset, t.Name)
	selectStmt += whereStmt
	return selectStmt
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
					formatted := fmt.Sprintf("%s %s :%s", currentTable.FullElementName(element), tmp, element.Name)
					if strings.Contains(formatted, ".") {
						whereValues = append(whereValues, formatted)
					}
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
		if (selectCol == nil || len(selectCol) == 0) && len(selectValues) == 0 {
			selectValues = append(selectValues, validTable.GetSelectableElements(true)...)
		} else {
			for _, e := range validTable.GetSelectableElements(true) {
				for _, s := range selectCol {
					if _, found := dedupMap[e]; found {
						break
					}
					eleName := strings.TrimSpace(e[strings.Index(e, "AS")+2:])
					if _, found := dedupMap[eleName]; found {
						break
					}
					if strings.EqualFold(s, eleName) || strings.EqualFold(s, e) {
						selectValues = append(selectValues, e)
						dedupMap[eleName] = true
						dedupMap[e] = true
						break
					}
				}
			}

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
