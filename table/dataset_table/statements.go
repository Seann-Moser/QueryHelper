package dataset_table

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
)

type Statements interface {
	InsertStatement() string
	SelectStatement(conditional string, cols []string, where ...string) string
	UpdateStatement() string
	DeleteStatement() string
	CountStatement(conditional string, whereElementsStr ...string) string
	SelectJoin(selectCol, whereElementsStr []string, joinTables ...Table) string
	IsAutoGenerateID() bool
	GenerateID() map[string]string
	GetGenerateID() []*Element
}

func (t *DefaultTable) IsAutoGenerateID() bool {
	for _, e := range t.Elements {
		if e.AutoGenerateID {
			return true
		}
	}
	return false
}
func (t *DefaultTable) GetGenerateID() []*Element {
	var output []*Element
	for _, e := range t.Elements {
		if e.AutoGenerateID {
			output = append(output, e)
		}
	}
	return output
}
func (t *DefaultTable) GenerateID() map[string]string {
	m := map[string]string{}
	for _, e := range t.GetGenerateID() {
		uid := uuid.New().String()
		switch e.AutoGenerateIDType {
		case "hex":
			hasher := sha1.New()
			hasher.Write([]byte(uid))
			m[e.Name] = hex.EncodeToString(hasher.Sum(nil))
		case "base64":
			hasher := sha1.New()
			hasher.Write([]byte(uid))
			m[e.Name] = base64.URLEncoding.EncodeToString(hasher.Sum(nil))
		case "uuid":
			fallthrough
		default:
			m[e.Name] = uid
		}
	}
	return m
}
func (t *DefaultTable) InsertStatement() string {
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

func (t *DefaultTable) SelectStatement(conditional string, cols []string, where ...string) string {
	var selectValues = []string{}
	if cols == nil || len(cols) == 0 {
		selectValues = t.GetSelectableElements(false)
	} else {
		for _, c := range cols {
			e := t.FindElementWithName(c)
			if e != nil {
				continue
			}
			selectValues = append(selectValues, t.FullElementName(e))
		}
		selectValues = cols
	}
	whereStmt := ""
	if len(where) > 0 {
		whereStmt = t.WhereStatement(conditional, where...)
	}
	selectStmt := fmt.Sprintf("SELECT %s FROM %s.%s ", strings.Join(selectValues, ", "), t.Dataset, t.Name)
	selectStmt += whereStmt
	selectStmt += t.getOrder()
	return selectStmt
}

func (t *DefaultTable) getOrder() string {
	order := []*Element{}
	for _, e := range t.Elements {
		if e.Order {
			order = append(order, e)
		}
	}
	if len(order) == 0 {
		return ""
	}
	sort.Slice(order, func(i, j int) bool {
		return order[i].OrderPriority > order[j].OrderPriority
	})
	output := []string{}
	for _, e := range order {
		v := t.FullElementName(e)
		if !e.OrderAsc {
			v += " DESC"
		}
		output = append(output, v)
	}

	return fmt.Sprintf(" ORDER BY %s ", strings.Join(output, ","))

}
func (t *DefaultTable) UpdateStatement() string {
	var setValues []string
	var whereValues []string
	for _, e := range t.Elements {
		if e.Primary {
			whereValues = append(whereValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
		}
		if !e.Update {
			continue
		}
		setValues = append(setValues, fmt.Sprintf("%s = :%s", e.Name, e.Name))
	}
	if len(setValues) == 0 {
		return ""
	}
	update := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		t.FullTableName(),
		strings.Join(setValues, " ,"), strings.Join(whereValues, " AND "))
	return update
}

func (t *DefaultTable) DeleteStatement() string {
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

// CountStatement will return a sql statement to find the counts of a table
func (t *DefaultTable) CountStatement(conditional string, whereElementsStr ...string) string {
	wh := t.WhereStatement(conditional, whereElementsStr...)
	return fmt.Sprintf("SELECT COUNT(*) as count FROM %s %s", t.FullTableName(), wh)
}

// SelectJoin returns a select join statement to the joinTables
//
// It will join tables based off the q_config provided, join, and join_name
// where statements will be included if they are matched to either of the tables provided including itself
//
// selectCol if nil will return all the columns in the current table
// if not it will return only the selected columns if they are found
func (t *DefaultTable) SelectJoin(selectCol, whereElementsStr []string, joinTables ...Table) string {
	validTables := []Table{t}
	var joinStmts []string
	whereValues := t.WhereValues(whereElementsStr...)
	for _, currentTable := range joinTables {
		whereValues = append(whereValues, currentTable.WhereValues(whereElementsStr...)...)
		commonElements, _ := t.FindCommonElementName(currentTable)
		if len(commonElements) == 0 {
			continue
		}
		validTables = append(validTables, currentTable)
		joinStmt := fmt.Sprintf(" JOIN %s ON %s", currentTable.FullTableName(), strings.Join(commonElements, " AND "))
		joinStmts = append(joinStmts, joinStmt)
	}

	var selectValues []string
	dedupMap := map[string]bool{}
	for _, validTable := range validTables {
		if (len(selectCol) == 0) && len(selectValues) == 0 {
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
	var wv []string
	dedupWhereMap := map[string]bool{}
	for _, w := range whereValues {
		if _, found := dedupWhereMap[w]; found {
			continue
		}
		wv = append(wv, w)
		dedupWhereMap[w] = true
	}
	whereStmt := ""
	if len(wv) > 0 {
		whereStmt = fmt.Sprintf(" WHERE %s", strings.Join(wv, " AND "))
	}
	strings.Join(selectValues, ",")
	selectStmt := fmt.Sprintf("SELECT %s FROM %s %s %s", strings.Join(selectValues, ","), t.FullTableName(), strings.Join(joinStmts, " "), whereStmt)
	return selectStmt
}
