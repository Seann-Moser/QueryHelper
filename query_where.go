package QueryHelper

import (
	"fmt"
	"strings"
)

type WhereStmt struct {
	LeftValue    Column
	Conditional  string
	RightValue   interface{}
	Level        int
	JoinOperator string
	Index        int
	Flip         bool
}

func NewWhere(column Column, value interface{}) *WhereStmt {
	return &WhereStmt{
		LeftValue:    column,
		Conditional:  "=",
		RightValue:   value,
		Level:        0,
		JoinOperator: "AND",
	}
}

func (w *WhereStmt) GetArg() (string, interface{}) {
	if w.RightValue == nil {
		return "", nil
	}
	var suffix string
	if w.Index > 0 {
		suffix = fmt.Sprintf("_%d", w.Index)
	}
	return fmt.Sprintf("%s%s", w.LeftValue.Name, suffix), w.RightValue
}

func (w *WhereStmt) ToString() string {
	column := w.LeftValue
	if column.Name == "" {
		return ""
	}
	tmp := column.Where
	if w.Conditional != "" {
		tmp = w.Conditional
	} else if tmp == "" {
		tmp = "="
	}
	var suffix string
	if w.Index > 0 {
		suffix = fmt.Sprintf("_%d", w.Index)
	}

	var formatted string
	switch strings.TrimSpace(strings.ToLower(tmp)) {
	case "is not":
		if w.RightValue == nil {
			formatted = fmt.Sprintf("%s %s null", column.FullName(false, false), tmp)
		}
	case "is":
		if w.RightValue == nil {
			formatted = fmt.Sprintf("%s %s null", column.FullName(false, false), tmp)
		}
	case "not in":
		fallthrough
	case "in":
		formatted = fmt.Sprintf("%s %s (:%s%s)", column.FullName(false, false), tmp, column.Name, suffix)
	default:
		if w.Flip {
			formatted = fmt.Sprintf(":%s%s %s %s", column.Name, suffix, tmp, column.FullName(false, false))
		} else {
			formatted = fmt.Sprintf("%s %s :%s%s", column.FullName(false, false), tmp, column.Name, suffix)
		}

	}
	if strings.Contains(formatted, ":") {
		return formatted
	}
	return ""
}

func generateWhere(whereStatements []*WhereStmt) string {
	previousLevel := 0
	stmt := ""
	for i, w := range whereStatements {
		if where := w.ToString(); where != "" {

			if w.Level < previousLevel {
				stmt += fmt.Sprintf(" %s", generateList(")", previousLevel-w.Level))
			}
			if i > 0 {
				if w.JoinOperator == "" {
					w.JoinOperator = "AND"
				}
				stmt += " " + strings.ToUpper(w.JoinOperator)
			}

			if w.Level > previousLevel {
				stmt += fmt.Sprintf(" %s", generateList("(", w.Level-previousLevel))
			}

			stmt += fmt.Sprintf(" %s", w.ToString())
			previousLevel = w.Level

		}
	}
	if previousLevel > 0 {
		stmt += fmt.Sprintf(" %s", generateList(")", previousLevel))
	}
	return "WHERE " + stmt
}

func generateList(symbol string, count int) string {
	output := ""
	for i := 0; i < count; i++ {
		output += symbol
	}
	return output
}
