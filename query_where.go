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
	var builder strings.Builder
	builder.WriteString("WHERE ")

	previousLevel := 0

	for i, w := range whereStatements {
		if where := w.ToString(); where != "" {
			// Handle level changes
			if w.Level < previousLevel {
				for j := 0; j < previousLevel-w.Level; j++ {
					builder.WriteString(")")
				}
			}

			if i > 0 {
				if w.JoinOperator == "" {
					w.JoinOperator = "AND"
				}
				builder.WriteString(" ")
				builder.WriteString(strings.ToUpper(w.JoinOperator))
			}

			if w.Level > previousLevel {
				for j := 0; j < w.Level-previousLevel; j++ {
					builder.WriteString("(")
				}
			}

			builder.WriteString(" ")
			builder.WriteString(where)
			previousLevel = w.Level
		}
	}

	if previousLevel > 0 {
		for j := 0; j < previousLevel; j++ {
			builder.WriteString(")")
		}
	}

	return builder.String()
}
