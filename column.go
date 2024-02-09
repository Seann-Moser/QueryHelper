package QueryHelper

import (
	"fmt"
	"strings"
)

type Column struct {
	Name        string `json:"name"`
	Table       string `json:"-"`
	Dataset     string `json:"-"`
	ColumnOrder int    `json:"-"`

	Primary           bool   `json:"primary"`
	Skip              bool   `json:"skip"`
	Update            bool   `json:"update"`
	Null              bool   `json:"null"`
	Select            bool   `json:"select"`
	Delete            bool   `json:"delete"`
	AutoGenerateID    bool   `json:"auto_generate_id"`
	Join              bool   `json:"join"`
	OrderAsc          bool   `json:"order_asc"`
	Order             bool   `json:"order"`
	ForceGroupByValue bool   `json:"force_group_by_value"`
	GroupByModifier   string `json:"group_by_modifier"`
	GroupByName       string `json:"group_by_name"`
	GroupByColumn     bool   `json:"group_by_column"`
	OrderPriority     int    `json:"order_priority"`

	Type    string `json:"data_type"`
	Default string `json:"default"`

	ForeignKey    string `json:"foreign_key"`
	ForeignTable  string `json:"foreign_table"`
	ForeignSchema string `json:"foreign_schema"`
	WhereJoin     string `json:"where_join"`
	Where         string `json:"where"`
	JoinName      string `json:"join_name"`

	AutoGenerateIDType string `json:"auto_generate_id_type"`
	Wrapper            string `json:"wrapper"`
	SelectAs           string `json:"as"`
}

func (c *Column) Wrap(wrap string) *Column {
	c.Wrapper = wrap
	return c
}

func (c *Column) As(as string) *Column {
	c.SelectAs = as
	return c
}

func (c *Column) GetDefinition() string {
	elementString := fmt.Sprintf("\n\t%s %s", c.Name, c.Type)
	if !c.Null {
		elementString += " NOT NULL"
	}
	switch c.Default {
	case "created_timestamp":
		elementString += " DEFAULT NOW()"
	case "updated_timestamp":
		elementString += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
	case "":
		if c.Null {
			elementString += " DEFAULT NULL"
		}
	default:
		elementString += fmt.Sprintf(" DEFAULT %s", c.Default)
	}
	return elementString
}

func (c *Column) HasFK() bool {
	return len(c.ForeignKey) > 0 && len(c.ForeignTable) > 0
}

func (c *Column) FullName(groupBy bool) string {
	name := fmt.Sprintf("%s.%s", c.Table, c.Name)
	if c.Wrapper != "" {
		name = fmt.Sprintf(c.Wrapper, name)
	}

	if groupBy && len(c.GroupByModifier) > 0 {
		if strings.Contains(c.GroupByModifier, "*") {
			name = strings.ReplaceAll(c.GroupByModifier, "*", name)
		} else {
			name = fmt.Sprintf("%s(%s.%s)", c.GroupByModifier, c.Table, c.Name)
		}
		if c.SelectAs == "" {
			if c.GroupByName != "" {
				name = fmt.Sprintf("%s AS %s", name, c.GroupByName)
			} else {
				name = fmt.Sprintf("%s AS %s", name, c.Name)
			}
		}
	}
	if c.SelectAs != "" {
		name = fmt.Sprintf("%s AS %s", name, c.SelectAs)
	}
	return name
}

func (c *Column) FullTableName() string {
	return fmt.Sprintf("%s.%s", c.Dataset, c.Table)
}

func (c *Column) GetFKReference() string {
	if !c.HasFK() {
		return ""
	}
	reference := ""
	if len(c.ForeignSchema) > 0 {
		reference = c.ForeignSchema + "."
	}
	reference += fmt.Sprintf("%s(%s)", c.ForeignTable, c.ForeignKey)
	return reference
}

func (c *Column) GetFK() string {
	if c.HasFK() {
		return fmt.Sprintf("\n\tFOREIGN KEY (%s) REFERENCES %s ON DELETE CASCADE on update cascade", c.Name, c.GetFKReference())
	}
	return ""
}

func (c *Column) GetUpdateStmt(add bool) string {
	if add {
		return fmt.Sprintf("ADD %s", c.GetDefinition())
	}
	return fmt.Sprintf("DROP COLUMN %s;", c.Name)
}

func (c *Column) GetOrderStmt(groupBy bool) string {
	name := c.Name
	if c.GroupByName != "" && groupBy {
		name = c.GroupByName
	}
	if c.SelectAs != "" {
		name = c.SelectAs
	}
	if c.OrderAsc {
		return fmt.Sprintf("%s ASC", name)
	}
	return fmt.Sprintf("%s DESC", name)
}
