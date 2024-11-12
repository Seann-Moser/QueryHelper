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
	Prefix      string `json:"-"`

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
	ignoreGroupBy      bool
	SelectAs           string `json:"as"`
	Ignore             bool   `json:"ignore"`

	Encrypt bool `json:"encrypt"`
	Decrypt bool `json:"decrypt"`
}

// GetDefinition Adjust the Column methods accordingly
func (col *Column) GetDefinition() string {
	// Properly quote the column name
	name := fmt.Sprintf("`%s`", col.Name)
	// Build the type and default value
	definition := fmt.Sprintf("%s %s", name, col.Type)
	if !col.Null {
		definition += " NOT NULL"
	}
	switch col.Default {
	case "created_timestamp":
		definition += " DEFAULT NOW()"
	case "updated_timestamp":
		definition += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
	default:
		if col.Default != "" {
			// Ensure the default value is compatible with MySQL 8
			definition += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
	}

	if col.AutoGenerateID {
		definition += " AUTO_INCREMENT"
	}
	return definition
}

func (col *Column) HasFK() bool {
	if len(col.ForeignKey) > 0 && len(col.ForeignTable) > 0 {
		return col.ForeignKey != ""
	}
	return false
}

func (col *Column) GetFK() (string, error) {
	// Properly quote identifiers
	constraintName := fmt.Sprintf("`FK_%s_%s`", col.Table, col.Name)
	columnName := fmt.Sprintf("`%s`", col.Name)
	foreignTable := fmt.Sprintf("`%s`.`%s`", col.ForeignSchema, col.ForeignTable)
	foreignColumn := fmt.Sprintf("`%s`", col.ForeignKey)
	return fmt.Sprintf("\n\tCONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", constraintName, columnName, foreignTable, foreignColumn), nil
}

func (c Column) Wrap(wrap string) Column {
	c.Wrapper = wrap
	return c
}

func (c Column) IgnoreGroupBy() Column {
	c.ignoreGroupBy = true
	return c
}

func (c Column) SetDataset(d string) Column {
	c.Dataset = d
	return c
}

func (c Column) As(as string) Column {
	c.SelectAs = as
	return c
}

func (c *Column) FullName(groupBy bool, inSelect bool) string {
	name := fmt.Sprintf("%s.%s", c.Table, c.Name)
	if c.Wrapper != "" && inSelect {
		name = fmt.Sprintf(c.Wrapper, name)
	}

	if groupBy && len(c.GroupByModifier) > 0 && !c.ignoreGroupBy {
		if strings.Contains(c.GroupByModifier, "*") {
			name = strings.ReplaceAll(c.GroupByModifier, "*", name)
		} else {
			name = fmt.Sprintf("%s(%s)", c.GroupByModifier, name)
		}
		if c.SelectAs == "" && inSelect {
			if c.GroupByName != "" {
				name = fmt.Sprintf("%s AS %s", name, c.GroupByName)
			} else {
				name = fmt.Sprintf("%s AS %s", name, c.Name)
			}
		}
	}
	if c.SelectAs != "" && inSelect {
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
