package dataset_table

type Table interface {
	Info
	Statements
}

var _ Table = &DefaultTable{}

type DefaultTable struct {
	Dataset  string     `json:"dataset"`
	Name     string     `json:"name"`
	Elements []*Element `json:"elements"`
}

type Element struct {
	Name           string
	Primary        bool `json:"primary"`
	Skip           bool `json:"skip"`
	Update         bool `json:"update"`
	Null           bool `json:"null"`
	Select         bool `json:"select"`
	Delete         bool `json:"delete"`
	AutoGenerateID bool `json:"auto_generate_id"`
	Join           bool `json:"join"`
	OrderAsc       bool `json:"order_asc"`

	Type    string `json:"data_type"`
	Default string `json:"default"`

	ForeignKey   string `json:"foreign_key"`
	ForeignTable string `json:"foreign_table"`

	WhereJoin string `json:"where_join"`
	Where     string `json:"where"`
	JoinName  string `json:"join_name"`

	Order string `json:"order"`

	AutoGenerateIDType string `json:"auto_generate_id_type"`
}
