package dataset_table

type Tables interface {
	Info
	Statements
}

var _ Tables = &Table{}

type Table struct {
	Dataset  string    `json:"dataset"`
	Name     string    `json:"name"`
	Elements []*Config `json:"elements"`
}
type Config struct {
	Name         string
	Type         string `json:"data_type"`
	Default      string `json:"default"`
	Primary      bool   `json:"primary"`
	ForeignKey   string `json:"foreign_key"`
	ForeignTable string `json:"foreign_table"`
	Skip         bool   `json:"skip"`
	Update       bool   `json:"update"`
	Null         bool   `json:"null"`
	Select       bool   `json:"select"`
	Where        string `json:"where"`
	WhereJoin    string `json:"where_join"`
	JoinName     string `json:"join_name"`
	Join         bool   `json:"join"`
	Delete       bool   `json:"delete"`
	Order        string `json:"order"`
	OrderAsc     bool   `json:"order_asc"`
}
