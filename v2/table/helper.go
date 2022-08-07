package table

import (
	"github.com/jmoiron/sqlx"
)

func SelectAll[s any](rows *sqlx.Rows, err error) ([]*s, error) {
	if err != nil {
		return nil, err
	}
	var output []*s
	for rows.Next() {
		tmp := &s{}
		err := rows.StructScan(tmp)
		if err != nil {
			return nil, err
		}
		output = append(output, tmp)
	}
	return output, nil
}
