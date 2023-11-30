package QueryHelper

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"go.opencensus.io/tag"
	"strconv"
	"strings"
)

var (
	QueryNameTag = tag.MustNewKey("query_name")
)

func CtxWithQueryTag(ctx context.Context, queryName string) context.Context {
	newCtx, err := tag.New(ctx, tag.Insert(QueryNameTag, queryName))
	if err != nil {
		return ctx
	}
	return newCtx
}

func safeString(d interface{}) string {
	switch v := d.(type) {
	case string:
		return v
	case int64:
		return strconv.Itoa(int(v))
	case int:
		return strconv.Itoa(int(v))
	case int32:
		return strconv.Itoa(int(v))
	case float32, float64:
		return fmt.Sprintf("%d", v)
	case bool:
		return strconv.FormatBool(v)
	}
	return ""
}

func fixArrays(query string, args map[string]interface{}) string {
	for k, v := range args {
		namedArg := fmt.Sprintf("(:%s)", k)
		if strings.Contains(query, namedArg) {
			s := safeString(v)
			if s == "" {
				continue
			}
			if strings.HasPrefix(s, "SELECT") {
				query = strings.ReplaceAll(query, namedArg, fmt.Sprintf("(%s)", s))
				continue
			}
			newArgs := []string{}
			switch t := v.(type) {
			case []interface{}:
				for i := 0; i < len(t); i++ {
					newKey := fmt.Sprintf("%s_%d", k, i)
					newArgs = append(newArgs, ":"+newKey)
					args[newKey] = t[i]
				}
			case []string:
				for i := 0; i < len(t); i++ {
					newKey := fmt.Sprintf("%s_%d", k, i)
					newArgs = append(newArgs, ":"+newKey)
					args[newKey] = t[i]
				}
			case []int:
				for i := 0; i < len(t); i++ {
					newKey := fmt.Sprintf("%s_%d", k, i)
					newArgs = append(newArgs, ":"+newKey)
					args[newKey] = t[i]
				}
			case []int64:
				for i := 0; i < len(t); i++ {
					newKey := fmt.Sprintf("%s_%d", k, i)
					newArgs = append(newArgs, ":"+newKey)
					args[newKey] = t[i]
				}
			case []float64:
				for i := 0; i < len(t); i++ {
					newKey := fmt.Sprintf("%s_%d", k, i)
					newArgs = append(newArgs, ":"+newKey)
					args[newKey] = t[i]
				}
			default:
				st := strings.Split(s, ",")

				for i := 0; i < len(st); i++ {
					newKey := fmt.Sprintf("%s_%d", k, i)
					newArgs = append(newArgs, ":"+newKey)
					args[newKey] = st[i]
				}
			}

			query = strings.ReplaceAll(query, namedArg, fmt.Sprintf("(%s)", strings.Join(newArgs, ",")))
		}
	}
	return query
}

func getKeys(i ...interface{}) ([]string, error) {
	m, err := combineStructs(i...)
	if err != nil {
		return nil, err
	}
	var output []string
	for k, v := range m {
		if v == nil || safeString(v) == "" || safeString(v) == "0" {
			continue
		}
		output = append(output, k)
	}
	return output, nil
}

func combineStructsWithPrefix[T any](i ...T) (map[string]interface{}, error) {
	output := map[string]interface{}{}
	for rowIndex, s := range i {
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		t := map[string]interface{}{}
		err = json.Unmarshal(b, &t)
		if err != nil {
			return nil, err
		}

		output = JoinMapsWithPrefix(fmt.Sprintf("%d_", rowIndex), output, t)
	}
	return output, nil
}

func combineStructs(i ...interface{}) (map[string]interface{}, error) {
	output := map[string]interface{}{}
	for _, s := range i {
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		t := map[string]interface{}{}
		err = json.Unmarshal(b, &t)
		if err != nil {
			return nil, err
		}
		output = JoinMaps(output, t)
	}
	return output, nil
}

func JoinMapsWithPrefix[T any](prefix string, m ...map[string]T) map[string]T {
	output := map[string]T{}
	for index, currentMap := range m {
		for k, v := range currentMap {
			if index == 0 {
				output[k] = v
			} else if _, found := output[prefix+k]; !found {
				output[prefix+k] = v
			}
		}
	}
	return output
}

func AddPrefix(prefix string, i map[string]interface{}) map[string]interface{} {
	output := map[string]interface{}{}
	for k, v := range i {
		if _, found := output[prefix+k]; !found {
			output[prefix+k] = v
		}
	}
	return output
}
func JoinMaps[T any](m ...map[string]T) map[string]T {
	output := map[string]T{}
	for _, currentMap := range m {
		for k, v := range currentMap {
			if _, found := output[k]; !found {
				output[k] = v
			}
		}
	}
	return output
}

func WhereValues(whereElements map[string]*Column, useJoin bool) []string {
	var whereValues []string
	dedup := map[string]struct{}{}
	for _, column := range whereElements {
		tmp := column.Where
		if useJoin {
			tmp = column.WhereJoin
		}
		if useJoin && tmp == "" {
			continue
		}
		if tmp == "" {
			tmp = "="
		}
		if _, found := dedup[column.Name]; found {
			continue
		}
		var formatted string
		switch strings.TrimSpace(strings.ToLower(tmp)) {
		case "not in":
			fallthrough
		case "in":
			formatted = fmt.Sprintf("%s %s (:%s)", column.FullName(false), tmp, column.Name)
		default:
			formatted = fmt.Sprintf("%s %s :%s", column.FullName(false), tmp, column.Name)
		}
		if strings.Contains(formatted, ".") {
			whereValues = append(whereValues, formatted)
		}
		dedup[column.Name] = struct{}{}
	}
	return whereValues

}

type NullString struct {
	sql.NullString
}

func (r NullString) MarshalJSON() ([]byte, error) {
	if r.Valid {
		return json.Marshal(r.String)
	} else {
		return nil, nil
	}
}

type NullInt64 struct {
	sql.NullInt64
}

func (r NullInt64) MarshalJSON() ([]byte, error) {
	if r.Valid {
		return json.Marshal(r.Int64)
	} else {
		return nil, nil
	}
}

type NullBool struct {
	sql.NullBool
}

func (r NullBool) MarshalJSON() ([]byte, error) {
	if r.Valid {
		return json.Marshal(r.Bool)
	} else {
		return nil, nil
	}
}

type NullFloat64 struct {
	sql.NullFloat64
}

func (r NullFloat64) MarshalJSON() ([]byte, error) {
	if r.Valid {
		return json.Marshal(r.Float64)
	} else {
		return nil, nil
	}
}

type NullTime struct {
	sql.NullTime
}

func (r NullTime) MarshalJSON() ([]byte, error) {
	if r.Valid {
		return json.Marshal(r.Time)
	} else {
		return nil, nil
	}
}
