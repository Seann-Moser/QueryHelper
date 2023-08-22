package QueryHelper

import (
	"encoding/json"
	"fmt"
	"strings"
)

func safeString(d interface{}) string {
	switch v := d.(type) {
	case string:
		return v
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
			st := strings.Split(s, ",")
			newArgs := []string{}
			for i := 0; i < len(st); i++ {
				newKey := fmt.Sprintf(":%s_%d", k, i)
				newArgs = append(newArgs, newKey)
				args[newKey] = st[i]
			}
			query = strings.ReplaceAll(query, namedArg, fmt.Sprintf("(%s)", strings.Join(newArgs, ",")))
		}
	}
	return query
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
			formatted = fmt.Sprintf("%s %s (:%s)", column.FullName(), tmp, column.Name)
		default:
			formatted = fmt.Sprintf("%s %s :%s", column.FullName(), tmp, column.Name)
		}
		if strings.Contains(formatted, ".") {
			whereValues = append(whereValues, formatted)
		}
		dedup[column.Name] = struct{}{}
	}
	return whereValues

}
