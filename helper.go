package main

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
		output = joinMaps(output, t)
	}
	return output, nil
}

func joinMaps(m ...map[string]interface{}) map[string]interface{} {
	output := map[string]interface{}{}
	for _, currentMap := range m {
		for k, v := range currentMap {
			if _, found := output[k]; !found {
				output[k] = v
			}
		}
	}
	return output
}
