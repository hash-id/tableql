package rql

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type Params struct {
	Select    []string
	Limit     int
	Offset    int
	OrderBy   []string
	WhereExp  string
	WhereArgs map[string]interface{}
}

var operators = map[string]string{
	"_eq":           "=",
	"_neq":          "<>",
	"_gt":           ">",
	"_lt":           "<",
	"_gte":          ">=",
	"_lte":          "<=",
	"_like":         "LIKE",
	"_nlike":        "NOT LIKE",
	"_ilike":        "ILIKE",
	"_nilike":       "NOT ILIKE",
	"_similar":      "SIMILAR TO",
	"_nsimilar":     "NOT SIMILAR TO",
	"_contains":     "@>",
	"_contained_in": "<@",
	"_has_key":      "?",
	"_has_keys_any": "?|",
	"_has_keys_all": "?&",
}

func traverse(parent string, where *map[string]interface{}, arguments *map[string]interface{}) (expression string, err error) {
	columns := []string{}
	for k, v := range *where {
		switch k {
		case "_eq":
			fallthrough
		case "_neq":
			var id string
			id, err = gonanoid.Generate("1234567890abcdef", 8)
			key := fmt.Sprint(parent, "_", id)
			columns = append(columns, fmt.Sprint(`"`, parent, `"`, " ", operators[k], " @", key))
			(*arguments)[key] = v
		default:
			x, ok := v.(map[string]interface{})
			if !ok {
				err = errors.New("not valid object")
				return
			}
			expression, err = traverse(k, &x, arguments)
			return
		}
	}
	expression = strings.Join(columns, " AND ")
	return
}

func Parse(input *[]byte) (params *Params, err error) {
	params = &Params{}
	var query map[string]interface{}
	err = json.Unmarshal(*input, &query)
	if err != nil {
		return
	}
	if query["select"] != nil {
		selectData, ok := query["select"].([]interface{})
		if !ok {
			err = errors.New("select is not valid array")
			return
		}
		params.Select = []string{}
		for k, v := range selectData {
			x, ok := v.(string)
			if !ok {
				err = errors.New(fmt.Sprint("select[", k, "] is not valid string"))
				return
			}
			params.Select = append(params.Select, x)
		}
	}
	if query["limit"] != nil {
		limit, ok := query["limit"].(float64)
		if !ok {
			err = errors.New("limit is not valid number")
			return
		}
		params.Limit = int(limit)
	}
	if query["offset"] != nil {
		offset, ok := query["offset"].(float64)
		if !ok {
			err = errors.New("offset is not valid number")
			return
		}
		params.Offset = int(offset)
	}
	if query["order_by"] != nil {
		order_by, ok := query["order_by"].([]interface{})
		if !ok {
			err = errors.New("order_by is not valid array")
			return
		}
		params.OrderBy = []string{}
		for k, v := range order_by {
			x, ok := v.(map[string]interface{})
			if !ok {
				err = errors.New(fmt.Sprint("order_by[", k, "] is not valid object"))
				return
			}
			if x["column"] == nil {
				err = errors.New("column not found")
				return
			}
			column, ok := x["column"].(string)
			if !ok {
				err = errors.New("column is not valid string")
				return
			}
			if x["order"] == nil {
				err = errors.New("order not found")
				return
			}
			order, ok := x["order"].(string)
			if !ok {
				err = errors.New("order is not valid string")
				return
			}
			switch strings.ToUpper(order) {
			case "ASC":
				fallthrough
			case "DESC":
				fallthrough
			case "ASC NULLS FIRST":
				fallthrough
			case "DESC NULLS FIRST":
				fallthrough
			case "ASC NULLS LAST":
				fallthrough
			case "DESC NULLS LAST":
				params.OrderBy = append(params.OrderBy, fmt.Sprint(`"`, column, `"`, ` `, strings.ToUpper(order)))
			default:
				err = errors.New("order must be ASC / DESC / ASC NULLS FIRST / DESC NULLS FIRST / ASC NULLS FIRST / ASC NULLS LAST")
				return
			}
		}
	}
	if query["where"] != nil {
		where, ok := query["where"].(map[string]interface{})
		if !ok {
			err = errors.New("where is not valid object")
			return
		}
		var expression string
		arguments := map[string]interface{}{}
		expression, err = traverse("", &where, &arguments)
		if err != nil {
			return
		}
		params.WhereExp = expression
		params.WhereArgs = arguments
	}
	return
}
