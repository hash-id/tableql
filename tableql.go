package tableql

import (
	"encoding/json"
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
	fields := []string{}
	for key, value := range *where {
		switch key {
		case "_and":
			fallthrough
		case "_or":
			var op string
			switch key {
			case "_and":
				op = " AND "
			case "_or":
				op = " OR "
			}
			operands, ok := value.([]interface{})
			if !ok {
				err = fmt.Errorf("%s is not a valid array", key)
				return
			}
			combinations := []string{}
			for i, v := range operands {
				operand, ok := v.(map[string]interface{})
				if !ok {
					err = fmt.Errorf("%s[%d] is not a valid object", key, i)
					return
				}
				var text string
				text, err = traverse("", &operand, arguments)
				if err != nil {
					return
				}
				combinations = append(combinations, text)
			}
			fields = append(fields, fmt.Sprintf("( %s )", strings.Join(combinations, op)))
		case "_not":
			operand, ok := value.(map[string]interface{})
			if !ok {
				err = fmt.Errorf("_not value is not a valid object")
				return
			}
			var text string
			text, err = traverse("", &operand, arguments)
			if err != nil {
				return
			}
			fields = append(fields, fmt.Sprintf("( NOT %s )", text))
		case "_eq":
			fallthrough
		case "_neq":
			fallthrough
		case "_gt":
			fallthrough
		case "_lt":
			fallthrough
		case "_gte":
			fallthrough
		case "_lte":
			fallthrough
		case "_like":
			fallthrough
		case "_nlike":
			fallthrough
		case "_ilike":
			fallthrough
		case "_nilike":
			fallthrough
		case "_similar":
			fallthrough
		case "_nsimilar":
			fallthrough
		case "_contains":
			fallthrough
		case "_contained_in":
			fallthrough
		case "_has_key":
			fallthrough
		case "_has_keys_any":
			fallthrough
		case "_has_keys_all":
			id := fmt.Sprintf("%s_%s", parent, gonanoid.MustGenerate("1234567890abcdef", 8))
			fields = append(fields, fmt.Sprintf(`"%s" %s @%s`, parent, operators[key], id))
			(*arguments)[id] = value
		case "_in":
			fallthrough
		case "_nin":
			not := ""
			if key == "_nin" {
				not = " NOT"
			}
			operand, ok := value.([]interface{})
			if !ok {
				err = fmt.Errorf("%s value is not a valid rrray", key)
				return
			}
			text := []string{}
			for _, v := range operand {
				id := fmt.Sprintf("%s_%s", parent, gonanoid.MustGenerate("1234567890abcdef", 8))
				text = append(text, fmt.Sprintf("@%s", id))
				(*arguments)[id] = v
			}
			fields = append(fields, fmt.Sprintf(`"%s" %s IN ( %s )`, parent, not, strings.Join(text, ", ")))
		case "_is_null":
			isNull, ok := value.(bool)
			if !ok {
				err = fmt.Errorf("_is_null value is not a valid boolean")
				return
			}
			not := ""
			if !isNull {
				not = " NOT"
			}
			fields = append(fields, fmt.Sprintf(`"%s" IS%s NULL`, parent, not))
		default:
			operand, ok := value.(map[string]interface{})
			if !ok {
				err = fmt.Errorf("not valid object")
				return
			}
			var text string
			text, err = traverse(key, &operand, arguments)
			if err != nil {
				return
			}
			fields = append(fields, text)
		}
	}
	expression = strings.Join(fields, " AND ")
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
			err = fmt.Errorf("select is not valid array")
			return
		}
		params.Select = []string{}
		for i, v := range selectData {
			column, ok := v.(string)
			if !ok {
				err = fmt.Errorf(fmt.Sprintf("select[%d] is not valid string", i))
				return
			}
			params.Select = append(params.Select, column)
		}
	}
	if query["limit"] != nil {
		limit, ok := query["limit"].(float64)
		if !ok {
			err = fmt.Errorf("limit is not valid number")
			return
		}
		params.Limit = int(limit)
	}
	if query["offset"] != nil {
		offset, ok := query["offset"].(float64)
		if !ok {
			err = fmt.Errorf("offset is not valid number")
			return
		}
		params.Offset = int(offset)
	}
	if query["order_by"] != nil {
		order_by, ok := query["order_by"].([]interface{})
		if !ok {
			err = fmt.Errorf("order_by is not valid array")
			return
		}
		params.OrderBy = []string{}
		for i, v := range order_by {
			orderBy, ok := v.(map[string]interface{})
			if !ok {
				err = fmt.Errorf(fmt.Sprint("order_by[", i, "] is not valid object"))
				return
			}
			if orderBy["column"] == nil {
				err = fmt.Errorf("column not found")
				return
			}
			column, ok := orderBy["column"].(string)
			if !ok {
				err = fmt.Errorf("column is not valid string")
				return
			}
			if orderBy["order"] == nil {
				err = fmt.Errorf("order not found")
				return
			}
			order, ok := orderBy["order"].(string)
			if !ok {
				err = fmt.Errorf("order is not valid string")
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
				params.OrderBy = append(params.OrderBy, fmt.Sprintf(`"%s" %s`, column, strings.ToUpper(order)))
			default:
				err = fmt.Errorf("order must be ASC / DESC / ASC NULLS FIRST / DESC NULLS FIRST / ASC NULLS FIRST / ASC NULLS LAST")
				return
			}
		}
	}
	if query["where"] != nil {
		where, ok := query["where"].(map[string]interface{})
		if !ok {
			err = fmt.Errorf("where is not valid object")
			return
		}
		var expression string
		arguments := map[string]interface{}{}
		expression, err = traverse("", &where, &arguments)
		if err != nil {
			return
		}
		params.WhereExp = expression
		if len(arguments) > 0 {
			params.WhereArgs = arguments
		}
	}
	return
}
