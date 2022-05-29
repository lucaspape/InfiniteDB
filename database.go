package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type Database struct {
	Name       string
	path       string
	tablesPath string
	Tables     map[string]Table
}

type Request struct {
	implement *[]Implement
	where     *Where
	sort      *Sort
	skip      *int64
	limit     *int64
}

type Where struct {
	t *string

	field    string
	operator string
	value    string

	where *Where
}

type Implement struct {
	from  ImplementFrom
	field string
	as    *string
}

type ImplementFrom struct {
	table string
	field string
}

type Sort struct {
	field       string
	direction   string
	levenshtein *string
}

const (
	equals  string = "="
	not     string = "!="
	larger  string = ">"
	smaller string = "<"
	between string = "><"
)

const (
	and string = "and"
	or  string = "or"
)

const (
	asc  string = "asc"
	desc string = "desc"
)

func NewDatabase(name string, path string) (*Database, error) {
	database := new(Database)

	database.Name = name
	database.path = path
	database.tablesPath = path + name + "/tables/"
	database.Tables = make(map[string]Table)

	err := database.initializeDatabase()

	return database, err
}

func createDatabase(name string) error {
	//TODO check if already exists

	err := os.MkdirAll("databases/"+name+"/tables", os.ModePerm)

	if err != nil {
		return err
	}

	return nil
}

func (database Database) initializeDatabase() error {
	files, err := ioutil.ReadDir(database.tablesPath)

	if err != nil {
		return err
	}

	for _, tableFolder := range files {
		err := database.loadTable(tableFolder.Name())

		if err != nil {
			return err
		}
	}

	return nil
}

func (database Database) loadTable(name string) error {
	bytes, err := ioutil.ReadFile(database.tablesPath + name + "/table.json")

	if err != nil {
		return err
	}

	var m map[string]interface{}
	err = json.Unmarshal(bytes, &m)

	if err != nil {
		return err
	}

	fields, err := parseTableConfig(m)

	if err != nil {
		return err
	}

	table, err := NewTable(name, database.tablesPath, fields)

	if err != nil {
		return err
	}

	database.Tables[name] = *table

	fmt.Println("loaded table " + name)

	return nil
}

func parseTableConfig(m map[string]interface{}) (map[string]Field, error) {
	fieldMap := make(map[string]Field)

	fields := m["fields"].(map[string]interface{})

	for name, field := range fields {
		field := field.(map[string]interface{})

		var kind reflect.Kind

		switch tFloat := field["Type"].(float64); tFloat {
		case 1:
			kind = reflect.Bool
			break
		case 14:
			kind = reflect.Float64
			break
		case 24:
			kind = reflect.String
			break
		default:
			return fieldMap, errors.New("type not found")
		}

		fieldMap[name] = *NewField(fmt.Sprintf("%v", field["Name"]), field["Indexed"].(bool), kind)
	}

	return fieldMap, nil
}

func (database Database) createTable(name string, fields map[string]Field) error {
	//TODO check if already exists

	err := os.MkdirAll(database.tablesPath+name+"/objects", os.ModePerm)

	if err != nil {
		return err
	}

	x := make(map[string]interface{})
	x["fields"] = fields

	bytes, err := json.Marshal(x)

	if err != nil {
		return err
	}

	err = ioutil.WriteFile(database.tablesPath+name+"/table.json", bytes, 0644)

	if err != nil {
		return err
	}

	return database.loadTable(name)
}

func (database Database) get(tableName string, request map[string]interface{}) (*Objects, error) {
	r, err := parseRequest(request)

	if err != nil {
		return nil, err
	}

	if r.where != nil {
		table := database.Tables[tableName]
		objects, err := runWhere(table, *r.where, nil)

		if r.sort != nil {
			objects, err = objects.sort(table, r.sort.field, r.sort.direction, r.sort.levenshtein)
		}

		objects = objects.skipAndLimit(r.skip, r.limit)

		return &objects, err
	} else {
		//TODO return entire table
		return new(Objects), nil
	}
}

func runWhere(table Table, where Where, previousObjects *Objects) (Objects, error) {
	var objects Objects
	var err error

	switch where.operator {
	case equals:
		objects, err = table.equal(where.field, where.value, previousObjects)
		break
	case not:
		objects, err = table.not(where.field, where.value, previousObjects)
		break
	case smaller:
		objects, err = table.smaller(where.field, where.value, previousObjects)
		break
	case larger:
		objects, err = table.larger(where.field, where.value, previousObjects)
		break
	case between:
		values := strings.Split(where.value, "-")

		objects, err = table.between(where.field, values[0], values[1], previousObjects)
		break
	}

	if err != nil {
		return objects, err
	}

	if where.where != nil {
		switch *where.where.t {
		case and:
			return runWhere(table, *where.where, &objects)
		case or:
			next, err := runWhere(table, *where.where, previousObjects)

			if err != nil {
				return objects, err
			}

			//TODO remove duplicates
			objects.objects = append(objects.objects, next.objects...)
			break
		}
	}

	return objects, nil
}

func parseRequest(m map[string]interface{}) (*Request, error) {
	//TODO this needs type checking
	//TODO cannot have and AND or in one query

	request := new(Request)

	for key, value := range m {
		switch key {
		case "where":
			where, err := parseWhere(nil, value.(map[string]interface{}))

			if err != nil {
				return nil, err
			}

			request.where = where
			break
		case "implement":
			request.implement = parseImplements(value.([]interface{}))
			break
		case "sort":
			sort, err := parseSort(value.(map[string]interface{}))

			if err != nil {
				return nil, err
			}

			request.sort = sort
			break
		case "skip":
			skip := int64(value.(float64))
			request.skip = &skip
			break
		case "limit":
			limit := int64(value.(float64))
			request.limit = &limit
			break
		}
	}

	return request, nil
}

func parseWhere(t *string, m map[string]interface{}) (*Where, error) {
	where := new(Where)

	if t != nil {
		where.t = t
	}

	var nextType *string
	var nextWhere interface{}

	for key, value := range m {
		switch key {
		case "field":
			where.field = value.(string)
			break
		case "operator":
			var operator string

			//TODO yeah this is kinda stupid
			switch value.(string) {
			case equals:
				operator = equals
				break
			case not:
				operator = not
				break
			case larger:
				operator = larger
				break
			case smaller:
				operator = smaller
				break
			case between:
				operator = between
				break
			default:
				return nil, errors.New("operator not supported")
			}

			where.operator = operator
			break
		case "value":
			switch value.(type) {
			case string:
				where.value = value.(string)
				break
			case float64:
				where.value = fmt.Sprintf("%f", value.(float64))
				break
			case bool:
				where.value = strconv.FormatBool(value.(bool))
				break
			default:
				return nil, errors.New("type not supported")
			}
			break
		case and:
			andType := and
			nextType = &andType
			nextWhere = value
			break
		case or:
			orType := or
			nextType = &orType
			nextWhere = value
			break
		}
	}

	if nextType != nil && nextWhere != nil {
		w, err := parseWhere(nextType, (nextWhere).(map[string]interface{}))

		if err != nil {
			return nil, err
		}

		where.where = w
	}

	return where, nil
}

func parseImplements(m []interface{}) *[]Implement {
	var implements []Implement

	for _, i := range m {
		implements = append(implements, *parseImplement(i.(map[string]interface{})))
	}

	return &implements
}

func parseImplement(m map[string]interface{}) *Implement {
	implement := new(Implement)

	for key, value := range m {
		switch key {
		case "from":
			from := parseFrom(value.(map[string]interface{}))
			implement.from = *from
			break
		case "field":
			implement.field = value.(string)
			break
		case "as":
			as := value.(string)
			implement.as = &as
			break
		}
	}

	return implement
}

func parseFrom(m map[string]interface{}) *ImplementFrom {
	from := new(ImplementFrom)

	for key, value := range m {
		switch key {
		case "table":
			from.table = value.(string)
			break
		case "field":
			from.field = value.(string)
			break
		}
	}

	return from
}

func parseSort(m map[string]interface{}) (*Sort, error) {
	sort := new(Sort)

	for key, value := range m {
		switch key {
		case "field":
			sort.field = value.(string)
			break
		case "direction":
			//TODO also kinda stupid
			switch value.(string) {
			case asc:
				sort.direction = asc
				break
			case desc:
				sort.direction = desc
				break
			default:
				return nil, errors.New("unknown sort direction")
			}

			break
		case "levenshtein":
			l := value.(string)
			sort.levenshtein = &l
			break
		}
	}

	return sort, nil
}
