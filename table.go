package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Table struct {
	Name        string
	path        string
	objectsPath string
	Fields      map[string]Field
	index       *Index
}

type Object struct {
	Id string
	M  map[string]interface{}
}

type Objects struct {
	objects []Object
}

func (objects Objects) equal(fieldName string, key string) Objects {
	var results []Object

	for _, object := range objects.objects {
		if object.M[fieldName].(string) == key {
			results = append(results, object)
		}
	}

	objects.objects = results

	return objects
}

func (objects Objects) not(fieldName string, key string) Objects {
	var results []Object

	for _, object := range objects.objects {
		if object.M[fieldName].(string) != key {
			results = append(results, object)
		}
	}

	objects.objects = results

	return objects
}

func (objects Objects) match(fieldName string, r regexp.Regexp) Objects {
	var results []Object

	for _, object := range objects.objects {
		if r.MatchString(object.M[fieldName].(string)) {
			results = append(results, object)
		}
	}

	objects.objects = results

	return objects
}

func (objects Objects) smaller(fieldName string, key string, parseNumber bool) Objects {
	var results []Object

	for _, object := range objects.objects {
		value := object.M[fieldName].(string)

		if parseNumber {
			keyInt, _ := strconv.ParseFloat(key, 64)
			valueInt, _ := strconv.ParseFloat(value, 64)

			if valueInt < keyInt {
				results = append(results, object)
			}
		} else {
			if value < key {
				results = append(results, object)
			}
		}
	}

	objects.objects = results

	return objects
}

func (objects Objects) larger(fieldName string, key string, parseNumber bool) Objects {
	var results []Object

	for _, object := range objects.objects {
		value := object.M[fieldName].(string)

		if parseNumber {
			keyInt, _ := strconv.ParseFloat(key, 64)
			valueInt, _ := strconv.ParseFloat(value, 64)

			if valueInt > keyInt {
				results = append(results, object)
			}
		} else {
			if value > key {
				results = append(results, object)
			}
		}
	}

	objects.objects = results

	return objects
}

func (objects Objects) between(fieldName string, smaller string, larger string, parseNumber bool) Objects {
	var results []Object

	for _, object := range objects.objects {
		value := object.M[fieldName].(string)

		if parseNumber {
			smallerInt, _ := strconv.ParseFloat(smaller, 64)
			largerInt, _ := strconv.ParseFloat(larger, 64)
			valueInt, _ := strconv.ParseFloat(value, 64)

			if valueInt > smallerInt && valueInt < largerInt {
				results = append(results, object)
			}
		} else {
			if value > smaller && value < larger {
				results = append(results, object)
			}
		}
	}

	objects.objects = results

	return objects
}

func (objects Objects) sort(table Table, fieldName string, direction string, levenshtein *string) (Objects, error) {
	t := table.Fields[fieldName].Type

	if levenshtein != nil && t == reflect.String {
		return objects.sortLevenshtein(fieldName, direction, *levenshtein), nil
	} else if levenshtein != nil {
		return objects, errors.New("can only sort string using levenshtein")
	} else {
		switch t {
		case reflect.String:
			return objects.sortString(fieldName, direction), nil
		case reflect.Float64:
			return objects.sortFloat(fieldName, direction), nil
		case reflect.Bool:
			return objects.sortBoolean(fieldName, direction), nil
		default:
			return objects, errors.New("cannot sort this type")
		}
	}
}

func (objects Objects) sortString(fieldName string, direction string) Objects {
	switch direction {
	case asc:
		sort.Slice(objects.objects, func(i, j int) bool {
			return objects.objects[i].M[fieldName].(string) < objects.objects[j].M[fieldName].(string)
		})
		break
	case desc:
		sort.Slice(objects.objects, func(i, j int) bool {
			return objects.objects[i].M[fieldName].(string) > objects.objects[j].M[fieldName].(string)
		})
		break
	}

	return objects
}

func (objects Objects) sortLevenshtein(fieldName string, direction string, l string) Objects {
	internalFieldName := "INTERNAL_DATABASE_DISTANCE"

	str1 := []rune(l)

	for _, object := range objects.objects {
		str2 := []rune(object.M[fieldName].(string))

		object.M[internalFieldName] = levenshtein(str1, str2)
	}

	return objects.sortFloat(internalFieldName, direction)
}

func (objects Objects) sortFloat(fieldName string, direction string) Objects {
	switch direction {
	case asc:
		sort.Slice(objects.objects, func(i, j int) bool {
			return objects.objects[i].M[fieldName].(float64) < objects.objects[j].M[fieldName].(float64)
		})
		break
	case desc:
		sort.Slice(objects.objects, func(i, j int) bool {
			return objects.objects[i].M[fieldName].(float64) > objects.objects[j].M[fieldName].(float64)
		})
		break
	}

	return objects
}

func (objects Objects) sortBoolean(fieldName string, direction string) Objects {
	switch direction {
	case asc:
		sort.Slice(objects.objects, func(i, j int) bool {
			return objects.objects[i].M[fieldName].(bool)
		})
		break
	case desc:
		sort.Slice(objects.objects, func(i, j int) bool {
			return !objects.objects[i].M[fieldName].(bool)
		})
		break
	}

	return objects
}

func (objects Objects) skipAndLimit(skip *int64, limit *int64) Objects {
	results := make([]Object, 0)

	if skip != nil && limit != nil {
		for i, object := range objects.objects {
			if int64(i) >= *skip && int64(i) < *limit {
				results = append(results, object)
			}
		}

	} else if skip == nil && limit != nil {
		for i, object := range objects.objects {
			if int64(i) < *limit {
				results = append(results, object)
			}
		}

	} else if skip != nil && limit == nil {
		for i, object := range objects.objects {
			if int64(i) >= *skip {
				results = append(results, object)
			}
		}
	}

	objects.objects = results

	return objects
}

func NewTable(name string, path string, fields map[string]Field) (*Table, error) {
	table := new(Table)

	table.Name = name
	table.path = path
	table.objectsPath = path + name + "/objects/"
	table.Fields = fields
	table.index = NewIndex()

	err := table.initializeIndex()

	return table, err
}

func NewObject(id string, m map[string]interface{}) *Object {
	object := new(Object)

	object.Id = id
	object.M = m

	return object
}

func NewObjects(objects []Object) *Objects {
	o := new(Objects)

	o.objects = objects

	return o
}

func (table Table) initializeIndex() error {
	files, err := ioutil.ReadDir(table.objectsPath)

	if err != nil {
		return err
	}

	for _, objectFile := range files {
		bytes, err := ioutil.ReadFile(table.objectsPath + objectFile.Name())

		if err != nil {
			return err
		}

		var x map[string]interface{}
		err = json.Unmarshal(bytes, &x)

		if err != nil {
			return err
		}

		object := *NewObject(objectFile.Name(), x)

		table.indexObject(object)
	}

	return nil
}

func (table Table) runWhere(where Where, previousObjects *Objects) (Objects, error) {
	var objects Objects
	var err error

	switch where.operator {
	case equals:
		objects, err = table.equal(where.field, where.value, previousObjects)
		break
	case not:
		objects, err = table.not(where.field, where.value, previousObjects)
		break
	case match:
		r, err := regexp.Compile(where.value)

		if err != nil {
			return objects, err
		}

		objects, err = table.match(where.field, *r, previousObjects)
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
			return table.runWhere(*where.where, &objects)
		case or:
			next, err := table.runWhere(*where.where, previousObjects)

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

func (table Table) insert(object Object, overwrite bool) error {
	err := object.checkAllFieldsExist(table)

	if err != nil {
		return err
	}

	if !overwrite {
		err = object.checkUnique(table)
	}

	if err != nil {
		return err
	}

	bytes, err := json.Marshal(object.M)

	if err != nil {
		return err
	}

	err = ioutil.WriteFile(table.objectsPath+object.Id, bytes, 0644)

	if err != nil {
		return err
	}

	table.indexObject(object)

	return nil
}

func (table Table) remove(object Object) error {
	table.unIndexObject(object)

	err := os.Remove(table.objectsPath + object.Id)

	if err != nil {
		return err
	}

	return nil
}

func (table Table) findExisting(object map[string]interface{}) (*Object, error) {
	for fieldName, field := range table.Fields {
		if field.Indexed && field.Unique {
			indexElements := table.index.equal(fieldName, fmt.Sprintf("%v", object[fieldName]))

			if len(indexElements) > 0 {
				objects, err := table.indexElementsToObjects(indexElements)

				if err != nil {
					return nil, err
				}

				return &objects.objects[0], nil
			}
		}
	}

	return nil, errors.New("could not find object with at least one indexed and unique field")
}

func (table Table) indexObject(object Object) {
	for fieldName, field := range table.Fields {
		if field.Indexed {
			table.index.add(fieldName, fmt.Sprintf("%v", object.M[fieldName]), *NewIndexElement(object.Id))
		}
	}
}

func (table Table) unIndexObject(object Object) {
	for fieldName, field := range table.Fields {
		if field.Indexed {
			table.index.remove(fieldName, fmt.Sprintf("%v", object.M[fieldName]), *NewIndexElement(object.Id))
		}
	}
}

func (object Object) checkUnique(table Table) error {
	for fieldName, field := range table.Fields {
		if field.Unique {
			if len(table.index.equal(fieldName, fmt.Sprint(object.M[fieldName]))) > 0 {
				return errors.New("found existing object for field " + fieldName)
			}
		}
	}

	return nil
}

func (object Object) checkAllFieldsExist(table Table) error {
	for fieldName := range table.Fields {
		if object.M[fieldName] == nil {
			return errors.New("object does not have value for field " + fieldName)
		}
	}

	return nil
}

func (table Table) equal(fieldName string, key string, previousObjects *Objects) (Objects, error) {
	if previousObjects == nil {
		return table.indexElementsToObjects(table.index.equal(fieldName, key))
	} else {
		return previousObjects.equal(fieldName, key), nil
	}
}

func (table Table) not(fieldName string, key string, previousObjects *Objects) (Objects, error) {
	if previousObjects == nil {
		return table.indexElementsToObjects(table.index.not(fieldName, key))
	} else {
		return previousObjects.not(fieldName, key), nil
	}
}

func (table Table) match(fieldName string, r regexp.Regexp, previousObjects *Objects) (Objects, error) {
	if previousObjects == nil {
		return table.indexElementsToObjects(table.index.match(fieldName, r))
	} else {
		return previousObjects.match(fieldName, r), nil
	}
}

func (table Table) larger(fieldName string, key string, previousObjects *Objects) (Objects, error) {
	parseNumber := table.Fields[fieldName].Type == reflect.Float64

	if previousObjects == nil {
		return table.indexElementsToObjects(table.index.larger(fieldName, key, parseNumber))
	} else {
		return previousObjects.larger(fieldName, key, parseNumber), nil
	}
}

func (table Table) smaller(fieldName string, key string, previousObjects *Objects) (Objects, error) {
	parseNumber := table.Fields[fieldName].Type == reflect.Float64

	if previousObjects == nil {
		return table.indexElementsToObjects(table.index.smaller(fieldName, key, parseNumber))
	} else {
		return previousObjects.smaller(fieldName, key, parseNumber), nil
	}
}

func (table Table) between(fieldName string, smaller string, larger string, previousObjects *Objects) (Objects, error) {
	parseNumber := table.Fields[fieldName].Type == reflect.Float64

	if previousObjects == nil {
		return table.indexElementsToObjects(table.index.between(fieldName, smaller, larger, parseNumber))
	} else {
		return previousObjects.between(fieldName, smaller, larger, parseNumber), nil
	}
}

func (table Table) indexElementsToObjects(indexElements []IndexElement) (Objects, error) {
	var objects []Object

	for _, indexElement := range indexElements {
		file, err := ioutil.ReadFile(table.objectsPath + indexElement.value)

		if err != nil {
			return *NewObjects(objects), err
		}

		var x map[string]interface{}
		err = json.Unmarshal(file, &x)

		if err != nil {
			return *NewObjects(objects), err
		}

		objects = append(objects, *NewObject(indexElement.value, x))
	}

	return *NewObjects(objects), nil
}
