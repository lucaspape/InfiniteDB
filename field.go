package main

import (
	"errors"
	"reflect"
)

type Field struct {
	Name    string
	Indexed bool
	Type    reflect.Kind
}

func NewField(name string, indexed bool, t reflect.Kind) *Field {
	field := new(Field)

	field.Name = name
	field.Indexed = indexed
	field.Type = t

	return field
}

func parseFields(m map[string]interface{}) (map[string]Field, error) {
	resultMap := make(map[string]Field)

	for fieldName, fieldValues := range m {
		var t *reflect.Kind
		var indexed bool

		fieldMap := fieldValues.(map[string]interface{})

		for key, value := range fieldMap {
			switch key {
			case "type":
				switch value {
				case "text":
					ts := reflect.String
					t = &ts
					break
				case "number":
					tn := reflect.Float64
					t = &tn
					break
				case "boolean":
					tb := reflect.Bool
					t = &tb
					break
				}

				break
			case "indexed":
				indexed = value.(bool)
				break
			}
		}

		if t == nil {
			return resultMap, errors.New("field does not have a type")
		}

		resultMap[fieldName] = *NewField(fieldName, indexed, *t)
	}

	return resultMap, nil
}
