package main

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

type Api struct {
	databasePath string
	databases    map[string]Database
}

func NewApi(databasePath string) *Api {
	api := new(Api)

	api.databasePath = databasePath
	api.databases = make(map[string]Database)

	return api
}

func (api Api) GetDatabases() (map[string]interface{}, error) {
	m := make(map[string]interface{})

	var databaseNames []string

	for key := range api.databases {
		databaseNames = append(databaseNames, key)
	}

	m["databases"] = databaseNames

	return m, nil
}

func (api Api) CreateDatabase(name interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	if name == nil {
		return m, errors.New("no name specified")
	}

	err := createDatabase(name.(string))

	if err != nil {
		return m, err
	}

	err = api.loadDatabase(name.(string))

	if err != nil {
		return m, err
	}

	m["message"] = "Created database"
	m["name"] = name.(string)

	return m, nil
}

func (api Api) loadDatabase(name string) error {
	database, err := NewDatabase(name, api.databasePath)

	if err != nil {
		return err
	}

	api.databases[name] = *database

	fmt.Println("Loaded database " + name)

	return nil
}

func (api Api) GetDatabase(name interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	if name == nil {
		return m, errors.New("no name specified")
	}

	m["name"] = name.(string)

	return m, nil
}

func (api Api) GetDatabaseTables(name interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	if name == nil {
		return m, errors.New("no name specified")
	}

	var tableNames []string

	database := api.databases[name.(string)]

	for tableName := range database.Tables {
		tableNames = append(tableNames, tableName)
	}

	m["name"] = name.(string)
	m["tables"] = tableNames

	return m, nil
}

func (api Api) CreateTableInDatabase(name interface{}, tableName interface{}, fields interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	if name == nil {
		return m, errors.New("no name specified")
	}

	if tableName == nil {
		return m, errors.New("no tableName specified")
	}

	if fields == nil {
		return m, errors.New("no fields specified")
	}

	parsedFields, err := parseFields(fields.(map[string]interface{}))

	if err != nil {
		return m, err
	}

	database := api.databases[name.(string)]

	err = database.createTable(tableName.(string), parsedFields)

	if err != nil {
		return m, err
	}

	m["name"] = name.(string)
	m["tableName"] = tableName.(string)
	m["fields"] = fields.(map[string]interface{})

	return m, nil
}

func (api Api) GetFromDatabaseTable(name interface{}, tableName interface{}, request interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	if name == nil {
		return m, errors.New("no name specified")
	}

	if tableName == nil {
		return m, errors.New("no tableName specified")
	}

	if request == nil {
		return m, errors.New("no request specified")
	}

	database := api.databases[name.(string)]

	objects, err := database.get(tableName.(string), request.(map[string]interface{}))

	if err != nil {
		return m, err
	}

	var results []map[string]interface{}

	for _, object := range objects.objects {
		results = append(results, object.M)
	}

	m["name"] = name.(string)
	m["tableName"] = tableName.(string)
	m["request"] = request.(map[string]interface{})
	m["results"] = results

	return m, nil
}

func (api Api) InsertToDatabaseTable(name interface{}, tableName interface{}, object interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	if name == nil {
		return m, errors.New("no name specified")
	}

	if tableName == nil {
		return m, errors.New("no tableName specified")
	}

	if object == nil {
		return m, errors.New("no object specified")
	}

	database := api.databases[name.(string)]

	table := database.Tables[tableName.(string)]

	objectId := uuid.New().String()

	err := table.insert(*NewObject(objectId, object.(map[string]interface{})))

	if err != nil {
		return m, err
	}

	m["name"] = name.(string)
	m["tableName"] = tableName.(string)
	m["object"] = object.(map[string]interface{})
	m["objectId"] = objectId

	return m, nil
}

func (api Api) RemoveFromDatabaseTable(name interface{}, tableName interface{}, request interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	if name == nil {
		return m, errors.New("no name specified")
	}

	if tableName == nil {
		return m, errors.New("no tableName specified")
	}

	if request == nil {
		return m, errors.New("no request specified")
	}

	database := api.databases[name.(string)]

	count, err := database.remove(tableName.(string), request.(map[string]interface{}))

	if err != nil {
		return m, err
	}

	m["name"] = name.(string)
	m["tableName"] = tableName.(string)
	m["request"] = request.(map[string]interface{})
	m["removed"] = count

	return m, nil
}
