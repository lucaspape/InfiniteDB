package main

import "github.com/google/uuid"

type Api struct {
	databases map[string]Database
}

func NewApi(databases map[string]Database) *Api {
	api := new(Api)

	api.databases = databases

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

func (api Api) CreateDatabase(name string) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	err := createDatabase(name)

	if err != nil {
		return m, err
	}

	m["message"] = "Created database"
	m["name"] = name

	return m, nil
}

func (api Api) GetDatabase(name string) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	m["name"] = name

	return m, nil
}

func (api Api) GetDatabaseTables(name string) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	var tableNames []string

	database := api.databases[name]

	for tableName := range database.Tables {
		tableNames = append(tableNames, tableName)
	}

	m["name"] = name
	m["tables"] = tableNames

	return m, nil
}

func (api Api) CreateTableInDatabase(name string, tableName string, fields map[string]interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	parsedFields, err := parseFields(fields)

	if err != nil {
		return m, err
	}

	database := api.databases[name]

	err = database.createTable(tableName, parsedFields)

	if err != nil {
		return m, err
	}

	m["name"] = name
	m["tableName"] = tableName
	m["fields"] = fields

	return m, nil
}

func (api Api) GetFromDatabaseTable(name string, tableName string, request map[string]interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	database := api.databases[name]

	objects, err := database.get(tableName, request)

	if err != nil {
		return m, err
	}

	var results []map[string]interface{}

	for _, object := range objects.objects {
		results = append(results, object.M)
	}

	m["name"] = name
	m["tableName"] = tableName
	m["request"] = request
	m["results"] = results

	return m, nil
}

func (api Api) InsertToDatabaseTable(name string, tableName string, object map[string]interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	database := api.databases[name]

	table := database.Tables[tableName]

	objectId := uuid.New().String()

	err := table.insert(*NewObject(objectId, object))

	if err != nil {
		return m, err
	}

	m["name"] = name
	m["tableName"] = tableName
	m["object"] = object
	m["objectId"] = objectId

	return m, nil
}
