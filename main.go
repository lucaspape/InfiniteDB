package main

import (
	"fmt"
	"io/ioutil"
)

const databasePath = "./databases/"

var api Api

func main() {
	databases, err := loadDatabases()

	if err != nil {
		fmt.Println(err)
		return
	}

	api = *NewApi(databases)

	err = runHttpApi()

	if err != nil {
		fmt.Println(err)
		return
	}
}

func loadDatabases() (map[string]Database, error) {
	databases := make(map[string]Database)

	files, err := ioutil.ReadDir(databasePath)

	if err != nil {
		return databases, err
	}

	for _, file := range files {
		database, err := NewDatabase(file.Name(), databasePath)

		if err != nil {
			return databases, err
		}

		databases[file.Name()] = *database

		fmt.Println("Loaded database " + file.Name())
	}

	return databases, nil
}
