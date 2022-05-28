package main

import (
	"fmt"
	"io/ioutil"
)

const databasePath = "./databases/"

var databases map[string]Database

func main() {
	err := loadDatabases()

	if err != nil {
		fmt.Println(err)
		return
	}

	runHttpApi()
}

func loadDatabases() error {
	databases = make(map[string]Database)

	files, err := ioutil.ReadDir(databasePath)

	if err != nil {
		return err
	}

	for _, file := range files {
		database, err := NewDatabase(file.Name(), databasePath)

		if err != nil {
			return err
		}

		databases[file.Name()] = *database

		fmt.Println("Loaded database " + file.Name())
	}

	return nil
}
