package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
)

const databasePath = "./databases/"

func main() {
	api := NewApi(databasePath)
	err := loadDatabases(api)

	if err != nil {
		fmt.Println(err)
		return
	}

	r := gin.Default()

	httpApi := NewHttpApi(api)
	httpApi.run(r)

	websocketApi := NewWebsocketApi(api)
	websocketApi.run(r)

	err = r.Run()

	if err != nil {
		fmt.Println(err)
		return
	}
}

func loadDatabases(api *Api) error {
	files, err := ioutil.ReadDir(databasePath)

	if err != nil {
		return err
	}

	for _, file := range files {
		err = api.loadDatabase(file.Name())
		
		if err != nil {
			return err
		}
	}

	return nil
}
