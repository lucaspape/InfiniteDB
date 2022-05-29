package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
)

const apiPrefix = ""

func runHttpApi() error {
	r := gin.Default()

	registerHttpHandlers(r)

	return r.Run()
}

func registerHttpHandlers(r *gin.Engine) {
	r.GET(apiPrefix+"/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Hello world"})
	})

	r.GET(apiPrefix+"/databases", getDatabasesHandler)
	r.POST(apiPrefix+"/database", createDatabaseHandler)
	r.GET(apiPrefix+"/database/:name", getDatabaseHandler)
	r.GET(apiPrefix+"/database/:name/tables", getDatabaseTablesHandler)
	r.POST(apiPrefix+"/database/:name/table", createTableInDatabaseHandler)
	r.POST(apiPrefix+"/database/:name/table/:tableName/get", getFromDatabaseTableHandler)
	r.POST(apiPrefix+"/database/:name/table/:tableName/insert", insertToDatabaseTableHandler)
}

func getDatabasesHandler(c *gin.Context) {
	results, err := api.GetDatabases()

	if err == nil {
		c.JSON(http.StatusOK, results)
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
	}
}

func createDatabaseHandler(c *gin.Context) {
	body := getBody(c)

	if body != nil {
		name := (*body)["name"].(string)

		results, err := api.CreateDatabase(name)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func getDatabaseHandler(c *gin.Context) {
	name := c.Param("name")

	results, err := api.GetDatabase(name)

	if err == nil {
		c.JSON(http.StatusOK, results)
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
	}
}

func getDatabaseTablesHandler(c *gin.Context) {
	name := c.Param("name")

	results, err := api.GetDatabaseTables(name)

	if err == nil {
		c.JSON(http.StatusOK, results)
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
	}
}

func createTableInDatabaseHandler(c *gin.Context) {
	body := getBody(c)

	if body != nil {
		name := c.Param("name")

		fields := (*body)["fields"].(map[string]interface{})
		tableName := (*body)["name"].(string)

		results, err := api.CreateTableInDatabase(name, tableName, fields)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func getFromDatabaseTableHandler(c *gin.Context) {
	body := getBody(c)

	if body != nil {
		name := c.Param("name")
		tableName := c.Param("tableName")

		results, err := api.GetFromDatabaseTable(name, tableName, *body)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

//TODO check if table exists
func insertToDatabaseTableHandler(c *gin.Context) {
	body := getBody(c)

	if body != nil {
		name := c.Param("name")
		tableName := c.Param("tableName")

		results, err := api.InsertToDatabaseTable(name, tableName, *body)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func getBody(c *gin.Context) *map[string]interface{} {
	bytes, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to parse JSON"})
		return nil
	}

	var m map[string]interface{}
	err = json.Unmarshal(bytes, &m)

	return &m
}
