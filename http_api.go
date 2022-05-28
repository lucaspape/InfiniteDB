package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io/ioutil"
	"net/http"
)

const apiPrefix = ""

func runHttpApi() {
	r := gin.Default()

	registerHttpHandlers(r)

	r.Run()
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
	var databaseNames []string

	for key := range databases {
		databaseNames = append(databaseNames, key)
	}

	c.JSON(http.StatusOK, gin.H{"databases": databaseNames})
}

func createDatabaseHandler(c *gin.Context) {
	body := getBody(c)

	if body != nil {
		name := (*body)["name"].(string)

		err := createDatabase(name)

		if err == nil {
			c.JSON(http.StatusOK, gin.H{"message": "Created database", "name": name})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func getDatabaseHandler(c *gin.Context) {
	name := c.Param("name")

	c.JSON(http.StatusOK, gin.H{"name": name})
}

func getDatabaseTablesHandler(c *gin.Context) {
	name := c.Param("name")

	var tableNames []string

	database := databases[name]

	for tableName := range database.Tables {
		tableNames = append(tableNames, tableName)
	}

	c.JSON(http.StatusOK, gin.H{"name": name, "tables": tableNames})
}

func createTableInDatabaseHandler(c *gin.Context) {
	body := getBody(c)

	if body != nil {
		name := c.Param("name")

		fields := (*body)["fields"].(map[string]interface{})
		tableName := (*body)["name"].(string)

		parsedFields, err := parseFields(fields)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
			return
		}

		database := databases[name]

		err = database.createTable(tableName, parsedFields)

		if err == nil {
			c.JSON(http.StatusOK, gin.H{"message": "Created table", "name": name, "tableName": tableName})
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

		database := databases[name]

		objects, err := database.get(tableName, *body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
			return
		}

		var results []map[string]interface{}

		for _, object := range objects.objects {
			results = append(results, object.M)
		}

		c.JSON(http.StatusOK, results)
	}
}

//TODO check if table exists
func insertToDatabaseTableHandler(c *gin.Context) {
	body := getBody(c)

	if body != nil {
		name := c.Param("name")
		tableName := c.Param("tableName")

		database := databases[name]

		table := database.Tables[tableName]

		objectId := uuid.New().String()

		err := table.insert(*NewObject(objectId, *body))

		if err == nil {
			c.JSON(http.StatusOK, gin.H{"message": "Inserted object", "name": name, "tableName": tableName, "id": objectId})
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
