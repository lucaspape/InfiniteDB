package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/lucaspape/idblib"
	"io/ioutil"
	"net/http"
)

const apiPrefix = ""

type HttpApi struct {
	api *idblib.Api
}

func NewHttpApi(api *idblib.Api) *HttpApi {
	httpApi := new(HttpApi)

	httpApi.api = api

	return httpApi
}

func (httpApi HttpApi) run(r *gin.Engine) {
	httpApi.registerHandlers(r)
}

func (httpApi HttpApi) registerHandlers(r *gin.Engine) {
	r.GET(apiPrefix+"/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Hello world"})
	})

	r.GET(apiPrefix+"/databases", httpApi.getDatabasesHandler)
	r.POST(apiPrefix+"/database", httpApi.createDatabaseHandler)
	r.GET(apiPrefix+"/database/:name", httpApi.getDatabaseHandler)
	r.GET(apiPrefix+"/database/:name/tables", httpApi.getDatabaseTablesHandler)
	r.POST(apiPrefix+"/database/:name/table", httpApi.createTableInDatabaseHandler)
	r.POST(apiPrefix+"/database/:name/table/:tableName/get", httpApi.getFromDatabaseTableHandler)
	r.POST(apiPrefix+"/database/:name/table/:tableName/insert", httpApi.insertToDatabaseTableHandler)
	r.POST(apiPrefix+"/database/:name/table/:tableName/remove", httpApi.removeFromDatabaseTableHandler)
	r.POST(apiPrefix+"/database/:name/table/:tableName/update", httpApi.updateInDatabaseTableHandler)
}

func (httpApi HttpApi) getDatabasesHandler(c *gin.Context) {
	results, err := httpApi.api.GetDatabases()

	if err == nil {
		c.JSON(http.StatusOK, results)
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
	}
}

func (httpApi HttpApi) createDatabaseHandler(c *gin.Context) {
	body := httpApi.getBody(c)

	if body != nil {
		name := (*body)["name"]

		results, err := httpApi.api.CreateDatabase(name)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func (httpApi HttpApi) getDatabaseHandler(c *gin.Context) {
	name := c.Param("name")

	results, err := httpApi.api.GetDatabase(name)

	if err == nil {
		c.JSON(http.StatusOK, results)
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
	}
}

func (httpApi HttpApi) getDatabaseTablesHandler(c *gin.Context) {
	name := c.Param("name")

	results, err := httpApi.api.GetDatabaseTables(name)

	if err == nil {
		c.JSON(http.StatusOK, results)
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
	}
}

func (httpApi HttpApi) createTableInDatabaseHandler(c *gin.Context) {
	body := httpApi.getBody(c)

	if body != nil {
		name := c.Param("name")

		fields := (*body)["fields"]
		tableName := (*body)["name"]

		results, err := httpApi.api.CreateTableInDatabase(name, tableName, fields)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func (httpApi HttpApi) getFromDatabaseTableHandler(c *gin.Context) {
	body := httpApi.getBody(c)

	if body != nil {
		name := c.Param("name")
		tableName := c.Param("tableName")

		results, err := httpApi.api.GetFromDatabaseTable(name, tableName, *body)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

//TODO check if table exists
func (httpApi HttpApi) insertToDatabaseTableHandler(c *gin.Context) {
	body := httpApi.getBody(c)

	if body != nil {
		name := c.Param("name")
		tableName := c.Param("tableName")

		results, err := httpApi.api.InsertToDatabaseTable(name, tableName, *body)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func (httpApi HttpApi) removeFromDatabaseTableHandler(c *gin.Context) {
	body := httpApi.getBody(c)

	if body != nil {
		name := c.Param("name")
		tableName := c.Param("tableName")

		results, err := httpApi.api.RemoveFromDatabaseTable(name, tableName, *body)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func (httpApi HttpApi) updateInDatabaseTableHandler(c *gin.Context) {
	body := httpApi.getBody(c)

	if body != nil {
		name := c.Param("name")
		tableName := c.Param("tableName")

		results, err := httpApi.api.UpdateInDatabaseTable(name, tableName, *body)

		if err == nil {
			c.JSON(http.StatusOK, results)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprint(err)})
		}
	}
}

func (httpApi HttpApi) getBody(c *gin.Context) *map[string]interface{} {
	bytes, err := ioutil.ReadAll(c.Request.Body)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to parse JSON"})
		return nil
	}

	var m map[string]interface{}
	err = json.Unmarshal(bytes, &m)

	return &m
}
