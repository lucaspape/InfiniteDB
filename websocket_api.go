package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"net/http"
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type WebsocketApi struct {
	api *Api
}

func NewWebsocketApi(api *Api) *WebsocketApi {
	websocketApi := new(WebsocketApi)

	websocketApi.api = api

	return websocketApi
}

func (websocketApi WebsocketApi) run(r *gin.Engine) {
	websocketApi.registerHandler(r)
}

func (websocketApi WebsocketApi) registerHandler(r *gin.Engine) {
	r.GET("/ws", func(c *gin.Context) {
		websocketApi.handler(c.Writer, c.Request)
	})
}

func (websocketApi WebsocketApi) handler(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)

	if err != nil {
		fmt.Printf("failed to set websocket upgrade: %+v\n", err)
		return
	}

	for {
		_, bytes, err := conn.ReadMessage()

		if err != nil {
			closed := websocketApi.send(conn, "", gin.H{"status": http.StatusInternalServerError, "message": "failed to read message"})

			if closed {
				return
			}
		}

		body, closed := websocketApi.getBody(conn, bytes)

		if closed {
			return
		}

		if body != nil {
			requestId := (*body)["requestId"]

			if requestId != nil {
				if websocketApi.methodHandler(conn, requestId.(string), *body) {
					return
				}
			} else {
				if websocketApi.send(conn, "", gin.H{"status": http.StatusInternalServerError, "message": "every request must have a requestId"}) {
					return
				}
			}
		}
	}
}

func (websocketApi WebsocketApi) getBody(conn *websocket.Conn, bytes []byte) (*map[string]interface{}, bool) {
	var r map[string]interface{}
	err := json.Unmarshal(bytes, &r)

	if err != nil {
		return nil, websocketApi.send(conn, "", gin.H{"status": http.StatusInternalServerError, "message": "failed to parse JSON"})
	}

	return &r, false
}

func (websocketApi WebsocketApi) send(conn *websocket.Conn, requestId string, m map[string]interface{}) bool {
	m["requestId"] = requestId

	err := conn.WriteJSON(m)

	if err != nil {
		fmt.Println(err)
		err = conn.Close()

		if err != nil {
			fmt.Println(err)
		}

		return true
	}

	return false
}

func (websocketApi WebsocketApi) methodHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) bool {
	method := r["method"]

	closed := false

	if method != nil {
		switch method.(string) {
		case "getDatabases":
			closed = websocketApi.getDatabasesHandler(conn, requestId)
			break
		case "createDatabase":
			closed = websocketApi.createDatabaseHandler(conn, requestId, r)
			break
		case "getDatabase":
			closed = websocketApi.getDatabaseHandler(conn, requestId, r)
			break
		case "getDatabaseTables":
			closed = websocketApi.getDatabaseTablesHandler(conn, requestId, r)
			break
		case "createTableInDatabase":
			closed = websocketApi.createTableInDatabaseHandler(conn, requestId, r)
			break
		case "getFromDatabaseTable":
			closed = websocketApi.getFromDatabaseTableHandler(conn, requestId, r)
			break
		case "insertToDatabaseTable":
			closed = websocketApi.insertToDatabaseTableHandler(conn, requestId, r)
			break
		default:
			closed = websocketApi.send(conn, requestId, gin.H{"status": http.StatusInternalServerError, "message": "method not found"})
		}
	} else {
		closed = websocketApi.send(conn, requestId, gin.H{"status": http.StatusInternalServerError, "message": "no method specified"})
	}

	return closed
}

func (websocketApi WebsocketApi) getDatabasesHandler(conn *websocket.Conn, requestId string) bool {
	results, err := websocketApi.api.GetDatabases()

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) createDatabaseHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) bool {
	name := r["name"]

	results, err := websocketApi.api.CreateDatabase(name)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) getDatabaseHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) bool {
	name := r["name"]

	results, err := websocketApi.api.GetDatabase(name)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) getDatabaseTablesHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) bool {
	name := r["name"]

	results, err := websocketApi.api.GetDatabaseTables(name)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) createTableInDatabaseHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) bool {
	name := r["name"]
	tableName := r["tableName"]
	fields := r["fields"]

	results, err := websocketApi.api.CreateTableInDatabase(name, tableName, fields)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) getFromDatabaseTableHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) bool {
	name := r["name"]
	tableName := r["tableName"]
	request := r["request"]

	results, err := websocketApi.api.GetFromDatabaseTable(name, tableName, request)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) insertToDatabaseTableHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) bool {
	name := r["name"]
	tableName := r["tableName"]
	object := r["object"]

	results, err := websocketApi.api.InsertToDatabaseTable(name, tableName, object)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) sendResults(conn *websocket.Conn, requestId string, results map[string]interface{}, err error) bool {
	if err == nil {
		return websocketApi.send(conn, requestId, results)
	} else {
		return websocketApi.send(conn, requestId, gin.H{"status": http.StatusInternalServerError, "message": fmt.Sprint(err)})
	}
}
