package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"net/http"
	"time"
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
		websocketApi.handler(c, c.Writer, c.Request)
	})
}

func (websocketApi WebsocketApi) handler(c *gin.Context, w http.ResponseWriter, r *http.Request) {
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
				if websocketApi.methodHandler(conn, c.ClientIP(), requestId.(string), *body) {
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

func (websocketApi WebsocketApi) methodHandler(conn *websocket.Conn, clientIp string, requestId string, r map[string]interface{}) bool {
	method := r["method"]

	closed := false
	status := 0
	since := time.Now()

	if method != nil {
		switch method.(string) {
		case "getDatabases":
			closed, status = websocketApi.getDatabasesHandler(conn, requestId)
			break
		case "createDatabase":
			closed, status = websocketApi.createDatabaseHandler(conn, requestId, r)
			break
		case "getDatabase":
			closed, status = websocketApi.getDatabaseHandler(conn, requestId, r)
			break
		case "getDatabaseTables":
			closed, status = websocketApi.getDatabaseTablesHandler(conn, requestId, r)
			break
		case "createTableInDatabase":
			closed, status = websocketApi.createTableInDatabaseHandler(conn, requestId, r)
			break
		case "getFromDatabaseTable":
			closed, status = websocketApi.getFromDatabaseTableHandler(conn, requestId, r)
			break
		case "insertToDatabaseTable":
			closed, status = websocketApi.insertToDatabaseTableHandler(conn, requestId, r)
			break
		default:
			closed = websocketApi.send(conn, requestId, gin.H{"status": http.StatusInternalServerError, "message": "method not found"})
			status = http.StatusInternalServerError
		}

		websocketApi.logHandler(method.(string), status, since, clientIp, requestId)
	} else {
		closed = websocketApi.send(conn, requestId, gin.H{"status": http.StatusInternalServerError, "message": "no method specified"})
	}

	return closed
}

func (websocketApi WebsocketApi) getDatabasesHandler(conn *websocket.Conn, requestId string) (bool, int) {
	results, err := websocketApi.api.GetDatabases()

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) createDatabaseHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) (bool, int) {
	name := r["name"]

	results, err := websocketApi.api.CreateDatabase(name)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) getDatabaseHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) (bool, int) {
	name := r["name"]

	results, err := websocketApi.api.GetDatabase(name)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) getDatabaseTablesHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) (bool, int) {
	name := r["name"]

	results, err := websocketApi.api.GetDatabaseTables(name)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) createTableInDatabaseHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) (bool, int) {
	name := r["name"]
	tableName := r["tableName"]
	fields := r["fields"]

	results, err := websocketApi.api.CreateTableInDatabase(name, tableName, fields)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) getFromDatabaseTableHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) (bool, int) {
	name := r["name"]
	tableName := r["tableName"]
	request := r["request"]

	results, err := websocketApi.api.GetFromDatabaseTable(name, tableName, request)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) insertToDatabaseTableHandler(conn *websocket.Conn, requestId string, r map[string]interface{}) (bool, int) {
	name := r["name"]
	tableName := r["tableName"]
	object := r["object"]

	results, err := websocketApi.api.InsertToDatabaseTable(name, tableName, object)

	return websocketApi.sendResults(conn, requestId, results, err)
}

func (websocketApi WebsocketApi) sendResults(conn *websocket.Conn, requestId string, results map[string]interface{}, err error) (bool, int) {
	if err == nil {
		results["status"] = http.StatusOK

		return websocketApi.send(conn, requestId, results), http.StatusOK
	} else {
		return websocketApi.send(conn, requestId, gin.H{"status": http.StatusInternalServerError, "message": fmt.Sprint(err)}), http.StatusInternalServerError
	}
}

func (websocketApi WebsocketApi) logHandler(method string, statusCode int, since time.Time, clientIp string, requestId string) {
	param := new(gin.LogFormatterParams)
	param.Path = method
	param.Method = http.MethodGet
	param.ClientIP = clientIp
	param.Latency = time.Since(since)
	param.StatusCode = statusCode
	param.TimeStamp = time.Now()

	statusColor := param.StatusCodeColor()
	methodColor := param.MethodColor()
	resetColor := param.ResetColor()

	param.Method = "Websocket"

	if param.Latency > time.Minute {
		param.Latency = param.Latency.Truncate(time.Second)
	}

	fmt.Print(fmt.Sprintf("[GIN-WS] %v |%s %3d %s| %13v | %15s | %s |%s %-7s %s %#v\n%s",
		param.TimeStamp.Format("2006/01/02 - 15:04:05"),
		statusColor, param.StatusCode, resetColor,
		param.Latency,
		param.ClientIP,
		requestId,
		methodColor, param.Method, resetColor,
		param.Path,
		param.ErrorMessage,
	))
}
