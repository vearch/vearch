package connection

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"runtime"
)

type Connection struct {
	basePath   string
	httpClient *http.Client
	headers    map[string]string
	doneCh     chan bool
}

func finalizer(c *Connection) {
	c.doneCh <- true
}

func NewConnection(host string, httpClient *http.Client, headers map[string]string) *Connection {
	client := httpClient
	if client == nil {
		client = &http.Client{}
	}
	connection := &Connection{
		basePath:   host,
		httpClient: client,
		headers:    headers,
		doneCh:     make(chan bool),
	}

	runtime.SetFinalizer(connection, finalizer)

	return connection
}

func (con *Connection) addHeaderToRequest(request *http.Request) {
	for k, v := range con.headers {
		request.Header.Add(k, v)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
}

func (con *Connection) marshalBody(body interface{}) (io.Reader, error) {
	if body == nil {
		return nil, nil
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(jsonBody), nil
}

func (con *Connection) createRequest(ctx context.Context, path string, restMethod string, body interface{}) (*http.Request, error) {
	url := con.basePath + path

	jsonBody, err := con.marshalBody(body)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest(restMethod, url, jsonBody)
	if err != nil {
		return nil, err
	}
	con.addHeaderToRequest(request)
	request = request.WithContext(ctx)
	return request, nil
}

func (con *Connection) RunREST(ctx context.Context, path string, restMethod string, requestBody interface{}) (*ResponseData, error) {
	request, requestErr := con.createRequest(ctx, path, restMethod, requestBody)
	if requestErr != nil {
		return nil, requestErr
	}
	response, responseErr := con.httpClient.Do(request)
	if responseErr != nil {
		return nil, responseErr
	}

	defer response.Body.Close()
	body, bodyErr := io.ReadAll(response.Body)
	if bodyErr != nil {
		return nil, bodyErr
	}

	return &ResponseData{
		Body:       body,
		StatusCode: response.StatusCode,
	}, nil
}

type ResponseData struct {
	Body       []byte
	StatusCode int
}
