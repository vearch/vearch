package vearch

import (
	"net/http"

	"github.com/vearch/vearch/sdk/go/v3/vearch/connection"
	"github.com/vearch/vearch/sdk/go/v3/vearch/schema"
)

type Config struct {
	Host             string
	ConnectionClient *http.Client
	Headers          map[string]string
}

type Client struct {
	connection *connection.Connection
	schema     *schema.API
}

func NewClient(config Config) (*Client, error) {
	con := connection.NewConnection(config.Host, config.ConnectionClient, config.Headers)
	client := &Client{
		connection: con,
		schema:     schema.New(con),
	}
	return client, nil
}

func (c *Client) Schema() *schema.API {
	return c.schema
}
