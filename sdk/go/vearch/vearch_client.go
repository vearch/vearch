package vearch

import (
	"net/http"

	"github.com/vearch/vearch/v3/sdk/go/vearch/auth"
	"github.com/vearch/vearch/v3/sdk/go/vearch/connection"
	"github.com/vearch/vearch/v3/sdk/go/vearch/data"
	"github.com/vearch/vearch/v3/sdk/go/vearch/schema"
)

type Config struct {
	Host             string
	ConnectionClient *http.Client
	AuthConfig       auth.Config
	Headers          map[string]string
}

type Client struct {
	connection *connection.Connection
	schema     *schema.API
	data       *data.API
}

func NewClient(config Config) (*Client, error) {
	if config.AuthConfig != nil {
		tmpCon := connection.NewConnection(config.Host, nil, config.Headers)
		connectionClient, additionalHeaders, err := config.AuthConfig.GetAuthInfo(tmpCon)
		if err != nil {
			return nil, err
		}
		config.ConnectionClient = connectionClient
		if config.Headers == nil {
			config.Headers = map[string]string{}
		}
		for k, v := range additionalHeaders {
			config.Headers[k] = v
		}
	}
	con := connection.NewConnection(config.Host, config.ConnectionClient, config.Headers)
	client := &Client{
		connection: con,
		schema:     schema.New(con),
		data:       data.New(con),
	}
	return client, nil
}

func (c *Client) Schema() *schema.API {
	return c.schema
}

func (c *Client) Data() *data.API {
	return c.data
}
