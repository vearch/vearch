package auth

import (
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/vearch/vearch/sdk/go/v3/connection"
)

type Config interface {
	GetAuthInfo(con *connection.Connection) (*http.Client, map[string]string, error)
}

type BasicAuth struct {
	UserName string
	Secret   string
}

// Returns the header used for the authentification.
func (bs BasicAuth) GetAuthInfo(con *connection.Connection) (*http.Client, map[string]string, error) {
	additional_headers := make(map[string]string)
	additional_headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", bs.UserName, bs.Secret)))
	return nil, additional_headers, nil
}
