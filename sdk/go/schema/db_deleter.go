package schema

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vearch/vearch/v3/sdk/go/connection"
	"github.com/vearch/vearch/v3/sdk/go/except"
)

type DBDeleter struct {
	connection *connection.Connection
	dbName     string
}

func (dc *DBDeleter) WithDBName(dbName string) *DBDeleter {
	dc.dbName = dbName
	return dc
}

func (dc *DBDeleter) Do(ctx context.Context) error {
	responseData, err := dc.connection.RunREST(ctx, fmt.Sprintf("/dbs/%s", dc.dbName), http.MethodDelete, nil)
	return except.CheckResponseDataErrorAndStatusCode(responseData, err, 200, 204)
}
