package schema

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vearch/vearch/v3/sdk/go/vearch/connection"
	"github.com/vearch/vearch/v3/sdk/go/vearch/entities/models"
	"github.com/vearch/vearch/v3/sdk/go/vearch/except"
)

type DBCreator struct {
	connection *connection.Connection
	db         *models.DB
}

func (dc *DBCreator) WithDB(db *models.DB) *DBCreator {
	dc.db = db
	return dc
}

func (dc *DBCreator) Do(ctx context.Context) error {
	responseData, err := dc.connection.RunREST(ctx, fmt.Sprintf("/dbs/%s", dc.db.Name), http.MethodPost, nil)
	return except.CheckResponseDataErrorAndStatusCode(responseData, err, 200)
}
