package schema

import (
	"context"
	"fmt"
	"net/http"

	"github.com/vearch/vearch/v3/sdk/go/connection"
	"github.com/vearch/vearch/v3/sdk/go/entities/models"
	"github.com/vearch/vearch/v3/sdk/go/except"
)

type SpaceCreator struct {
	connection *connection.Connection
	space      *models.Space
	dbName     string
}

func (sc *SpaceCreator) WithDBName(dbName string) *SpaceCreator {
	sc.dbName = dbName
	return sc
}

func (sc *SpaceCreator) WithSpace(space *models.Space) *SpaceCreator {
	sc.space = space
	return sc
}

func (sc *SpaceCreator) Do(ctx context.Context) error {
	responseData, err := sc.connection.RunREST(ctx, fmt.Sprintf("/dbs/%s/spaces", sc.dbName), http.MethodPost, sc.space)
	return except.CheckResponseDataErrorAndStatusCode(responseData, err, 200)
}
