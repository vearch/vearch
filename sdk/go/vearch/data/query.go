package data

import (
	"context"
	"net/http"

	"github.com/vearch/vearch/v3/sdk/go/vearch/connection"
	"github.com/vearch/vearch/v3/sdk/go/vearch/entities/models"
	"github.com/vearch/vearch/v3/sdk/go/vearch/except"
)

type QueryResultDocs struct {
	Code int     `json:"code"`
	Msg  *string `json:"msg,omitempty"`
	Data struct {
		Documents []interface{} `json:"documents"`
	} `json:"data"`
}

type QueryWrapper struct {
	Docs *QueryResultDocs
}

type Query struct {
	connection *connection.Connection
	dbName     string
	spaceName  string
	ids        []string
	filters    *models.Filters
}

func (query *Query) WithDBName(name string) *Query {
	query.dbName = name
	return query
}

func (query *Query) WithSpaceName(name string) *Query {
	query.spaceName = name
	return query
}

func (query *Query) WithIDs(ids []string) *Query {
	query.ids = ids
	return query
}

func (query *Query) WithFilters(filters *models.Filters) *Query {
	query.filters = filters
	return query
}

func (query *Query) Do(ctx context.Context) (*QueryWrapper, error) {
	var err error
	var responseData *connection.ResponseData
	req, _ := query.PayloadDoc()

	path := query.buildPath()
	responseData, err = query.connection.RunREST(ctx, path, http.MethodPost, req)
	respErr := except.CheckResponseDataErrorAndStatusCode(responseData, err, 200)
	if respErr != nil {
		return nil, respErr
	}

	var resultDoc QueryResultDocs
	parseErr := responseData.DecodeBodyIntoTarget(&resultDoc)
	return &QueryWrapper{
		Docs: &resultDoc,
	}, parseErr
}

func (query *Query) buildPath() string {
	path := "/document/query"
	return path
}

func (query *Query) PayloadDoc() (*models.QueryRequest, error) {
	doc := models.QueryRequest{
		DBName:    query.dbName,
		SpaceName: query.spaceName,
		IDs:       query.ids,
		Filters:   query.filters,
	}
	return &doc, nil
}
