package data

import (
	"context"
	"net/http"

	"github.com/vearch/vearch/v3/sdk/go/vearch/connection"
	"github.com/vearch/vearch/v3/sdk/go/vearch/entities/models"
	"github.com/vearch/vearch/v3/sdk/go/vearch/except"
)

type DeleteResultDocs struct {
	Code int     `json:"code"`
	Msg  *string `json:"msg,omitempty"`
	Data struct {
		Total        int      `json:"total"`
		DocumentsIDs []string `json:"document_ids"`
	} `json:"data"`
}

type DeleteWrapper struct {
	Docs *DeleteResultDocs
}

type Deleter struct {
	connection *connection.Connection
	dbName     string
	spaceName  string
	ids        []string
	filters    *models.Filters
}

func (delete *Deleter) WithDBName(name string) *Deleter {
	delete.dbName = name
	return delete
}

func (delete *Deleter) WithSpaceName(name string) *Deleter {
	delete.spaceName = name
	return delete
}

func (delete *Deleter) WithIDs(ids []string) *Deleter {
	delete.ids = ids
	return delete
}

func (delete *Deleter) WithFilters(filters *models.Filters) *Deleter {
	delete.filters = filters
	return delete
}

func (query *Deleter) Do(ctx context.Context) (*DeleteWrapper, error) {
	var err error
	var responseData *connection.ResponseData
	req, _ := query.PayloadDoc()

	path := query.buildPath()
	responseData, err = query.connection.RunREST(ctx, path, http.MethodPost, req)
	respErr := except.CheckResponseDataErrorAndStatusCode(responseData, err, 200)
	if respErr != nil {
		return nil, respErr
	}

	var resultDoc DeleteResultDocs
	parseErr := responseData.DecodeBodyIntoTarget(&resultDoc)
	return &DeleteWrapper{
		Docs: &resultDoc,
	}, parseErr
}

func (query *Deleter) buildPath() string {
	path := "/document/delete"
	return path
}

func (query *Deleter) PayloadDoc() (*models.DeleteRequest, error) {
	doc := models.DeleteRequest{
		DBName:    query.dbName,
		SpaceName: query.spaceName,
		IDs:       query.ids,
		Filters:   query.filters,
	}
	return &doc, nil
}
