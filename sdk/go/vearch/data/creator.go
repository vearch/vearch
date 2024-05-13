package data

import (
	"context"
	"net/http"

	"github.com/vearch/vearch/v3/sdk/go/vearch/connection"
	"github.com/vearch/vearch/v3/sdk/go/vearch/entities/models"
	"github.com/vearch/vearch/v3/sdk/go/vearch/except"
)

type ResultDocs struct {
	Code int `json:"code"`
	Data struct {
		Total       int `json:"total"`
		DocumentIds []struct {
			ID string `json:"_id"`
		} `json:"document_ids"`
	} `json:"data"`
}

type DocWrapper struct {
	Docs *ResultDocs
}

type Creator struct {
	connection *connection.Connection
	dbName     string
	spaceName  string
	documents  []interface{}
}

func (creator *Creator) WithDBName(name string) *Creator {
	creator.dbName = name
	return creator
}

func (creator *Creator) WithSpaceName(name string) *Creator {
	creator.spaceName = name
	return creator
}

func (creator *Creator) WithDocs(documents []interface{}) *Creator {
	creator.documents = documents
	return creator
}

func (creator *Creator) Do(ctx context.Context) (*DocWrapper, error) {
	var err error
	var responseData *connection.ResponseData
	doc, _ := creator.PayloadDoc()

	path := creator.buildPath()
	responseData, err = creator.connection.RunREST(ctx, path, http.MethodPost, doc)
	respErr := except.CheckResponseDataErrorAndStatusCode(responseData, err, 200)
	if respErr != nil {
		return nil, respErr
	}

	var resultDoc ResultDocs
	parseErr := responseData.DecodeBodyIntoTarget(&resultDoc)
	return &DocWrapper{
		Docs: &resultDoc,
	}, parseErr
}

func (creator *Creator) buildPath() string {
	path := "/document/upsert"
	return path
}

func (creator *Creator) PayloadDoc() (*models.Docs, error) {
	doc := models.Docs{
		DBName:    creator.dbName,
		SpaceName: creator.spaceName,
		Documents: creator.documents,
	}
	return &doc, nil
}
