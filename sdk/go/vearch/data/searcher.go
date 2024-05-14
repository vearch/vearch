package data

import (
	"context"
	"net/http"

	"github.com/vearch/vearch/v3/sdk/go/vearch/connection"
	"github.com/vearch/vearch/v3/sdk/go/vearch/entities/models"
	"github.com/vearch/vearch/v3/sdk/go/vearch/except"
)

type SearchResultDocs struct {
	Code int     `json:"code"`
	Msg  *string `json:"msg,omitempty"`
	Data struct {
		Documents []interface{} `json:"documents"`
	} `json:"data"`
}

type SearchWrapper struct {
	Docs *SearchResultDocs
}

type Searcher struct {
	connection *connection.Connection
	dbName     string
	spaceName  string
	limit      int
	vectors    []models.Vector
	filters    *models.Filters
}

func (searcher *Searcher) WithDBName(name string) *Searcher {
	searcher.dbName = name
	return searcher
}

func (searcher *Searcher) WithSpaceName(name string) *Searcher {
	searcher.spaceName = name
	return searcher
}

func (searcher *Searcher) WithLimit(limit int) *Searcher {
	searcher.limit = limit
	return searcher
}

func (searcher *Searcher) WithVectors(vectors []models.Vector) *Searcher {
	searcher.vectors = vectors
	return searcher
}

func (searcher *Searcher) WithFilters(filters *models.Filters) *Searcher {
	searcher.filters = filters
	return searcher
}

func (searcher *Searcher) Do(ctx context.Context) (*SearchWrapper, error) {
	var err error
	var responseData *connection.ResponseData
	req, _ := searcher.PayloadDoc()

	path := searcher.buildPath()
	responseData, err = searcher.connection.RunREST(ctx, path, http.MethodPost, req)
	respErr := except.CheckResponseDataErrorAndStatusCode(responseData, err, 200)
	if respErr != nil {
		return nil, respErr
	}

	var resultDoc SearchResultDocs
	parseErr := responseData.DecodeBodyIntoTarget(&resultDoc)
	return &SearchWrapper{
		Docs: &resultDoc,
	}, parseErr
}

func (searcher *Searcher) buildPath() string {
	path := "/document/search"
	return path
}

func (searcher *Searcher) PayloadDoc() (*models.SearchRequest, error) {
	doc := models.SearchRequest{
		DBName:    searcher.dbName,
		SpaceName: searcher.spaceName,
		Limit:     searcher.limit,
		Vectors:   searcher.vectors,
		Filters:   searcher.filters,
	}
	return &doc, nil
}
