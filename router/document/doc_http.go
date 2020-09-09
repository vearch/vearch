// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package document

import (
	"context"
	"fmt"
	"github.com/vearch/vearch/proto/entity"
	"net/http"
	"strings"
	"time"

	"github.com/vearch/vearch/util/log"

	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/monitor"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/router/document/resp"
	"github.com/vearch/vearch/util/netutil"
)

const (
	URLParamDbName    = "db_name"
	URLParamSpaceName = "space_name"
	URLParamID        = "_id"
	URLParams         = "url_params"
	ReqsBody          = "req_body"
	SpaceEntity       = "space_entity"
	IDType            = "id_type"
	IDIsLong          = "IDIsLong"
	QueryIsOnlyID     = "QueryIsOnlyID"
	URLQueryTimeout   = "timeout"
)

type DocumentHandler struct {
	httpServer *netutil.Server
	docService docService
	client     *client.Client
}

func ExportDocumentHandler(httpServer *netutil.Server, client *client.Client) {
	docService := newDocService(client)

	documentHandler := &DocumentHandler{
		httpServer: httpServer,
		docService: *docService,
		client:     client,
	}
	if err := documentHandler.ExportToServer(); err != nil {
		panic(err)
	}

}

func (handler *DocumentHandler) ExportToServer() error {
	// bulk: /$dbName/$spaceName/_bulk
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_bulk", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleBulk}, nil)

	// search doc: /$dbName/$spaceName/_search
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_search", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleSearchDoc}, nil)

	// msearch doc: /$dbName/$spaceName/_search
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_msearch", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMSearchDoc}, nil)

	// search doc: /$dbName/$spaceName/_msearch_ids
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_msearch_ids", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMSearchIdsDoc}, nil)

	// bulk: /$dbName/$spaceName/_query_byids
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_query_byids", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handlerQueryDocByIds}, nil)

	// bulk: /$dbName/$spaceName/_query_byids_feture
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_query_byids_feature", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handlerQueryDocByIdsFeature}, nil)

	// bulk: /$dbName/$spaceName/_query_byids_feture
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_bulk_search", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleBulkSearchDoc}, nil)

	// delete: /$dbName/$spaceName/_delete_by_query
	handler.httpServer.HandlesMethods([]string{http.MethodDelete, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_delete_by_query", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDeleteByQuery}, nil)

	// forcemerge space: /$dbName/$spaceName/_forcemerge
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_forcemerge", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleForceMerge}, nil)

	// get doc: /$dbName/$spaceName/$docId
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/{%s}/{%s}/{%s}", URLParamDbName, URLParamSpaceName, URLParamID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleGetDoc}, nil)

	// delete doc: /$dbName/$spaceName/$docId
	handler.httpServer.HandlesMethods([]string{http.MethodDelete}, fmt.Sprintf("/{%s}/{%s}/{%s}", URLParamDbName, URLParamSpaceName, URLParamID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDeleteDoc}, nil)

	// create doc: /$dbName/$spaceName/$docId/_create
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/{%s}/_create", URLParamDbName, URLParamSpaceName, URLParamID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleUpdateDoc}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodPut}, fmt.Sprintf("/{%s}/{%s}/{%s}", URLParamDbName, URLParamSpaceName, URLParamID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleUpdateDoc}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodPut}, fmt.Sprintf("/{%s}/{%s}", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleUpdateDoc}, nil)

	// update doc: /$dbName/$spaceName/$docId/_update
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodPut}, fmt.Sprintf("/{%s}/{%s}/{%s}/_update", URLParamDbName, URLParamSpaceName, URLParamID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleUpdateDoc}, nil)

	return nil
}

func (handler *DocumentHandler) handleTimeout(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	return ctx, true
}

func (handler *DocumentHandler) handleAuth(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	return ctx, true
}

func (handler *DocumentHandler) handleRouterInfo(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	versionLayer := make(map[string]interface{})
	versionLayer["build_version"] = config.GetBuildVersion()
	versionLayer["build_time"] = config.GetBuildTime()
	versionLayer["commit_id"] = config.GetCommitID()

	layer := make(map[string]interface{})
	layer["cluster_name"] = config.Conf().Global.Name
	layer["version"] = versionLayer

	resp.SendJson(ctx, w, layer)
	return ctx, true
}

func (handler *DocumentHandler) cacheInfo(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	reqArgs := netutil.GetUrlQuery(r)
	dbName := reqArgs[URLParamDbName]
	spaceName := reqArgs[URLParamSpaceName]
	if space, err := handler.client.Master().Cache().SpaceByCache(context.Background(), dbName, spaceName); err != nil {
		resp.SendErrorRootCause(ctx, w, 404, err.Error(), err.Error())
	} else {
		resp.SendJson(ctx, w, space)
	}
	return ctx, true
}

func (handler *DocumentHandler) handleGetDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleGetDoc", startTime)
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHead(params, r)
	args.PrimaryKeys = strings.Split(params.ByName(URLParamID), ",")
	space, err := handler.client.Space(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	reply := handler.docService.getDocs(ctx, args)
	if resultBytes, err := docGetResponse(handler.client, args, reply, nil); err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	} else {
		resp.SendJsonBytes(ctx, w, resultBytes)
		return ctx, true
	}
}

func (handler *DocumentHandler) handleDeleteDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleDeleteDoc", startTime)
	args := &vearchpb.DeleteRequest{}
	args.Head = setRequestHead(params, r)
	args.PrimaryKeys = strings.Split(params.ByName(URLParamID), ",")
	space, err := handler.client.Space(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	reply := handler.docService.deleteDocs(ctx, args)
	if resultBytes, err := docDeleteResponses(handler.client, args, reply); err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	} else {
		resp.SendJsonBytes(ctx, w, resultBytes)
		return ctx, true
	}
}

func (handler *DocumentHandler) handleUpdateDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleUpdateDoc", startTime)
	args := &vearchpb.UpdateRequest{}
	args.Head = setRequestHead(params, r)
	space, err := handler.client.Space(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	err = docParse(ctx, r, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	args.Doc.PKey = params.ByName(URLParamID)
	reply := handler.docService.updateDoc(ctx, args)
	if resultBytes, err := docUpdateResponses(handler.client, args, reply); err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	} else {
		resp.SendJsonBytes(ctx, w, resultBytes)
		return ctx, true
	}
}

// handleBulk For add documents by batch
func (handler *DocumentHandler) handleBulk(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	t1 := time.Now()
	defer monitor.Profiler("handleBulk", t1)
	args := &vearchpb.BulkRequest{}
	args.Head = setRequestHead(params, r)
	space, err := handler.client.Space(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if space.SpaceProperties == nil {
		spaceProperties, _ := entity.UnmarshalPropertyJSON(space.Properties)
		space.SpaceProperties = spaceProperties
	}

	err = docBulkParse(ctx, r, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	reply := handler.docService.bulk(ctx, args)
	if resultBytes, err := docBulkResponses(handler.client, args, reply); err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	} else {
		resp.SendJsonBytes(ctx, w, resultBytes)
		return ctx, true
	}
	resp.SendText(ctx, w, reply.String())
	return ctx, true
}

// handleSearchDoc for search by param
func (handler *DocumentHandler) handleSearchDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleSearchDoc", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)
	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "query Cache space null")
		return ctx, false
	}
	err = docSearchParse(r, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	serviceStart := time.Now()
	searchResp := handler.docService.search(ctx, args)
	serviceCost := time.Now().Sub(serviceStart)

	var bs []byte
	if searchResp.Results == nil || len(searchResp.Results) == 0 {
		searchStatus := vearchpb.SearchStatus{Failed: 0, Successful: 0, Total: 0}
		bs, err = SearchNullToContent(searchStatus, serviceCost)
	} else {
		bs, err = ToContent(searchResp.Results[0], args.Head, serviceCost, space)
	}

	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)
	endTime := time.Now()
	log.Debug("search total use :[%f] service use :[%f]",
		(endTime.Sub(startTime).Seconds())*1000, serviceCost.Seconds()*1000)
	return ctx, true
}

// handleMSearchDoc for search by param
func (handler *DocumentHandler) handleMSearchDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleMSearchDoc", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)
	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "query Cache space null")
		return ctx, false
	}
	err = docSearchParse(r, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	serviceStart := time.Now()
	searchRes := handler.docService.search(ctx, args)
	serviceEnd := time.Now()
	serviceCost := serviceEnd.Sub(serviceStart)

	bs, err := ToContents(searchRes.Results, args.Head, serviceCost, space)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)
	endTime := time.Now()
	log.Debug("msearch total use :[%f] service use :[%f]",
		(endTime.Sub(startTime).Seconds())*1000, serviceCost.Seconds()*1000)
	return ctx, true
}

// handleMSearchIdsDoc for search by param
func (handler *DocumentHandler) handleMSearchIdsDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleMSearchIdsDoc", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)
	if args.Head.Params != nil {
		paramMap := args.Head.Params
		paramMap["queryOnlyId"] = "true"
		args.Head.Params = paramMap
	}
	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "query Cache space null")
		return ctx, false
	}
	err = docSearchParse(r, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	serviceStart := time.Now()
	searchRes := handler.docService.search(ctx, args)
	serviceEnd := time.Now()
	serviceCost := serviceEnd.Sub(serviceStart)

	bs, err := ToContentIds(searchRes.Results, space)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)
	endTime := time.Now()
	log.Debug("msearchids total use :[%f] service use :[%f]",
		(endTime.Sub(startTime).Seconds())*1000, serviceCost.Seconds()*1000)
	return ctx, true
}

// getUrlQuery get param in uri by name
func getUrlQuery(r *http.Request, name string) (value string) {
	reqArgs := netutil.GetUrlQuery(r)
	if value, ok := reqArgs[name]; ok {
		return value
	}
	return
}

// setRequestHead set head of request
func setRequestHead(params netutil.UriParams, r *http.Request) (head *vearchpb.RequestHead) {
	head = &vearchpb.RequestHead{}
	head.DbName = params.ByName(URLParamDbName)
	head.SpaceName = params.ByName(URLParamSpaceName)
	head.Params = netutil.GetUrlQuery(r)
	return
}

// handlerQueryDocByIds query byids
func (handler *DocumentHandler) handlerQueryDocByIds(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handlerQueryDocByIds", startTime)
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHead(params, r)

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	fieldsParam, ids, _, err := docSearchByIdsParse(r, space)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	args.PrimaryKeys = ids

	reply := handler.docService.getDocs(ctx, args)
	var queryFieldsParam map[string]string
	if fieldsParam != nil {
		queryFieldsParam = arrayToMap(fieldsParam)
	}
	if resultBytes, err := docGetResponse(handler.client, args, reply, queryFieldsParam); err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	} else {
		resp.SendJsonBytes(ctx, w, resultBytes)
		return ctx, true
	}
}

// handlerQueryDocByIdsFeature query by ids and feature
func (handler *DocumentHandler) handlerQueryDocByIdsFeature(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handlerQueryDocByIdsFeature", startTime)
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHead(params, r)

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	_, ids, reqBody, err := docSearchByIdsParse(r, space)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	args.PrimaryKeys = ids
	getDocStart := time.Now()
	reply := handler.docService.getDocs(ctx, args)
	getDocEnd := time.Now()
	if reply.Items == nil || len(reply.Items) == 0 {
		result, err := queryDocByIdsNoResult(getDocEnd.Sub(getDocStart))
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}
		resp.SendJsonBytes(ctx, w, result)
		return ctx, true
	}

	searchArgs := &vearchpb.SearchRequest{}
	searchArgs.Head = setRequestHead(params, r)

	err = docSearchByFeaturesParse(space, reqBody, searchArgs, reply.Items)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	serviceStart := time.Now()
	searchRes := handler.docService.search(ctx, searchArgs)
	serviceEnd := time.Now()
	serviceCost := serviceEnd.Sub(serviceStart)

	bs, err := ToContents(searchRes.Results, args.Head, serviceCost, space)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)
	endTime := time.Now()
	log.Debug("handlerQueryDocByIdsFeature total use :[%f] service use :[%f]",
		(endTime.Sub(startTime).Seconds())*1000, serviceCost.Seconds()*1000)
	return ctx, true
}

// handleBulkSearchDoc query byids
func (handler *DocumentHandler) handleBulkSearchDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleBulkSearchDoc", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	searchReqs, err := docBulkSearchParse(r, space, args.Head)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	if searchReqs == nil || len(searchReqs) == 0 {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "param is null")
		return ctx, false
	}

	serviceStart := time.Now()
	searchRes := handler.docService.bulkSearch(ctx, searchReqs)
	serviceCost := time.Now().Sub(serviceStart)

	bs, err := ToContents(searchRes.Results, args.Head, serviceCost, space)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)
	endTime := time.Now()
	log.Debug("handleBulkSearchDoc total use :[%f] service use :[%f]",
		(endTime.Sub(startTime).Seconds())*1000, serviceCost.Seconds()*1000)
	return ctx, true
}

// handleForceMerge build index for gpu
func (handler *DocumentHandler) handleForceMerge(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleForceMerge", startTime)
	args := &vearchpb.ForceMergeRequest{}
	args.Head = setRequestHead(params, r)

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "query Cache space null")
		return ctx, false
	}
	forceMergeResponse := handler.docService.forceMerge(ctx, args)
	shardsBytes, err := ForceMergeToContent(forceMergeResponse.Shards)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}

func (handler *DocumentHandler) handleDeleteByQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleDeleteByQuery", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)
	if args.Head.Params != nil {
		paramMap := args.Head.Params
		paramMap["queryOnlyId"] = "true"
		args.Head.Params = paramMap
	} else {
		paramMap := make(map[string]string)
		paramMap["queryOnlyId"] = "true"
		args.Head.Params = paramMap
	}

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "query Cache space null")
		return ctx, false
	}

	IDIsLong := idIsLong(space)
	if IDIsLong {
		args.Head.Params["idIsLong"] = "true"
	} else {
		args.Head.Params["idIsLong"] = "false"
	}

	err = docSearchParse(r, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	serviceStart := time.Now()
	delByQueryResp := handler.docService.deleteByQuery(ctx, args)
	serviceEnd := time.Now()
	serviceCost := serviceEnd.Sub(serviceStart)

	log.Debug("handleDeleteByQuery cost :%f", serviceCost)

	resp.SendJson(ctx, w, delByQueryResp)
	return ctx, true
}
