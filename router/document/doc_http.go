// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
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
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/monitor"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/router/document/resp"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/netutil"
	"github.com/vearch/vearch/util/uuid"
)

const (
	URLParamDbName      = "db_name"
	URLParamSpaceName   = "space_name"
	URLParamPartitionID = "partition_id"
	URLParamID          = "_id"
	URLParams           = "url_params"
	ReqsBody            = "req_body"
	SpaceEntity         = "space_entity"
	IDType              = "id_type"
	IDIsLong            = "IDIsLong"
	QueryIsOnlyID       = "QueryIsOnlyID"
	URLQueryTimeout     = "timeout"
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

	documentHandler.proxyMaster()
	// open router api
	if err := documentHandler.ExportInterfacesToServer(); err != nil {
		panic(err)
	}

	if err := documentHandler.ExportToServer(); err != nil {
		panic(err)
	}
}

func (handler *DocumentHandler) proxyMaster() error {
	// list/server
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/server", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/db", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/space", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/partition", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/router", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)

	// db handler
	handler.httpServer.HandlesMethods([]string{http.MethodPut}, "/db/_create", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodDelete}, fmt.Sprintf("/db/{%s}", URLParamDbName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/db/modify", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	// space handler
	handler.httpServer.HandlesMethods([]string{http.MethodPut}, fmt.Sprintf("/space/{%s}/_create", URLParamDbName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost, http.MethodDelete}, fmt.Sprintf("/space/{%s}/{%s}", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)

	// cluster handler
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/_cluster/health", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/_cluster/stats", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest}, nil)

	return nil
}

func (handler *DocumentHandler) handleMasterRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	reqBody, err := netutil.GetReqBody(r)

	if err != nil {
		log.Error("handleMasterRequest %v, req %+v", err, *r)
		return ctx, false
	}

	response, err := handler.client.Master().ProxyHTTPRequest(ctx, r.Method, r.RequestURI, string(reqBody))
	if err != nil {
		log.Error("handleMasterRequest %v, req %+v", err, *r)
		resp.SendJsonBytes(ctx, w, response)
		return ctx, false
	}
	resp.SendJsonBytes(ctx, w, response)
	return ctx, true
}

func (handler *DocumentHandler) ExportInterfacesToServer() error {
	// The data operation will be redefined as the following 2 type interfaces: document and index
	// document
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/document/upsert", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDocumentUpsert}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/document/query", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDocumentQuery}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/document/search", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDocumentSearch}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/document/delete", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDocumentDelete}, nil)

	// index
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/index/flush", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleIndexFlush}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/index/forcemerge", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleIndexForceMerge}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/index/rebuild", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleIndexRebuild}, nil)

	return nil
}

func (handler *DocumentHandler) ExportToServer() error {
	// routerInfo
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleRouterInfo}, nil)
	// list router
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/router", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleRouterIPs}, nil)
	// cacheInfo /$dbName/$spaceName
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/{%s}/{%s}", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.cacheInfo}, nil)

	// bulk: /$dbName/$spaceName/_bulk
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_bulk", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleBulk}, nil)

	// flush space: /$dbName/$spaceName/_flush
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_flush", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleFlush}, nil)

	// search doc: /$dbName/$spaceName/_search
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_search", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleSearchDoc}, nil)

	// msearch doc: /$dbName/$spaceName/_msearch
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_msearch", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMSearchDoc}, nil)

	// search doc: /$dbName/$spaceName/_msearch_ids
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_msearch_ids", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMSearchIdsDoc}, nil)

	// bulk: /$dbName/$spaceName/_query_byids
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_query_byids", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handlerQueryDocByIds}, nil)

	// bulk: /$dbName/$spaceName/_query_by_ids
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_query_by_ids", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handlerQueryDocByIds}, nil)

	// bulk: /$dbName/$spaceName/_query_byids_feture
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_query_byids_feature", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handlerQueryDocByIdsFeature}, nil)

	// bulk: /$dbName/$spaceName/_query_byids_feture
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_bulk_search", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleBulkSearchDoc}, nil)

	// delete: /$dbName/$spaceName/_delete_by_query
	handler.httpServer.HandlesMethods([]string{http.MethodDelete, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_delete_by_query", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDeleteByQuery}, nil)

	// forcemerge space: /$dbName/$spaceName/_forcemerge
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_forcemerge", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleForceMerge}, nil)

	// update doc: /$dbName/$spaceName/_log_collect
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodPut}, fmt.Sprintf("/{%s}/{%s}/_log_print_switch", URLParamDbName, URLParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleLogPrintSwitch}, nil)

	// get doc: /$dbName/$spaceName/$docId
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/{%s}/{%s}/{%s}", URLParamDbName, URLParamSpaceName, URLParamID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleGetDoc}, nil)

	// get doc: /$dbName/$spaceName/$partitionId/$docId
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/{%s}/{%s}/{%s}/{%s}", URLParamDbName, URLParamSpaceName, URLParamPartitionID, URLParamID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleGetDocByPartition}, nil)

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
	messageID := uuid.FlakeUUID()
	ctx = context.WithValue(ctx, entity.MessageID, messageID)
	return ctx, true
}

func (handler *DocumentHandler) handleAuth(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	if config.Conf().Global.SkipAuth {
		return ctx, true
	}
	headerData := r.Header.Get("Authorization")
	username, password, err := util.AuthDecrypt(headerData)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	if username != "root" || password != config.Conf().Global.Signkey {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "Authorization failed, wrong user or password")
		return ctx, false
	}
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

func (handler *DocumentHandler) handleRouterIPs(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	ips, err := handler.client.Master().QueryRouter(ctx, config.Conf().Global.Name)
	if err != nil {
		log.Errorf("get router ips failed, err: [%s]", err.Error())
		resp.SendError(ctx, w, 500, err.Error())
		return ctx, false
	}

	resp.SendText(ctx, w, strings.Join(ips, ","))
	return ctx, true
}

func (handler *DocumentHandler) cacheInfo(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(URLParamDbName)
	spaceName := params.ByName(URLParamSpaceName)
	if space, err := handler.client.Master().Cache().SpaceByCache(context.Background(), dbName, spaceName); err != nil {
		resp.SendErrorRootCause(ctx, w, 404, err.Error(), err.Error())
	} else {
		resp.SendJson(ctx, w, space)
	}
	return ctx, true
}

func (handler *DocumentHandler) handleGetDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	operateName := "handleGetDoc"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHead(params, r)
	args.PrimaryKeys = strings.Split(params.ByName(URLParamID), ",")
	reply := handler.docService.getDocs(ctx, args)
	if resultBytes, err := docGetResponse(handler.client, args, reply, nil, false); err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	} else {
		resp.SendJsonBytes(ctx, w, resultBytes)
		return ctx, true
	}
}

func (handler *DocumentHandler) handleGetDocByPartition(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	operateName := "handleGetDocByPartition"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHead(params, r)
	args.PrimaryKeys = strings.Split(params.ByName(URLParamID), ",")
	partitionId := params.ByName(URLParamPartitionID)
	reply := handler.docService.getDocsByPartition(ctx, args, partitionId)
	if resultBytes, err := docGetResponse(handler.client, args, reply, nil, false); err != nil {
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
	if space == nil || err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}

	pkey := params.ByName(URLParamID)
	err = docParse(ctx, handler, r, space, args, pkey)
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
	startTime := time.Now()
	operateName := "handleBulk"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.BulkRequest{}
	args.Head = setRequestHead(params, r)
	space, err := handler.client.Space(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil || err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "dbName or spaceName param not build db or space")
		return ctx, true
	}
	if space.SpaceProperties == nil {
		spaceProperties, _ := entity.UnmarshalPropertyJSON(space.Properties)
		space.SpaceProperties = spaceProperties
	}

	err = docBulkParse(ctx, handler, r, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	reply := handler.docService.bulk(ctx, args)
	resultBytes, err := docBulkResponses(handler.client, args, reply)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, resultBytes)
	return ctx, true
}

// handleFlush for flush
func (handler *DocumentHandler) handleFlush(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleFlush", startTime)
	args := &vearchpb.FlushRequest{}
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
	flushResponse := handler.docService.flush(ctx, args)
	shardsBytes, err := FlushToContent(flushResponse.Shards)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}

// handleSearchDoc for search by param
func (handler *DocumentHandler) handleSearchDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	operateName := "handleSearchDoc"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)
	if args.Head.Params == nil {
		params := make(map[string]string)
		args.Head.Params = params
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
	searchResp := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)

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
	operateName := "handleMSearchDoc"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)
	if args.Head.Params == nil {
		params := make(map[string]string)
		args.Head.Params = params
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
	paramStart := time.Now()
	err = docSearchParse(r, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	serviceStart := time.Now()
	searchRes := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)
	contentStartTime := time.Now()
	log.Info("handleMSearchDoc service cost:[%f]", serviceCost.Seconds()*1000)
	bs, err := ToContents(searchRes.Results, args.Head, serviceCost, space)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	endTime := time.Now()
	resp.SendJsonBytes(ctx, w, bs)
	log.Debug("msearch total use :[%f] service use :[%f]",
		(endTime.Sub(startTime).Seconds())*1000, serviceCost.Seconds()*1000)

	if config.LogInfoPrintSwitch {
		serializeCostTime := searchRes.Head.Params["serializeCostTime"]
		reqBodyCostTime := args.Head.Params["reqBodyCostTime"]
		gammaCostTime := searchRes.Head.Params["gammaCostTime"]
		mergeCostTime := searchRes.Head.Params["mergeCostTime"]
		pidCacheTime := searchRes.Head.Params["pidCacheTime"]
		nodeIdTime := searchRes.Head.Params["nodeIdTime"]
		rpcClientTime := searchRes.Head.Params["rpcClientTime"]
		rpcCostTime := searchRes.Head.Params["rpcCostTime"]
		deSerializeCostTime := searchRes.Head.Params["deSerializeCostTime"]
		fieldParsingTime := searchRes.Head.Params["fieldParsingTime"]
		sortCostTime := searchRes.Head.Params["sortCostTime"]
		normalCostTime := searchRes.Head.Params["normalCostTime"]
		partitionCostTime := searchRes.Head.Params["partitionCostTime"]
		executeCostTime := searchRes.Head.Params["executeCostTime"]
		normalTime := searchRes.Head.Params["normalTime"]
		rpcBeforeTime := searchRes.Head.Params["rpcBeforeTime"]
		rpcTotalTime := searchRes.Head.Params["rpcTotalTime"]

		msg := fmt.Sprintf("getspace [%f]ms "+
			"search param [%f]ms "+
			"reqbody [%s]ms "+
			"pidCache [%s]ms "+
			"nodeId [%s]ms "+
			"rpcClient [%s]ms "+
			"normal [%s]ms "+
			"rpcBefore [%s]ms "+
			"serialize [%s]ms "+
			"gamma [%s]ms "+
			"rpc [%s]ms "+
			"deSerialize [%s]ms "+
			"sort [%s]ms "+
			"fieldparsing [%s]ms "+
			"merge [%s]ms, "+
			"normal [%s]ms "+
			"rpcTotal [%s]ms "+
			"partition [%s]ms "+
			"execute [%s]ms "+
			"service [%f]ms "+
			"respv [%f]ms "+
			"total [%f]ms ",
			(paramStart.Sub(startTime).Seconds())*1000,
			(serviceStart.Sub(paramStart).Seconds())*1000,
			reqBodyCostTime,
			pidCacheTime,
			nodeIdTime,
			rpcClientTime,
			normalTime,
			rpcBeforeTime,
			serializeCostTime,
			gammaCostTime,
			rpcCostTime,
			deSerializeCostTime,
			sortCostTime,
			fieldParsingTime,
			mergeCostTime,
			normalCostTime,
			rpcTotalTime,
			partitionCostTime,
			executeCostTime, serviceCost.Seconds()*1000,
			(endTime.Sub(contentStartTime).Seconds())*1000,
			(endTime.Sub(startTime).Seconds())*1000)
		log.Info(msg)
	}
	return ctx, true
}

// handleMSearchIdsDoc for search by param
func (handler *DocumentHandler) handleMSearchIdsDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	operateName := "handleMSearchIdsDoc"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)
	if args.Head.Params != nil {
		paramMap := args.Head.Params
		paramMap["queryOnlyId"] = "true"
		args.Head.Params = paramMap
	} else {
		params := make(map[string]string)
		args.Head.Params = params
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

// setRequestHead set head of request
func setRequestHead(params netutil.UriParams, r *http.Request) (head *vearchpb.RequestHead) {
	head = &vearchpb.RequestHead{}
	head.DbName = params.ByName(URLParamDbName)
	head.SpaceName = params.ByName(URLParamSpaceName)
	head.Params = netutil.GetUrlQuery(r)
	if timeout, ok := head.Params["timeout"]; ok {
		var err error
		if head.TimeOutMs, err = strconv.ParseInt(timeout, 10, 64); err != nil {
			log.Warnf("timeout[%s] param parse to int failed, err: %s", timeout, err.Error())
		}
	}
	return
}

// setRequestHeadParams set head params of request
func setRequestHeadParams(params netutil.UriParams, r *http.Request) (head *vearchpb.RequestHead) {
	head = &vearchpb.RequestHead{}
	head.Params = netutil.GetUrlQuery(r)
	if len(head.Params) == 0 {
		return
	}

	if timeout, ok := head.Params["timeout"]; ok {
		var err error
		if head.TimeOutMs, err = strconv.ParseInt(timeout, 10, 64); err != nil {
			log.Warnf("timeout[%s] param parse to int failed, err: %s", timeout, err.Error())
		}
	}

	return head
}

// handlerQueryDocByIds query byids
func (handler *DocumentHandler) handlerQueryDocByIds(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	operateName := "handlerQueryDocByIds"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHead(params, r)
	if args.Head.Params == nil {
		params := make(map[string]string)
		args.Head.Params = params
	}
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
	if resultBytes, err := docGetResponse(handler.client, args, reply, queryFieldsParam, true); err != nil {
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
	operateName := "handlerQueryDocByIdsFeature"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHead(params, r)
	if args.Head.Params == nil {
		params := make(map[string]string)
		args.Head.Params = params
	}
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

	// TODO: If ids are not found, the result should be 0 instead of empty.
	// filter error items
	if reply != nil && reply.Items != nil && len(reply.Items) != 0 {
		tmpItems := make([]*vearchpb.Item, 0)
		for _, i := range reply.Items {
			if i == nil || (i.Err != nil && i.Err.Code != vearchpb.ErrorEnum_SUCCESS) {
				continue
			}
			tmpItems = append(tmpItems, i)
		}
		reply.Items = tmpItems
	}

	if reply == nil || reply.Items == nil || len(reply.Items) == 0 {
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
	if searchArgs.Head.Params == nil {
		params := make(map[string]string)
		searchArgs.Head.Params = params
	}
	err = docSearchByFeaturesParse(space, reqBody, searchArgs, reply.Items, request.QuerySum)
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
	operateName := "handleBulkSearchDoc"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHead(params, r)
	if args.Head.Params == nil {
		params := make(map[string]string)
		args.Head.Params = params
	}
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

	if len(searchReqs) == 0 {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "param is null")
		return ctx, false
	}

	serviceStart := time.Now()
	searchRes := handler.docService.bulkSearch(ctx, searchReqs)
	serviceCost := time.Since(serviceStart)

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

	if args.TermFilters == nil && args.RangeFilters == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "query filter is null")
		return ctx, false
	}
	serviceStart := time.Now()
	delByQueryResp := handler.docService.deleteByQuery(ctx, args)
	serviceEnd := time.Now()
	serviceCost := serviceEnd.Sub(serviceStart)

	log.Debug("handleDeleteByQuery cost :%f", serviceCost)
	shardsBytes, err := deleteByQueryResult(delByQueryResp)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}

// handleLogPrintSwitch log print switch
func (handler *DocumentHandler) handleLogPrintSwitch(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleLogPrintSwitch", startTime)
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHead(params, r)
	printSwitch, err := doLogPrintSwitchParse(r)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	config.LogInfoPrintSwitch = printSwitch
	if resultBytes, err := docPrintLogSwitchResponse(config.LogInfoPrintSwitch); err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	} else {
		resp.SendJsonBytes(ctx, w, resultBytes)
		return ctx, true
	}
}

func (handler *DocumentHandler) handleDocumentUpsert(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	operateName := "handleDocumentUpsert"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.BulkRequest{}
	args.Head = setRequestHeadParams(params, r)
	docRequest, dbName, spaceName, err := documentHeadParse(r)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("documentHeadParse error: %v", err))
		return ctx, true
	}

	args.Head.DbName = dbName
	args.Head.SpaceName = spaceName
	space, err := handler.client.Space(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("dbName:%s or spaceName:%s param not build db or space", args.Head.DbName, args.Head.SpaceName))
		return ctx, false
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	err = documentParse(ctx, handler, r, docRequest, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	reply := handler.docService.bulk(ctx, args)
	resultBytes, err := documentUpsertResponse(handler.client, args, reply)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, resultBytes)
	return ctx, true
}

func (handler *DocumentHandler) handleDocumentQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	operateName := "handleDocumentQuery"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadParams(params, r)
	if args.Head.Params == nil {
		params := make(map[string]string)
		args.Head.Params = params
	}

	searchDoc, fields, documentIds, partitionId, err := documentRequestParse(r, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("dbName:%s or spaceName:%s param not build db or space", args.Head.DbName, args.Head.SpaceName))
		return ctx, false
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	IDIsLong := idIsLong(space)
	if IDIsLong {
		args.Head.Params["idIsLong"] = "true"
	} else {
		args.Head.Params["idIsLong"] = "false"
	}

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	if args.VecFields != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/query query condition must be one of the [document_ids, filter]")
		return ctx, false
	}

	if len(documentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/query query condition must be one of the [document_ids, filter]")
			return ctx, false
		}
		if len(documentIds) >= 500 {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/query length of document_ids in query condition above 500")
			return ctx, false
		}
		args := &vearchpb.GetRequest{}
		args.Head = setRequestHeadParams(params, r)
		args.Head.DbName = searchDoc.DbName
		args.Head.SpaceName = searchDoc.SpaceName
		args.PrimaryKeys = documentIds

		var queryFieldsParam map[string]string
		if fields != nil {
			queryFieldsParam = arrayToMap(fields)
		}

		reply := &vearchpb.GetResponse{}
		if partitionId != "" {
			reply = handler.docService.getDocsByPartition(ctx, args, partitionId)
		} else {
			reply = handler.docService.getDocs(ctx, args)
		}

		if resultBytes, err := documentGetResponse(handler.client, args, reply, queryFieldsParam, searchDoc.VectorValue); err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		} else {
			resp.SendJsonBytes(ctx, w, resultBytes)
			return ctx, true
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/query query condition must be one of the [document_ids, filter]")
			return ctx, false
		}
	}
	serviceStart := time.Now()
	searchResp := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)

	var bs []byte
	if searchResp.Results == nil || len(searchResp.Results) == 0 {
		bs, err = documentSearchResponse(nil, searchResp.Head, serviceCost, space, request.QueryResponse)
	} else {
		bs, err = documentSearchResponse(searchResp.Results, searchResp.Head, serviceCost, space, request.QueryResponse)
	}

	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)
	endTime := time.Now()
	log.Debug("handleDocumentQuery total use :[%f] service use :[%f]",
		(endTime.Sub(startTime).Seconds())*1000, serviceCost.Seconds()*1000)
	return ctx, true
}

func (handler *DocumentHandler) handleDocumentSearch(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	operateName := "handleDocumentSearch"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(ctx, operateName)
	defer span.Finish()
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadParams(params, r)
	if args.Head.Params == nil {
		params := make(map[string]string)
		args.Head.Params = params
	}

	searchDoc, _, documentIds, _, err := documentRequestParse(r, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("dbName:%s or spaceName:%s param not build db or space", args.Head.DbName, args.Head.SpaceName))
		return ctx, false
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	IDIsLong := idIsLong(space)
	if IDIsLong {
		args.Head.Params["idIsLong"] = "true"
	} else {
		args.Head.Params["idIsLong"] = "false"
	}

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	if len(documentIds) != 0 {
		if args.VecFields != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/search search condition must be one of the [document_ids, vector]")
			return ctx, false
		}
		if len(documentIds) >= 100 {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/search length of document_ids in search condition above 100")
			return ctx, false
		}
		getArgs := &vearchpb.GetRequest{}
		getArgs.Head = setRequestHeadParams(params, r)
		getArgs.Head.DbName = searchDoc.DbName
		getArgs.Head.SpaceName = searchDoc.SpaceName
		getArgs.PrimaryKeys = documentIds

		getDocStart := time.Now()
		reply := handler.docService.getDocs(ctx, getArgs)
		getDocEnd := time.Now()

		if reply == nil || reply.Items == nil || len(reply.Items) == 0 {
			result, err := documentSearchResponse(nil, reply.Head, getDocEnd.Sub(getDocStart), space, request.SearchResponse)
			if err != nil {
				resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
				return ctx, true
			}
			resp.SendJsonBytes(ctx, w, result)
			return ctx, true
		}

		// filter error items
		if reply != nil && reply.Items != nil && len(reply.Items) != 0 {
			tmpItems := make([]*vearchpb.Item, 0)
			for _, i := range reply.Items {
				if i == nil || (i.Err != nil && i.Err.Code != vearchpb.ErrorEnum_SUCCESS) {
					continue
				}
				tmpItems = append(tmpItems, i)
			}
			reply.Items = tmpItems
		}

		// TODO: If ids are not found, the result should be [].
		err = documentRequestVectorParse(space, searchDoc, args, reply.Items, request.QueryVector)
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, false
		}
	} else {
		if args.VecFields == nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/search search condition must be one of the [document_ids, vector]")
			return ctx, false
		}
	}
	serviceStart := time.Now()
	searchResp := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)

	var bs []byte
	if searchResp.Results == nil || len(searchResp.Results) == 0 {
		bs, err = documentSearchResponse(nil, searchResp.Head, serviceCost, space, request.SearchResponse)
	} else {
		bs, err = documentSearchResponse(searchResp.Results, searchResp.Head, serviceCost, space, request.SearchResponse)
	}

	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)
	log.Debug("handleDocumentSearch total use :[%d] service use :[%d]", time.Since(startTime).Milliseconds(), serviceCost.Milliseconds())
	return ctx, true
}

func (handler *DocumentHandler) handleDocumentDelete(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleDocumentDelete", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadParams(params, r)
	if args.Head.Params != nil {
		paramMap := args.Head.Params
		paramMap["queryOnlyId"] = "true"
		args.Head.Params = paramMap
	} else {
		paramMap := make(map[string]string)
		paramMap["queryOnlyId"] = "true"
		args.Head.Params = paramMap
	}

	searchDoc, _, documentIds, _, err := documentRequestParse(r, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("dbName:%s or spaceName:%s param not build db or space", args.Head.DbName, args.Head.SpaceName))
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	IDIsLong := idIsLong(space)
	if IDIsLong {
		args.Head.Params["idIsLong"] = "true"
	} else {
		args.Head.Params["idIsLong"] = "false"
	}

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	if args.VecFields != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/delete query condition must be one of the [document_ids, filter]")
		return ctx, false
	}

	if len(documentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/delete query condition must be one of the [document_ids, filter]")
			return ctx, false
		}
		if len(documentIds) >= 500 {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/delete length of document_ids in query condition above 500")
			return ctx, false
		}
		args := &vearchpb.DeleteRequest{}
		args.Head = setRequestHeadParams(params, r)
		args.Head.DbName = searchDoc.DbName
		args.Head.SpaceName = searchDoc.SpaceName
		args.PrimaryKeys = documentIds
		var resultIds []string
		reply := handler.docService.deleteDocs(ctx, args)
		if resultBytes, err := documentDeleteResponse(reply.Items, reply.Head, resultIds); err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		} else {
			resp.SendJsonBytes(ctx, w, resultBytes)
			return ctx, true
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "document/delete query condition must be one of the [document_ids, filter]")
			return ctx, false
		}
	}
	serviceStart := time.Now()
	delByQueryResp := handler.docService.deleteByQuery(ctx, args)
	serviceCost := time.Since(serviceStart)

	log.Debug("handleDocumentDelete cost :%f", serviceCost)
	shardsBytes, err := deleteByQueryResult(delByQueryResp)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}

// handleIndexFlush
func (handler *DocumentHandler) handleIndexFlush(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexFlush", startTime)

	args := &vearchpb.FlushRequest{}
	args.Head = setRequestHeadParams(params, r)

	indexRequest, err := IndexRequestParse(r)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("dbName:%s or spaceName:%s param not build db or space", args.Head.DbName, args.Head.SpaceName))
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}
	flushResponse := handler.docService.flush(ctx, args)
	shardsBytes, err := IndexResponseToContent(flushResponse.Shards)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}

// handleIndexForceMerge build index for gpu
func (handler *DocumentHandler) handleIndexForceMerge(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexForceMerge", startTime)
	args := &vearchpb.ForceMergeRequest{}
	args.Head = setRequestHeadParams(params, r)

	indexRequest, err := IndexRequestParse(r)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("dbName:%s or spaceName:%s param not build db or space", args.Head.DbName, args.Head.SpaceName))
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
	}

	forceMergeResponse := handler.docService.forceMerge(ctx, args)
	shardsBytes, err := IndexResponseToContent(forceMergeResponse.Shards)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}

// handleIndexRebuild rebuild index
func (handler *DocumentHandler) handleIndexRebuild(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexRebuild", startTime)
	args := &vearchpb.IndexRequest{}
	args.Head = setRequestHeadParams(params, r)

	indexRequest, err := IndexRequestParse(r)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName
	if indexRequest.DropBeforeRebuild {
		args.DropBeforeRebuild = 1
	} else {
		args.DropBeforeRebuild = 0
	}
	args.LimitCpu = int64(indexRequest.LimitCPU)
	args.Describe = int64(indexRequest.Describe)

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("dbName:%s or spaceName:%s param not build db or space", args.Head.DbName, args.Head.SpaceName))
		return ctx, true
	}
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
	}

	indexResponse := handler.docService.rebuildIndex(ctx, args)
	shardsBytes, err := IndexResponseToContent(indexResponse.Shards)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}
