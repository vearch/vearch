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
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/opentracing/opentracing-go"
	"github.com/vearch/vearch/internal/client"
	"github.com/vearch/vearch/internal/config"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/entity/request"
	"github.com/vearch/vearch/internal/monitor"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/router/document/resp"
	"github.com/vearch/vearch/internal/util"
	"github.com/vearch/vearch/internal/util/ginutil"
	"github.com/vearch/vearch/internal/util/log"
	"github.com/vearch/vearch/internal/util/netutil"
	"github.com/vearch/vearch/internal/util/uuid"
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
	httpServer *gin.Engine
	docService docService
	client     *client.Client
}

func ExportDocumentHandler(httpServer *gin.Engine, client *client.Client) {
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
	// list/*
	handler.httpServer.Handle(http.MethodGet, "/list/server", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/list/db", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/list/space", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/list/partition", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/list/router", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// db handler
	handler.httpServer.Handle(http.MethodPut, "/db/_create", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/db/:%s", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodDelete, fmt.Sprintf("/db/:%s", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodPost, "/db/modify", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// space handler
	handler.httpServer.Handle(http.MethodPut, fmt.Sprintf("/space/:%s/_create", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/space/:%s/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/space/:%s/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodDelete, fmt.Sprintf("/space/:%s/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// new space handler
	// handler.httpServer.Handle(http.MethodPost, "/space/create", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/space/describe", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// handler.httpServer.Handle(http.MethodPost, "/space/delete", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// handler.httpServer.Handle(http.MethodPost, "/space/update", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)

	// cluster handler
	handler.httpServer.Handle(http.MethodGet, "/_cluster/health", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/_cluster/stats", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)

	return nil
}

func (handler *DocumentHandler) handleMasterRequest(c *gin.Context) {
	method := c.Request.Method
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, "Error reading body")
		return
	}

	response, err := handler.client.Master().ProxyHTTPRequest(method, c.Request.RequestURI, string(bodyBytes))
	if err != nil {
		log.Error("handleMasterRequest %v, response %s", err, string(response))
		ginutil.NewAutoMehtodName(c).SetHttpStatus(http.StatusInternalServerError).SendJsonBytes(response)
		return
	}
	resp.SendJsonBytes(c, response)
}

func (handler *DocumentHandler) ExportInterfacesToServer() error {
	// The data operation will be redefined as the following 2 type interfaces: document and index
	// document
	handler.httpServer.Handle(http.MethodPost, "/document/upsert", handler.handleTimeout, handler.handleAuth, handler.handleDocumentUpsert)
	handler.httpServer.Handle(http.MethodPost, "/document/query", handler.handleTimeout, handler.handleAuth, handler.handleDocumentQuery)
	handler.httpServer.Handle(http.MethodPost, "/document/search", handler.handleTimeout, handler.handleAuth, handler.handleDocumentSearch)
	handler.httpServer.Handle(http.MethodPost, "/document/delete", handler.handleTimeout, handler.handleAuth, handler.handleDocumentDelete)

	// index
	handler.httpServer.Handle(http.MethodPost, "/index/flush", handler.handleTimeout, handler.handleAuth, handler.handleIndexFlush)
	handler.httpServer.Handle(http.MethodPost, "/index/forcemerge", handler.handleTimeout, handler.handleAuth, handler.handleIndexForceMerge)
	handler.httpServer.Handle(http.MethodPost, "/index/rebuild", handler.handleTimeout, handler.handleAuth, handler.handleIndexRebuild)

	return nil
}

func (handler *DocumentHandler) ExportToServer() error {
	// routerInfo
	handler.httpServer.Handle(http.MethodGet, "/", handler.handleTimeout, handler.handleAuth, handler.handleRouterInfo)
	// list router
	// handler.httpServer.Handle(http.MethodGet, "/list/router", handler.handleTimeout, handler.handleAuth, handler.handleRouterIPs)
	// cacheInfo /$dbName/$spaceName
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/:%s/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.cacheInfo)

	// bulk: /$dbName/$spaceName/_bulk
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_bulk", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleBulk)

	// flush space: /$dbName/$spaceName/_flush
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_flush", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleFlush)

	// search doc: /$dbName/$spaceName/_search
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_search", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleSearchDoc)

	// msearch doc: /$dbName/$spaceName/_msearch
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_msearch", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMSearchDoc)

	// bulk: /$dbName/$spaceName/_query_byids
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_query_byids", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handlerQueryDocByIds)

	// bulk: /$dbName/$spaceName/_query_byids_feture
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_bulk_search", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleBulkSearchDoc, nil)

	// delete: /$dbName/$spaceName/_delete_by_query
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_delete_by_query", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleDeleteByQuery)

	// forcemerge space: /$dbName/$spaceName/_forcemerge
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_forcemerge", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleForceMerge)

	// update doc: /$dbName/$spaceName/_log_collect
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_log_print_switch", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleLogPrintSwitch)

	// get doc: /$dbName/$spaceName/$partitionId/$docId
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/:%s/:%s/:%s/:%s", URLParamDbName, URLParamSpaceName, URLParamPartitionID, URLParamID), handler.handleTimeout, handler.handleAuth, handler.handleGetDocByPartition)

	// delete doc: /$dbName/$spaceName/$docId
	handler.httpServer.Handle(http.MethodDelete, fmt.Sprintf("/:%s/:%s/:%s", URLParamDbName, URLParamSpaceName, URLParamID), handler.handleTimeout, handler.handleAuth, handler.handleDeleteDoc)

	// create doc: /$dbName/$spaceName/$docId/_create
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/:%s/_create", URLParamDbName, URLParamSpaceName, URLParamID), handler.handleTimeout, handler.handleAuth, handler.handleUpdateDoc)
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/:%s", URLParamDbName, URLParamSpaceName, URLParamID), handler.handleTimeout, handler.handleAuth, handler.handleUpdateDoc)
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleUpdateDoc)

	return nil
}

func (handler *DocumentHandler) handleTimeout(c *gin.Context) {
	messageID := uuid.FlakeUUID()
	c.Set(entity.MessageID, messageID)
}

func (handler *DocumentHandler) handleAuth(c *gin.Context) {
	if config.Conf().Global.SkipAuth {
		return
	}
	headerData := c.GetHeader("Authorization")
	username, password, err := util.AuthDecrypt(headerData)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	if username != "root" || password != config.Conf().Global.Signkey {
		resp.SendError(c, http.StatusBadRequest, "authorization failed, wrong user or password")
		return
	}
}

func (handler *DocumentHandler) handleRouterInfo(c *gin.Context) {
	versionLayer := make(map[string]interface{})
	versionLayer["build_version"] = config.GetBuildVersion()
	versionLayer["build_time"] = config.GetBuildTime()
	versionLayer["commit_id"] = config.GetCommitID()

	layer := make(map[string]interface{})
	layer["cluster_name"] = config.Conf().Global.Name
	layer["version"] = versionLayer

	resp.SendJson(c, layer)
}

func (handler *DocumentHandler) handleRouterIPs(c *gin.Context) {
	ctx := c.Request.Context()
	ips, err := handler.client.Master().QueryRouter(ctx, config.Conf().Global.Name)
	if err != nil {
		log.Errorf("get router ips failed, err: [%s]", err.Error())
		resp.SendError(c, http.StatusInternalServerError, err.Error())
		return
	}

	c.String(http.StatusOK, strings.Join(ips, ","))
}

func (handler *DocumentHandler) cacheInfo(c *gin.Context) {
	dbName := c.Param(URLParamDbName)
	spaceName := c.Param(URLParamSpaceName)
	if space, err := handler.client.Master().Cache().SpaceByCache(context.Background(), dbName, spaceName); err != nil {
		resp.SendError(c, http.StatusNotFound, err.Error())
	} else {
		resp.SendJson(c, space)
	}
}

func (handler *DocumentHandler) handleGetDocByPartition(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetDocByPartition"
	defer monitor.Profiler(operateName, startTime)

	span, _ := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.GetRequest{}
	args.Head = setRequestHeadFromGin(c)
	args.PrimaryKeys = strings.Split(c.Param(URLParamID), ",")
	partitionId := c.Param(URLParamPartitionID)

	reply := handler.docService.getDocsByPartition(c.Request.Context(), args, partitionId, false)
	if resultBytes, err := docGetResponse(handler.client, args, reply, nil, false); err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	} else {
		resp.SendJsonBytes(c, resultBytes)
		return
	}
}

func (handler *DocumentHandler) handleDeleteDoc(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleDeleteDoc", startTime)

	args := &vearchpb.DeleteRequest{}
	args.Head = setRequestHeadFromGin(c)
	args.PrimaryKeys = strings.Split(c.Param(URLParamID), ",")

	reply := handler.docService.deleteDocs(c.Request.Context(), args)
	if resultBytes, err := docDeleteResponses(handler.client, args, reply); err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	} else {
		resp.SendJsonBytes(c, resultBytes)
		return
	}
}

func (handler *DocumentHandler) handleUpdateDoc(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleUpdateDoc", startTime)

	args := &vearchpb.UpdateRequest{}
	args.Head = setRequestHeadFromGin(c)

	dbName := args.Head.DbName
	spaceName := args.Head.SpaceName

	space, err := handler.client.Space(c.Request.Context(), dbName, spaceName)
	if space == nil || err != nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}

	pkey := c.Param(URLParamID)
	err = docParse(c.Request.Context(), handler, c.Request, space, args, pkey)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	args.Doc.PKey = pkey
	reply := handler.docService.updateDoc(c.Request.Context(), args)
	if resultBytes, err := docUpdateResponses(handler.client, args, reply); err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	} else {
		resp.SendJsonBytes(c, resultBytes)
		return
	}
}

// handleBulk For add documents by batch
func (handler *DocumentHandler) handleBulk(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleBulk"
	defer monitor.Profiler(operateName, startTime)

	span, ctx := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.BulkRequest{}
	args.Head = setRequestHeadFromGin(c)
	space, err := handler.client.Space(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil || err != nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}
	if space.SpaceProperties == nil {
		spaceProperties, _ := entity.UnmarshalPropertyJSON(space.Properties)
		space.SpaceProperties = spaceProperties
	}

	err = docBulkParse(ctx, handler, c.Request, space, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	reply := handler.docService.bulk(ctx, args)
	resultBytes, err := docBulkResponses(handler.client, args, reply)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, resultBytes)
}

// handleFlush for flush
func (handler *DocumentHandler) handleFlush(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleFlush", startTime)

	args := &vearchpb.FlushRequest{
		Head: setRequestHeadFromGin(c),
	}

	ctx := c.Request.Context()

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, "query Cache space null")
		return
	}

	flushResponse := handler.docService.flush(ctx, args)
	shardsBytes, err := FlushToContent(flushResponse.Shards)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, shardsBytes)
}

// handleSearchDoc for search by param
func (handler *DocumentHandler) handleSearchDoc(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleSearchDoc"
	defer monitor.Profiler(operateName, startTime)

	span, ctx := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.SearchRequest{
		Head: setRequestHeadFromGin(c),
	}

	if args.Head.Params == nil {
		args.Head.Params = make(map[string]string)
	}

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, "query Cache space null")
		return
	}

	err = docSearchParse(c.Request, space, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	serviceStart := time.Now()
	searchResp := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)

	var bs []byte
	if searchResp.Results == nil || len(searchResp.Results) == 0 {
		searchStatus := vearchpb.SearchStatus{Failed: 0, Successful: 0, Total: 0}
		bs, err = SearchNullToContent(searchStatus, serviceCost)
	} else {
		bs, err = ToContentBytes(searchResp.Results[0], args.Head, serviceCost, space)
	}

	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, bs)

	log.Debug("search total use :[%f] service use :[%f]",
		(time.Since(startTime).Seconds())*1000, serviceCost.Seconds()*1000)
}

// handleMSearchDoc for search by param
func (handler *DocumentHandler) handleMSearchDoc(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleMSearchDoc"
	defer monitor.Profiler(operateName, startTime)

	span, ctx := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.SearchRequest{
		Head: setRequestHeadFromGin(c),
	}

	if args.Head.Params == nil {
		args.Head.Params = make(map[string]string)
	}

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, "query Cache space null")
		return
	}

	paramStart := time.Now()
	err = docSearchParse(c.Request, space, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	serviceStart := time.Now()
	searchRes := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)

	log.Info("handleMSearchDoc service cost:[%f]", serviceCost.Seconds()*1000)

	contentStartTime := time.Now()
	bs, err := ToContents(searchRes.Results, args.Head, serviceCost, space)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, bs)

	log.Debug("msearch total use :[%f] service use :[%f]",
		(time.Since(startTime).Seconds())*1000, serviceCost.Seconds()*1000)

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
			(time.Since(contentStartTime).Seconds())*1000,
			(time.Since(startTime).Seconds())*1000)
		log.Info(msg)
	}
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

func setRequestHeadFromGin(c *gin.Context) *vearchpb.RequestHead {
	head := &vearchpb.RequestHead{
		DbName:    c.Param(URLParamDbName),
		SpaceName: c.Param(URLParamSpaceName),
		Params:    make(map[string]string),
	}

	for k, v := range c.Request.URL.Query() {
		if len(v) > 0 {
			head.Params[k] = v[0]
		}
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
func (handler *DocumentHandler) handlerQueryDocByIds(c *gin.Context) {
	startTime := time.Now()
	operateName := "handlerQueryDocByIds"
	defer monitor.Profiler(operateName, startTime)

	span, ctx := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.GetRequest{
		Head: setRequestHeadFromGin(c),
	}

	if args.Head.Params == nil {
		args.Head.Params = make(map[string]string)
	}

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	fieldsParam, ids, _, err := docSearchByIdsParse(c.Request, space)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	args.PrimaryKeys = ids

	reply := handler.docService.getDocs(ctx, args)
	var queryFieldsParam map[string]string
	if fieldsParam != nil {
		queryFieldsParam = arrayToMap(fieldsParam)
	}
	if resultBytes, err := docGetResponse(handler.client, args, reply, queryFieldsParam, true); err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	} else {
		resp.SendJsonBytes(c, resultBytes)
	}
}

// handleBulkSearchDoc query byids
func (handler *DocumentHandler) handleBulkSearchDoc(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleBulkSearchDoc"
	defer monitor.Profiler(operateName, startTime)

	span, ctx := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.SearchRequest{
		Head: setRequestHeadFromGin(c),
	}

	if args.Head.Params == nil {
		args.Head.Params = make(map[string]string)
	}

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	searchReqs, err := docBulkSearchParse(c.Request, space, args.Head)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	if len(searchReqs) == 0 {
		resp.SendError(c, http.StatusBadRequest, "param is null")
		return
	}

	serviceStart := time.Now()
	searchRes := handler.docService.bulkSearch(ctx, searchReqs)
	serviceCost := time.Since(serviceStart)

	bs, err := ToContents(searchRes.Results, args.Head, serviceCost, space)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, bs)

	log.Debug("handleBulkSearchDoc total use :[%f] service use :[%f]",
		(time.Since(startTime).Seconds())*1000, serviceCost.Seconds()*1000)
}

// handleForceMerge build index for gpu
func (handler *DocumentHandler) handleForceMerge(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleForceMerge", startTime)
	args := &vearchpb.ForceMergeRequest{}
	args.Head = setRequestHeadFromGin(c)

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, "query Cache space null")
		return
	}

	forceMergeResponse := handler.docService.forceMerge(c.Request.Context(), args)
	shardsBytes, err := ForceMergeToContent(forceMergeResponse.Shards)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, shardsBytes)
}

func (handler *DocumentHandler) handleDeleteByQuery(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleDeleteByQuery", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadFromGin(c)

	if args.Head.Params == nil {
		args.Head.Params = make(map[string]string)
	}
	args.Head.Params["queryOnlyId"] = "true"

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, "dbName or spaceName param not build db or space")
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, "query Cache space null")
		return
	}

	IDIsLong := idIsLong(space)
	args.Head.Params["idIsLong"] = strconv.FormatBool(IDIsLong)

	err = docSearchParse(c.Request, space, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	if args.TermFilters == nil && args.RangeFilters == nil {
		resp.SendError(c, http.StatusBadRequest, "query filter is null")
		return
	}

	serviceStart := time.Now()
	delByQueryResp := handler.docService.deleteByQuery(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	log.Debug("handleDeleteByQuery cost :%f", serviceCost.Seconds())

	shardsBytes, err := deleteByQueryResult(delByQueryResp)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, shardsBytes)
}

// handleLogPrintSwitch log print switch
func (handler *DocumentHandler) handleLogPrintSwitch(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleLogPrintSwitch", startTime)
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHeadFromGin(c)

	printSwitch, err := doLogPrintSwitchParse(c.Request)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	config.LogInfoPrintSwitch = printSwitch
	if resultBytes, err := docPrintLogSwitchResponse(config.LogInfoPrintSwitch); err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	} else {
		resp.SendJsonBytes(c, resultBytes)
	}
}
func (handler *DocumentHandler) handleDocumentUpsert(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDocumentUpsert"
	defer monitor.Profiler(operateName, startTime)
	span, _ := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.BulkRequest{}
	args.Head = setRequestHeadFromGin(c)

	docRequest, dbName, spaceName, err := documentHeadParse(c.Request)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, fmt.Sprintf("documentHeadParse error: %v", err))
		return
	}

	args.Head.DbName = dbName
	args.Head.SpaceName = spaceName
	space, err := handler.client.Space(c.Request.Context(), args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, fmt.Sprintf("dbName:%s or spaceName:%s not exist", args.Head.DbName, args.Head.SpaceName))
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	err = documentParse(c.Request.Context(), handler, c.Request, docRequest, space, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	reply := handler.docService.bulk(c.Request.Context(), args)
	resultBytes, err := documentUpsertResponse(handler.client, args, reply)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	resp.SendJsonBytes(c, resultBytes)
}

func (handler *DocumentHandler) handleDocumentQuery(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDocumentQuery"
	defer monitor.Profiler(operateName, startTime)
	span, _ := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadFromGin(c)

	searchDoc, query, err := documentRequestParse(c.Request, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, fmt.Sprintf("dbName:%s or spaceName:%s not exist", args.Head.DbName, args.Head.SpaceName))
		return
	}

	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	IDIsLong := idIsLong(space)
	if IDIsLong {
		args.Head.Params["idIsLong"] = "true"
	} else {
		args.Head.Params["idIsLong"] = "false"
	}

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	if args.VecFields != nil {
		resp.SendError(c, http.StatusBadRequest, "document/query query condition must be one of the [document_ids, filter], vector field shouldn't set")
		return
	}

	if len(query.DocumentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			resp.SendError(c, http.StatusBadRequest, "document/query query condition must be one of the [document_ids, filter], shouldn't set both")
			return
		}
		if len(query.DocumentIds) >= 500 {
			resp.SendError(c, http.StatusBadRequest, "document/query length of document_ids in query condition above 500")
			return
		}
		args := &vearchpb.GetRequest{}
		args.Head = setRequestHeadFromGin(c)
		args.Head.DbName = searchDoc.DbName
		args.Head.SpaceName = searchDoc.SpaceName
		args.PrimaryKeys = query.DocumentIds

		var queryFieldsParam map[string]string
		if searchDoc.Fields != nil {
			queryFieldsParam = arrayToMap(searchDoc.Fields)
		}

		reply := &vearchpb.GetResponse{}
		if query.PartitionId != "" {
			reply = handler.docService.getDocsByPartition(c.Request.Context(), args, query.PartitionId, query.Next)
		} else {
			reply = handler.docService.getDocs(c.Request.Context(), args)
		}

		if resultBytes, err := documentGetResponse(handler.client, args, reply, queryFieldsParam, searchDoc.VectorValue); err != nil {
			resp.SendError(c, http.StatusBadRequest, err.Error())
			return
		} else {
			resp.SendJsonBytes(c, resultBytes)
			return
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			resp.SendError(c, http.StatusBadRequest, "document/query query condition must be one of the [document_ids, filter], must set one")
			return
		}
	}
	serviceStart := time.Now()
	searchResp := handler.docService.search(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	var bs []byte
	if searchResp.Results == nil || len(searchResp.Results) == 0 {
		bs, err = documentSearchResponse(nil, searchResp.Head, space, request.QueryResponse)
	} else {
		bs, err = documentSearchResponse(searchResp.Results, searchResp.Head, space, request.QueryResponse)
	}

	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	resp.SendJsonBytes(c, bs)
	log.Debug("handleDocumentQuery total use :[%f] service use :[%f]", time.Since(startTime).Seconds()*1000, serviceCost.Seconds()*1000)
}

func (handler *DocumentHandler) handleDocumentSearch(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDocumentSearch"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadFromGin(c)
	if args.Head.Params == nil {
		params := make(map[string]string)
		args.Head.Params = params
	}

	searchDoc, query, err := documentRequestParse(c.Request, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(ctx, args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, fmt.Sprintf("dbName:%s or spaceName:%s not exist", args.Head.DbName, args.Head.SpaceName))
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	IDIsLong := idIsLong(space)
	if IDIsLong {
		args.Head.Params["idIsLong"] = "true"
	} else {
		args.Head.Params["idIsLong"] = "false"
	}

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	if len(query.DocumentIds) != 0 {
		if args.VecFields != nil {
			resp.SendError(c, http.StatusBadRequest, "document/search search condition must be one of the [document_ids, vector], shouldn't set both")
			return
		}
		if len(query.DocumentIds) > 100 {
			resp.SendError(c, http.StatusBadRequest, "document/search length of document_ids in search condition above 100")
			return
		}
		getArgs := &vearchpb.GetRequest{}
		getArgs.Head = setRequestHeadFromGin(c)
		getArgs.Head.DbName = searchDoc.DbName
		getArgs.Head.SpaceName = searchDoc.SpaceName
		getArgs.PrimaryKeys = query.DocumentIds

		reply := handler.docService.getDocs(ctx, getArgs)

		if reply == nil || reply.Items == nil || len(reply.Items) == 0 {
			result, err := documentSearchResponse(nil, reply.Head, space, request.SearchResponse)
			if err != nil {
				resp.SendError(c, http.StatusBadRequest, err.Error())
				return
			}
			resp.SendJsonBytes(c, result)
			return
		}

		// filter error items
		if reply.Items != nil && len(reply.Items) != 0 {
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
			resp.SendError(c, http.StatusBadRequest, err.Error())
			return
		}
	} else {
		if args.VecFields == nil {
			resp.SendError(c, http.StatusBadRequest, "document/search search condition must be one of the [document_ids, vector], must set one")
			return
		}
	}
	serviceStart := time.Now()
	searchResp := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)

	var bs []byte
	if searchResp.Results == nil || len(searchResp.Results) == 0 {
		bs, err = documentSearchResponse(nil, searchResp.Head, space, request.SearchResponse)
	} else {
		bs, err = documentSearchResponse(searchResp.Results, searchResp.Head, space, request.SearchResponse)
	}

	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	resp.SendJsonBytes(c, bs)
	log.Debug("handleDocumentSearch total use :[%f] service use :[%f]", time.Since(startTime).Seconds()*1000, serviceCost.Seconds()*1000)
}

func (handler *DocumentHandler) handleDocumentDelete(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleDocumentDelete", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadFromGin(c)
	if args.Head.Params != nil {
		paramMap := args.Head.Params
		paramMap["queryOnlyId"] = "true"
		args.Head.Params = paramMap
	} else {
		paramMap := make(map[string]string)
		paramMap["queryOnlyId"] = "true"
		args.Head.Params = paramMap
	}

	searchDoc, query, err := documentRequestParse(c.Request, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, fmt.Sprintf("dbName:%s or spaceName:%s not exist", args.Head.DbName, args.Head.SpaceName))
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	IDIsLong := idIsLong(space)
	if IDIsLong {
		args.Head.Params["idIsLong"] = "true"
	} else {
		args.Head.Params["idIsLong"] = "false"
	}

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	if args.VecFields != nil {
		resp.SendError(c, http.StatusBadRequest, "document/delete query condition must be one of the [document_ids, filter], vector field shouldn't set")
		return
	}

	if len(query.DocumentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			resp.SendError(c, http.StatusBadRequest, "document/delete query condition must be one of the [document_ids, filter], shouldn't set both")
			return
		}
		if len(query.DocumentIds) >= 500 {
			resp.SendError(c, http.StatusBadRequest, "document/delete length of document_ids in query condition above 500")
			return
		}
		args := &vearchpb.DeleteRequest{}
		args.Head = setRequestHeadFromGin(c)
		args.Head.DbName = searchDoc.DbName
		args.Head.SpaceName = searchDoc.SpaceName
		args.PrimaryKeys = query.DocumentIds
		var resultIds []string
		reply := handler.docService.deleteDocs(c.Request.Context(), args)
		if resultBytes, err := documentDeleteResponse(reply.Items, reply.Head, resultIds); err != nil {
			resp.SendError(c, http.StatusBadRequest, err.Error())
			return
		} else {
			resp.SendJsonBytes(c, resultBytes)
			return
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			resp.SendError(c, http.StatusBadRequest, "document/delete query condition must be one of the [document_ids, filter], must set one")
			return
		}
	}
	serviceStart := time.Now()
	delByQueryResp := handler.docService.deleteByQuery(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	log.Debug("handleDocumentDelete cost :%f", serviceCost)
	shardsBytes, err := deleteByQueryResult(delByQueryResp)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, shardsBytes)
}

// handleIndexFlush
func (handler *DocumentHandler) handleIndexFlush(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexFlush", startTime)

	args := &vearchpb.FlushRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, fmt.Sprintf("dbName:%s or spaceName:%s not exist", args.Head.DbName, args.Head.SpaceName))
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	flushResponse := handler.docService.flush(c.Request.Context(), args)
	shardsBytes, err := IndexResponseToContent(flushResponse.Shards)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, shardsBytes)
}

// handleIndexForceMerge build index for gpu
func (handler *DocumentHandler) handleIndexForceMerge(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexForceMerge", startTime)
	args := &vearchpb.ForceMergeRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, fmt.Sprintf("dbName:%s or spaceName:%s not exist", args.Head.DbName, args.Head.SpaceName))
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
	}

	forceMergeResponse := handler.docService.forceMerge(c.Request.Context(), args)
	shardsBytes, err := IndexResponseToContent(forceMergeResponse.Shards)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, shardsBytes)
}

// handleIndexRebuild rebuild index
func (handler *DocumentHandler) handleIndexRebuild(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexRebuild", startTime)
	args := &vearchpb.IndexRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
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

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head.DbName, args.Head.SpaceName)
	if space == nil {
		resp.SendError(c, http.StatusBadRequest, fmt.Sprintf("dbName:%s or spaceName:%s not exist", args.Head.DbName, args.Head.SpaceName))
		return
	}
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
	}

	indexResponse := handler.docService.rebuildIndex(c.Request.Context(), args)
	shardsBytes, err := IndexResponseToContent(indexResponse.Shards)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}

	resp.SendJsonBytes(c, shardsBytes)
}

