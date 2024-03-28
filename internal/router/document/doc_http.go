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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/vearch/vearch/internal/client"
	"github.com/vearch/vearch/internal/config"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/entity/request"
	"github.com/vearch/vearch/internal/monitor"
	"github.com/vearch/vearch/internal/pkg/ginutil"
	"github.com/vearch/vearch/internal/pkg/log"
	"github.com/vearch/vearch/internal/pkg/netutil"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/router/document/resp"
)

const (
	URLParamDbName      = "db_name"
	URLParamSpaceName   = "space_name"
	URLParamPartitionID = "partition_id"
	URLParamID          = "_id"
	URLParams           = "url_params"
	ReqsBody            = "req_body"
	SpaceEntity         = "space_entity"
	QueryIsOnlyID       = "QueryIsOnlyID"
	URLQueryTimeout     = "timeout"
	URLAliasName        = "alias_name"
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
	handler.httpServer.Handle(http.MethodGet, "/servers", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/partitions", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/routers", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// db handler
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/dbs", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodDelete, fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodPut, fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// space handler
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/dbs/:%s/spaces", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/dbs/:%s/spaces", URLParamDbName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodDelete, fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodPut, fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// alias handler
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", URLAliasName, URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/alias/:%s", URLAliasName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/alias", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodDelete, fmt.Sprintf("/alias/:%s", URLAliasName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodPut, fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", URLAliasName, URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	// cluster handler
	handler.httpServer.Handle(http.MethodGet, "/cluster/health", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)
	handler.httpServer.Handle(http.MethodGet, "/cluster/stats", handler.handleTimeout, handler.handleAuth, handler.handleMasterRequest)

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
	// update doc: /$dbName/$spaceName/_log_collect
	handler.httpServer.Handle(http.MethodPost, fmt.Sprintf("/:%s/:%s/_log_print_switch", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.handleLogPrintSwitch)
	// cacheInfo /$dbName/$spaceName
	handler.httpServer.Handle(http.MethodGet, fmt.Sprintf("/:%s/:%s", URLParamDbName, URLParamSpaceName), handler.handleTimeout, handler.handleAuth, handler.cacheInfo)
	return nil
}

func (handler *DocumentHandler) handleTimeout(c *gin.Context) {
	messageID := uuid.NewString()
	c.Set(entity.MessageID, messageID)
}

func (handler *DocumentHandler) handleAuth(c *gin.Context) {
	if config.Conf().Global.SkipAuth {
		return
	}
	headerData := c.GetHeader("Authorization")
	username, password, err := netutil.AuthDecrypt(headerData)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, err.Error())
		return
	}
	if username != "root" || password != config.Conf().Global.Signkey {
		resp.SendError(c, http.StatusBadRequest, "authorization failed, wrong user or password")
		return
	}
}

func (handler *DocumentHandler) cacheInfo(c *gin.Context) {
	dbName := c.Param(URLParamDbName)
	spaceName := c.Param(URLParamSpaceName)
	if space, err := handler.client.Master().Cache().SpaceByCache(context.Background(), dbName, spaceName); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(space)
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

// handleLogPrintSwitch log print switch
func (handler *DocumentHandler) handleLogPrintSwitch(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleLogPrintSwitch", startTime)
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHeadFromGin(c)

	printSwitch, err := doLogPrintSwitchParse(c.Request)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	config.LogInfoPrintSwitch = printSwitch
	if resultBytes, err := docPrintLogSwitchResponse(config.LogInfoPrintSwitch); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(resultBytes)
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
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	args.Head.DbName = dbName
	args.Head.SpaceName = spaceName
	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	err = documentParse(c.Request.Context(), handler, c.Request, docRequest, space, args)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	reply := handler.docService.bulk(c.Request.Context(), args)
	result, err := documentUpsertResponse(args, reply)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
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
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = args.Head.SpaceName

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if args.VecFields != nil {
		err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_SHOULD_NOT_HAVE_VECTOR_FIELD, nil)
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if len(query.DocumentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_BOTH_DOCUMENT_IDS_AND_FILTER, nil)
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		}
		if len(query.DocumentIds) >= 500 {
			err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_LENGTH_OF_DOCUMENT_IDS_BEYOND_500, nil)
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
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

		if result, err := documentGetResponse(handler.client, args, reply, queryFieldsParam, searchDoc.VectorValue); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		} else {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
			return
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_SHOULD_HAVE_ONE_OF_DOCUMENT_IDS_OR_FILTER, nil)
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		}
	}
	serviceStart := time.Now()
	searchResp := handler.docService.search(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	var result map[string]interface{}
	if searchResp.Results == nil || len(searchResp.Results) == 0 {
		result, err = documentSearchResponse(nil, searchResp.Head, request.QueryResponse)
	} else {
		result, err = documentSearchResponse(searchResp.Results, searchResp.Head, request.QueryResponse)
	}

	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
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

	searchDoc, _, err := documentRequestParse(c.Request, args)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = args.Head.SpaceName

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if args.VecFields == nil {
		err := vearchpb.NewError(vearchpb.ErrorEnum_SEARCH_INVALID_PARAMS_SHOULD_HAVE_VECTOR_FIELD, nil)
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	serviceStart := time.Now()
	searchResp := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)

	var result map[string]interface{}
	if len(searchResp.Results) == 0 {
		result, err = documentSearchResponse(nil, searchResp.Head, request.SearchResponse)
	} else {
		result, err = documentSearchResponse(searchResp.Results, searchResp.Head, request.SearchResponse)
	}

	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
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
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = args.Head.SpaceName

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if args.VecFields != nil {
		err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_SHOULD_NOT_HAVE_VECTOR_FIELD, nil)
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if len(query.DocumentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_BOTH_DOCUMENT_IDS_AND_VECTOR, nil)
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		}
		if len(query.DocumentIds) >= 500 {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_LENGTH_OF_DOCUMENT_IDS_BEYOND_500, nil)
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		}
		args := &vearchpb.DeleteRequest{}
		args.Head = setRequestHeadFromGin(c)
		args.Head.DbName = searchDoc.DbName
		args.Head.SpaceName = searchDoc.SpaceName
		args.PrimaryKeys = query.DocumentIds
		var resultIds []string
		reply := handler.docService.deleteDocs(c.Request.Context(), args)
		if result, err := documentDeleteResponse(reply.Items, reply.Head, resultIds); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		} else {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
			return
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_SHOULD_HAVE_ONE_OF_DOCUMENT_IDS_OR_FILTER, nil)
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		}
	}
	serviceStart := time.Now()
	delByQueryResp := handler.docService.deleteByQuery(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	log.Debug("handleDocumentDelete cost :%f", serviceCost)
	result, err := deleteByQueryResult(delByQueryResp)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
}

// handleIndexFlush
func (handler *DocumentHandler) handleIndexFlush(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexFlush", startTime)

	args := &vearchpb.FlushRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	_, err = handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	flushResponse := handler.docService.flush(c.Request.Context(), args)
	result, err := IndexResponseToContent(flushResponse.Shards)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
}

// handleIndexForceMerge build index for gpu
func (handler *DocumentHandler) handleIndexForceMerge(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexForceMerge", startTime)
	args := &vearchpb.ForceMergeRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	_, err = handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	forceMergeResponse := handler.docService.forceMerge(c.Request.Context(), args)
	result, err := IndexResponseToContent(forceMergeResponse.Shards)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
}

// handleIndexRebuild rebuild index
func (handler *DocumentHandler) handleIndexRebuild(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexRebuild", startTime)
	args := &vearchpb.IndexRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
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

	_, err = handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	indexResponse := handler.docService.rebuildIndex(c.Request.Context(), args)
	result, err := IndexResponseToContent(indexResponse.Shards)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(result)
}
