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
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/entity/errors"
	"github.com/vearch/vearch/v3/internal/entity/request"
	"github.com/vearch/vearch/v3/internal/monitor"
	"github.com/vearch/vearch/v3/internal/pkg/httphelper"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/netutil"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/router/document/resp"
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

	var group *gin.RouterGroup
	if !config.Conf().Global.SkipAuth {
		group = documentHandler.httpServer.Group("", documentHandler.handleTimeout, gin.BasicAuth(gin.Accounts{
			"root": config.Conf().Global.Signkey,
		}))
	} else {
		group = documentHandler.httpServer.Group("", documentHandler.handleTimeout)
	}
	documentHandler.proxyMaster(group)
	// open router api
	if err := documentHandler.ExportInterfacesToServer(group); err != nil {
		panic(err)
	}
}

func (handler *DocumentHandler) proxyMaster(group *gin.RouterGroup) error {
	// list/*
	group.GET("/servers", handler.handleMasterRequest)
	group.GET("/partitions", handler.handleMasterRequest)
	group.GET("/routers", handler.handleMasterRequest)
	// db handler
	group.POST(fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	group.GET("/dbs", handler.handleMasterRequest)
	group.DELETE(fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	group.PUT(fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	// space handler
	group.POST(fmt.Sprintf("/dbs/:%s/spaces", URLParamDbName), handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/dbs/:%s/spaces", URLParamDbName), handler.handleMasterRequest)
	group.DELETE(fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	group.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	// alias handler
	group.POST(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", URLAliasName, URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/alias/:%s", URLAliasName), handler.handleMasterRequest)
	group.GET("/alias", handler.handleMasterRequest)
	group.DELETE(fmt.Sprintf("/alias/:%s", URLAliasName), handler.handleMasterRequest)
	group.PUT(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", URLAliasName, URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	// cluster handler
	group.GET("/cluster/health", handler.handleMasterRequest)
	group.GET("/cluster/stats", handler.handleMasterRequest)

	return nil
}

func (handler *DocumentHandler) handleMasterRequest(c *gin.Context) {
	method := c.Request.Method
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		resp.SendError(c, http.StatusBadRequest, "Error reading body")
		return
	}

	res, err := handler.client.Master().ProxyHTTPRequest(method, c.Request.RequestURI, string(bodyBytes))
	if err != nil {
		log.Error("handleMasterRequest %v, response %s", err, string(res))
		httphelper.New(c).SetHttpStatus(http.StatusInternalServerError).SendJsonBytes(res)
		return
	}
	resp.SendJsonBytes(c, res)
}

func (handler *DocumentHandler) ExportInterfacesToServer(group *gin.RouterGroup) error {
	// The data operation will be redefined as the following 2 type interfaces: document and index
	// document
	group.POST("/document/upsert", handler.handleDocumentUpsert)
	group.POST("/document/query", handler.handleDocumentQuery)
	group.POST("/document/search", handler.handleDocumentSearch)
	group.POST("/document/delete", handler.handleDocumentDelete)

	// index
	group.POST("/index/flush", handler.handleIndexFlush)
	group.POST("/index/forcemerge", handler.handleIndexForceMerge)
	group.POST("/index/rebuild", handler.handleIndexRebuild)

	// config
	// trace: /config/trace
	group.POST("/config/trace", handler.handleConfigTrace)

	// cacheInfo
	// /cache/$dbName/$spaceName
	group.GET(fmt.Sprintf("/cache/:%s/:%s", URLParamDbName, URLParamSpaceName), handler.cacheInfo)

	return nil
}

func (handler *DocumentHandler) handleTimeout(c *gin.Context) {
	messageID := uuid.NewString()
	c.Set(entity.MessageID, messageID)
}

func (handler *DocumentHandler) cacheInfo(c *gin.Context) {
	dbName := c.Param(URLParamDbName)
	spaceName := c.Param(URLParamSpaceName)
	if space, err := handler.client.Master().Cache().SpaceByCache(context.Background(), dbName, spaceName); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(space)
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

// handleConfigTrace config trace switch
func (handler *DocumentHandler) handleConfigTrace(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleConfigTrace", startTime)
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHeadFromGin(c)

	trace, err := configTraceParse(c.Request)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	config.Trace = trace
	if resultBytes, err := configTraceResponse(config.Trace); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httphelper.New(c).JsonSuccess(resultBytes)
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
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	args.Head.DbName = dbName
	args.Head.SpaceName = spaceName
	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	err = documentParse(c.Request.Context(), handler, c.Request, docRequest, space, args)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	reply := handler.docService.bulk(c.Request.Context(), args)
	result, err := documentUpsertResponse(args, reply)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}
	httphelper.New(c).JsonSuccess(result)
}

func (handler *DocumentHandler) handleDocumentQuery(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDocumentQuery"
	defer monitor.Profiler(operateName, startTime)
	span, _ := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.QueryRequest{}
	args.Head = setRequestHeadFromGin(c)

	searchDoc, err := documentRequestParse(c.Request)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = args.Head.SpaceName

	err = queryRequestToPb(searchDoc, space, args)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if searchDoc.DocumentIds != nil && len(*searchDoc.DocumentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_BOTH_DOCUMENT_IDS_AND_FILTER, nil)
			httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
		handler.handleDocumentGet(c, searchDoc)
		return
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_SHOULD_HAVE_ONE_OF_DOCUMENT_IDS_OR_FILTER, nil)
			httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
	}
	serviceStart := time.Now()
	searchResp := handler.docService.query(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	result, err := documentQueryResponse(searchResp.Results, searchResp.Head)

	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}
	httphelper.New(c).JsonSuccess(result)
	log.Trace("handleDocumentQuery total use :[%f] service use :[%f]", time.Since(startTime).Seconds()*1000, serviceCost.Seconds()*1000)
}

func (handler *DocumentHandler) handleDocumentGet(c *gin.Context, searchDoc *request.SearchDocumentRequest) {
	if len(*searchDoc.DocumentIds) >= 500 {
		err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_LENGTH_OF_DOCUMENT_IDS_BEYOND_500, nil)
		httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}
	args := &vearchpb.GetRequest{}
	args.Head = setRequestHeadFromGin(c)
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName
	args.PrimaryKeys = *searchDoc.DocumentIds

	var queryFieldsParam map[string]string
	if searchDoc.Fields != nil {
		queryFieldsParam = arrayToMap(searchDoc.Fields)
	}

	reply := &vearchpb.GetResponse{}
	if searchDoc.PartitionId != nil {
		reply = handler.docService.getDocsByPartition(c.Request.Context(), args, *searchDoc.PartitionId, searchDoc.Next)
	} else {
		reply = handler.docService.getDocs(c.Request.Context(), args)
	}

	if result, err := documentGetResponse(handler.client, args, reply, queryFieldsParam, searchDoc.VectorValue); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httphelper.New(c).JsonSuccess(result)
		return
	}
}

func (handler *DocumentHandler) handleDocumentSearch(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDocumentSearch"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadFromGin(c)

	searchDoc, err := documentRequestParse(c.Request)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	trace := config.Trace
	if trace_info, ok := args.Head.Params["trace"]; ok {
		if trace_info == "true" {
			trace = true
		}
	}

	getSpaceStart := time.Now()
	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = args.Head.SpaceName
	getSpaceCost := time.Since(getSpaceStart)

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if args.VecFields == nil {
		err := vearchpb.NewError(vearchpb.ErrorEnum_SEARCH_INVALID_PARAMS_SHOULD_HAVE_VECTOR_FIELD, nil)
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	serviceStart := time.Now()
	searchResp := handler.docService.search(ctx, args)
	serviceCost := time.Since(serviceStart)

	result, err := documentSearchResponse(searchResp.Results, searchResp.Head)

	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	httphelper.New(c).JsonSuccess(result)
	if trace {
		log.Trace("handleDocumentSearch total use :[%f] getSpace use :[%f] service use :[%f] detail use :[%v]",
			time.Since(startTime).Seconds()*1000, getSpaceCost.Seconds()*1000, serviceCost.Seconds()*1000, searchResp.Head.Params)
	}
}

func (handler *DocumentHandler) handleDocumentDelete(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleDocumentDelete", startTime)
	args := &vearchpb.SearchRequest{}
	args.Head = setRequestHeadFromGin(c)
	args.Head.Params["queryOnlyId"] = "true"

	searchDoc, err := documentRequestParse(c.Request)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = args.Head.SpaceName

	err = requestToPb(searchDoc, space, args)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if args.VecFields != nil {
		err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_SHOULD_NOT_HAVE_VECTOR_FIELD, nil)
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if searchDoc.DocumentIds != nil && len(*searchDoc.DocumentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_BOTH_DOCUMENT_IDS_AND_VECTOR, nil)
			httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
		if len(*searchDoc.DocumentIds) >= 500 {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_LENGTH_OF_DOCUMENT_IDS_BEYOND_500, nil)
			httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
		args := &vearchpb.DeleteRequest{}
		args.Head = setRequestHeadFromGin(c)
		args.Head.DbName = searchDoc.DbName
		args.Head.SpaceName = searchDoc.SpaceName
		args.PrimaryKeys = *searchDoc.DocumentIds
		var resultIds []string
		reply := handler.docService.deleteDocs(c.Request.Context(), args)
		if result, err := documentDeleteResponse(reply.Items, reply.Head, resultIds); err != nil {
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
			return
		} else {
			httphelper.New(c).JsonSuccess(result)
			return
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_SHOULD_HAVE_ONE_OF_DOCUMENT_IDS_OR_FILTER, nil)
			httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
	}
	serviceStart := time.Now()
	delByQueryResp := handler.docService.deleteByQuery(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	log.Trace("handleDocumentDelete cost :%f", serviceCost)
	result, err := deleteByQueryResult(delByQueryResp)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}

	httphelper.New(c).JsonSuccess(result)
}

// handleIndexFlush
func (handler *DocumentHandler) handleIndexFlush(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexFlush", startTime)

	args := &vearchpb.FlushRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	_, err = handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	flushResponse := handler.docService.flush(c.Request.Context(), args)
	result := IndexResponseToContent(flushResponse.Shards)
	httphelper.New(c).JsonSuccess(result)
}

// handleIndexForceMerge build index for gpu
func (handler *DocumentHandler) handleIndexForceMerge(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexForceMerge", startTime)
	args := &vearchpb.ForceMergeRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	_, err = handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	forceMergeResponse := handler.docService.forceMerge(c.Request.Context(), args)
	result := IndexResponseToContent(forceMergeResponse.Shards)

	httphelper.New(c).JsonSuccess(result)
}

// handleIndexRebuild rebuild index
func (handler *DocumentHandler) handleIndexRebuild(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexRebuild", startTime)
	args := &vearchpb.IndexRequest{}
	args.Head = setRequestHeadFromGin(c)

	indexRequest, err := IndexRequestParse(c.Request)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
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
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	indexResponse := handler.docService.rebuildIndex(c.Request.Context(), args)
	result := IndexResponseToContent(indexResponse.Shards)

	httphelper.New(c).JsonSuccess(result)
}
