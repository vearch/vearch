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
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/opentracing/opentracing-go"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/entity/errors"
	"github.com/vearch/vearch/v3/internal/entity/request"
	"github.com/vearch/vearch/v3/internal/entity/response"
	"github.com/vearch/vearch/v3/internal/master"
	"github.com/vearch/vearch/v3/internal/monitor"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/netutil"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
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
	URLParamAliasName   = "alias_name"
	URLParamUserName    = "user_name"
	URLParamRoleName    = "role_name"
	URLParamMemberId    = "member_id"
	NodeID              = "node_id"
	defaultTimeout      = 10 * time.Second
)

type DocumentHandler struct {
	httpServer *gin.Engine
	docService docService
	client     *client.Client
}

func BasicAuthMiddleware(docService docService) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			err := fmt.Errorf("auth header is empty")
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Basic" {
			err := fmt.Errorf("auth header type is invalid")
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		credentials := strings.SplitN(string(decoded), ":", 2)
		if len(credentials) != 2 {
			err := fmt.Errorf("auth header credentials is invalid")
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		user, err := docService.getUser(c, credentials[0])
		if err != nil {
			ferr := fmt.Errorf("auth header user %s is invalid", credentials[0])
			response.New(c).JsonError(errors.NewErrUnauthorized(ferr))
			c.Abort()
			return
		}
		if *user.Password != credentials[1] {
			err := fmt.Errorf("auth header password is invalid")
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}
		role, err := docService.getRole(c, *user.RoleName)
		if err != nil {
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}
		endpoint := c.FullPath()
		method := c.Request.Method
		if err := role.HasPermissionForResources(endpoint, method); err != nil {
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		c.Next()
	}
}

func HttpLimitMiddleware(docService docService) gin.HandlerFunc {
	return func(c *gin.Context) {
		Request := strings.TrimPrefix(c.Request.RequestURI, "/document/")

		switch Request {
		case "upsert", "delete":
			if !entity.WriteLimiter.Allow() {
				msg := fmt.Sprintf("document write request too frequency, have reached limit %d", entity.WriteLimiter.Burst())
				log.Error(msg)
				response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf(msg)))
				c.Abort()
				return
			}
		case "query", "search":
			if !entity.ReadLimiter.Allow() {
				msg := fmt.Sprintf("document read request too frequency, have reached limit %d", entity.ReadLimiter.Burst())
				log.Error(msg)
				response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf(msg)))
				c.Abort()
				return
			}
		}

		c.Next()
	}
}

func ExportDocumentHandler(httpServer *gin.Engine, client *client.Client) {
	docService := newDocService(client)

	documentHandler := &DocumentHandler{
		httpServer: httpServer,
		docService: *docService,
		client:     client,
	}

	var group *gin.RouterGroup = documentHandler.httpServer.Group("", master.TimeoutMiddleware(defaultTimeout))
	var groupProxy *gin.RouterGroup = documentHandler.httpServer.Group("")
	if !config.Conf().Global.SkipAuth {
		group.Use(BasicAuthMiddleware(documentHandler.docService))
	}

	documentHandler.proxyMaster(groupProxy)
	// open router api
	if err := documentHandler.ExportInterfacesToServer(group); err != nil {
		panic(err)
	}
}

func (handler *DocumentHandler) proxyMaster(group *gin.RouterGroup) error {
	// server handler
	group.GET("/servers", handler.handleMasterRequest)

	// partition handler
	group.GET("/partitions", handler.handleMasterRequest)
	group.POST("/partitions/change_member", handler.handleMasterRequest)
	group.POST("/partitions/resource_limit", handler.handleMasterRequest)

	// schedule
	group.POST("/schedule/recover_server", handler.handleMasterRequest)
	group.POST("/schedule/change_replicas", handler.handleMasterRequest)
	group.GET("/schedule/fail_server", handler.handleMasterRequest)
	group.DELETE("/schedule/fail_server/:"+NodeID, handler.handleMasterRequest)
	group.GET("/schedule/clean_task", handler.handleMasterRequest)

	// remove server metadata
	group.POST("/meta/remove_server", handler.handleMasterRequest)

	group.GET("/routers", handler.handleMasterRequest)

	// db handler
	group.POST(fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	group.GET("/dbs", handler.handleMasterRequest)
	group.DELETE(fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	group.PUT(fmt.Sprintf("/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	group.POST(fmt.Sprintf("/backup/dbs/:%s", URLParamDbName), handler.handleMasterRequest)
	group.POST(fmt.Sprintf("/backup/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	// space handler
	group.POST(fmt.Sprintf("/dbs/:%s/spaces", URLParamDbName), handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/dbs/:%s/spaces", URLParamDbName), handler.handleMasterRequest)
	group.DELETE(fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	group.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)

	// alias handler
	group.POST(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", URLParamAliasName, URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/alias/:%s", URLParamAliasName), handler.handleMasterRequest)
	group.GET("/alias", handler.handleMasterRequest)
	group.DELETE(fmt.Sprintf("/alias/:%s", URLParamAliasName), handler.handleMasterRequest)
	group.PUT(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", URLParamAliasName, URLParamDbName, URLParamSpaceName), handler.handleMasterRequest)

	// user handler
	group.POST("/users", handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/users/:%s", URLParamUserName), handler.handleMasterRequest)
	group.GET("/users", handler.handleMasterRequest)
	group.DELETE(fmt.Sprintf("/users/:%s", URLParamUserName), handler.handleMasterRequest)
	group.PUT("/users", handler.handleMasterRequest)

	// role handler
	group.POST("/roles", handler.handleMasterRequest)
	group.GET(fmt.Sprintf("/roles/:%s", URLParamRoleName), handler.handleMasterRequest)
	group.GET("/roles", handler.handleMasterRequest)
	group.DELETE(fmt.Sprintf("/roles/:%s", URLParamRoleName), handler.handleMasterRequest)
	group.PUT("/roles", handler.handleMasterRequest)

	// cluster handler
	group.GET("/cluster/health", handler.handleMasterRequest)
	group.GET("/cluster/stats", handler.handleMasterRequest)

	// config handler
	group.POST("/config/:"+URLParamDbName+"/:"+URLParamSpaceName, handler.handleMasterRequest)
	group.GET("/config/:"+URLParamDbName+"/:"+URLParamSpaceName, handler.handleMasterRequest)

	group.POST("/config/request_limit", handler.handleMasterRequest)
	group.GET("/config/request_limit", handler.handleMasterRequest)

	// members handler
	group.GET("/members", handler.handleMasterRequest)
	group.GET("/members/stats", handler.handleMasterRequest)
	group.DELETE("/members", handler.handleMasterRequest)
	group.POST("/members", handler.handleMasterRequest)
	return nil
}

func (handler *DocumentHandler) handleMasterRequest(c *gin.Context) {
	method := c.Request.Method
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	authHeader := c.GetHeader("Authorization")
	res, err := handler.client.Master().ProxyHTTPRequest(method, c.Request.RequestURI, string(bodyBytes), authHeader)
	if err != nil {
		log.Error("handleMasterRequest %v, response %s", err, string(res))
		if string(res) != "" {
			response.New(c).SetHttpStatus(http.StatusInternalServerError).SendJsonBytes(res)
		} else {
			response.New(c).JsonError(errors.NewErrInternal(err))
		}
		return
	}
	response.New(c).SendJsonBytes(res)
}

func (handler *DocumentHandler) ExportInterfacesToServer(group *gin.RouterGroup) error {
	// router info
	group.GET("/", handler.handleRouterInfo)

	// document
	groupdoc := group.Group("/document")
	groupdoc.Use(HttpLimitMiddleware(handler.docService))

	groupdoc.POST("/upsert", handler.handleDocumentUpsert)
	groupdoc.POST("/query", handler.handleDocumentQuery)
	groupdoc.POST("/search", handler.handleDocumentSearch)
	groupdoc.POST("/delete", handler.handleDocumentDelete)

	// index
	group.POST("/index/flush", handler.handleIndexFlush)
	group.POST("/index/forcemerge", handler.handleIndexForceMerge)
	group.POST("/index/rebuild", handler.handleIndexRebuild)

	// config
	// trace: /config/trace
	group.POST("/config/trace", handler.handleConfigTrace)

	// cacheInfo
	// /cache/$dbName/$spaceName
	group.GET(fmt.Sprintf("/cache/dbs/:%s/spaces/:%s", URLParamDbName, URLParamSpaceName), handler.cacheSpaceInfo)
	group.GET(fmt.Sprintf("/cache/users/:%s", URLParamUserName), handler.cacheUserInfo)
	group.GET(fmt.Sprintf("/cache/roles/:%s", URLParamRoleName), handler.cacheRoleInfo)

	return nil
}

func (handler *DocumentHandler) handleRouterInfo(c *gin.Context) {
	versionLayer := make(map[string]interface{})
	versionLayer["build_version"] = config.GetBuildVersion()
	versionLayer["build_time"] = config.GetBuildTime()
	versionLayer["commit_id"] = config.GetCommitID()

	layer := make(map[string]interface{})
	layer["version"] = versionLayer
	layer["name"] = config.Conf().Global.Name

	response.New(c).JsonSuccess(layer)
}

func (handler *DocumentHandler) cacheSpaceInfo(c *gin.Context) {
	dbName := c.Param(URLParamDbName)
	spaceName := c.Param(URLParamSpaceName)
	if space, err := handler.client.Master().Cache().SpaceByCache(context.Background(), dbName, spaceName); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(space)
	}
}

func (handler *DocumentHandler) cacheUserInfo(c *gin.Context) {
	userName := c.Param(URLParamUserName)
	if space, err := handler.client.Master().Cache().UserByCache(context.Background(), userName); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(space)
	}
}

func (handler *DocumentHandler) cacheRoleInfo(c *gin.Context) {
	roleName := c.Param(URLParamRoleName)
	if space, err := handler.client.Master().Cache().RoleByCache(context.Background(), roleName); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(space)
	}
}

// setRequestHead set head of request
func setRequestHead(params netutil.UriParams, r *http.Request) (head *vearchpb.RequestHead) {
	head = &vearchpb.RequestHead{}
	head.DbName = params.ByName(URLParamDbName)
	head.SpaceName = params.ByName(URLParamSpaceName)
	head.Params = netutil.GetUrlQuery(r)
	return
}

func setRequestHeadFromGin(c *gin.Context) (*vearchpb.RequestHead, error) {
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

	head.Params["request_id"] = c.GetHeader("X-Request-Id")

	return head, nil
}

// handleConfigTrace config trace switch
func (handler *DocumentHandler) handleConfigTrace(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleConfigTrace", startTime)
	var err error
	_, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	trace, err := configTraceParse(c.Request)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	config.Trace = trace
	if resultBytes, err := configTraceResponse(config.Trace); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		response.New(c).JsonSuccess(resultBytes)
	}
}

func (handler *DocumentHandler) handleDocumentUpsert(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDocumentUpsert"
	defer monitor.Profiler(operateName, startTime)
	span, _ := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.BulkRequest{}
	var err error
	args.Head, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	docRequest := &request.DocumentRequest{}
	err = c.ShouldBindJSON(docRequest)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	dbName := docRequest.DbName
	spaceName := docRequest.SpaceName
	args.Head.DbName = dbName
	args.Head.SpaceName = spaceName
	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	err = documentParse(c.Request.Context(), handler, c.Request, docRequest, space, args)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	reply := handler.docService.bulk(c.Request.Context(), args)
	result, err := documentUpsertResponse(reply)
	if err != nil {
		response.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}
	response.New(c).JsonSuccess(result)
}

func (handler *DocumentHandler) handleDocumentQuery(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDocumentQuery"
	defer monitor.Profiler(operateName, startTime)
	span, _ := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()

	args := &vearchpb.QueryRequest{}
	var err error
	args.Head, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	trace := config.Trace
	if trace_info, ok := args.Head.Params["trace"]; ok {
		if trace_info == "true" {
			trace = true
		}
	}

	searchDoc := &request.SearchDocumentRequest{}
	err = c.ShouldBindJSON(searchDoc)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = args.Head.SpaceName

	err = queryRequestToPb(searchDoc, space, args)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if searchDoc.DocumentIds != nil && len(*searchDoc.DocumentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_BOTH_DOCUMENT_IDS_AND_FILTER, nil)
			response.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
		if len(*searchDoc.DocumentIds) >= 500 {
			err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_LENGTH_OF_DOCUMENT_IDS_BEYOND_500, nil)
			response.New(c).JsonError(errors.NewErrUnprocessable(err))
			return
		}
		if searchDoc.GetByHash || searchDoc.PartitionId != nil {
			handler.handleDocumentGet(c, searchDoc, space)
			return
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_QUERY_INVALID_PARAMS_SHOULD_HAVE_ONE_OF_DOCUMENT_IDS_OR_FILTER, nil)
			response.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
	}

	serviceStart := time.Now()
	searchResp := handler.docService.query(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	result, err := documentQueryResponse(searchResp.Results, searchResp.Head, space)
	if err != nil {
		response.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}
	response.New(c).JsonSuccess(result)
	if trace {
		log.Trace("handleDocumentQuery total use :[%.4f] service use :[%.4f] detail use :[%v]", time.Since(startTime).Seconds()*1000, serviceCost.Seconds()*1000, searchResp.Head.Params)
	}
}

func (handler *DocumentHandler) handleDocumentGet(c *gin.Context, searchDoc *request.SearchDocumentRequest, space *entity.Space) {
	args := &vearchpb.GetRequest{}
	var err error
	args.Head, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName
	args.PrimaryKeys = *searchDoc.DocumentIds

	var queryFieldsParam map[string]string
	if searchDoc.Fields != nil {
		queryFieldsParam = arrayToMap(searchDoc.Fields)
	}

	if searchDoc.PartitionId != nil {
		found := false
		for _, partition := range space.Partitions {
			if partition.Id == *searchDoc.PartitionId {
				found = true
				break
			}
		}
		if !found {
			err := vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition_id %d not belong to space %s", *searchDoc.PartitionId, space.Name))
			response.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
	}

	var reply *vearchpb.GetResponse
	if searchDoc.PartitionId != nil {
		reply = handler.docService.getDocsByPartition(c.Request.Context(), args, *searchDoc.PartitionId, searchDoc.Next)
	} else {
		reply = handler.docService.getDocs(c.Request.Context(), args)
	}

	if result, err := documentGetResponse(space, reply, queryFieldsParam, searchDoc.VectorValue); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		response.New(c).JsonSuccess(result)
		return
	}
}

func (handler *DocumentHandler) handleDocumentSearch(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDocumentSearch"
	defer monitor.Profiler(operateName, startTime)
	span, ctx := opentracing.StartSpanFromContext(c.Request.Context(), operateName)
	defer span.Finish()
	searchReq := &vearchpb.SearchRequest{}
	var err error
	searchReq.Head, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	searchDoc := &request.SearchDocumentRequest{}
	err = c.ShouldBindJSON(searchDoc)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	searchReq.Head.DbName = searchDoc.DbName
	searchReq.Head.SpaceName = searchDoc.SpaceName

	trace := config.Trace
	if bTrace, ok := searchReq.Head.Params["trace"]; ok {
		if bTrace == "true" {
			trace = true
		}
	}

	getSpaceStart := time.Now()
	space, err := handler.docService.getSpace(c.Request.Context(), searchReq.Head)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = searchReq.Head.SpaceName
	getSpaceCost := time.Since(getSpaceStart)

	err = requestToPb(searchDoc, space, searchReq)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if searchReq.VecFields == nil {
		err := vearchpb.NewError(vearchpb.ErrorEnum_SEARCH_INVALID_PARAMS_SHOULD_HAVE_VECTOR_FIELD, nil)
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	serviceStart := time.Now()
	searchResp := handler.docService.search(ctx, searchReq)
	serviceCost := time.Since(serviceStart)

	var responseCostStart time.Time
	var responseCost time.Duration
	if trace {
		responseCostStart = time.Now()
	}
	result, err := documentSearchResponse(searchResp.Results, searchResp.Head, space)

	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	response.New(c).JsonSuccess(result)
	if trace {
		responseCost = time.Since(responseCostStart)
	}
	if trace {
		log.Trace("handleDocumentSearch %s total: [%.4f] getSpace: [%.4f] service: [%.4f] response: [%.4f] detail: [%v]",
			searchReq.Head.Params["request_id"], time.Since(startTime).Seconds()*1000, getSpaceCost.Seconds()*1000,
			serviceCost.Seconds()*1000, responseCost.Seconds()*1000, searchResp.Head.Params)
	}
}

func (handler *DocumentHandler) handleDocumentDelete(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleDocumentDelete", startTime)
	args := &vearchpb.QueryRequest{}
	var err error
	args.Head, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	args.Head.Params["queryOnlyId"] = "true"

	trace := config.Trace
	if trace_info, ok := args.Head.Params["trace"]; ok {
		if trace_info == "true" {
			trace = true
		}
	}

	searchDoc := &request.SearchDocumentRequest{}
	err = c.ShouldBindJSON(searchDoc)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	args.Head.DbName = searchDoc.DbName
	args.Head.SpaceName = searchDoc.SpaceName

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	// update space name because maybe is alias name
	searchDoc.SpaceName = args.Head.SpaceName

	err = queryRequestToPb(searchDoc, space, args)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if searchDoc.DocumentIds != nil && len(*searchDoc.DocumentIds) != 0 {
		if args.TermFilters != nil || args.RangeFilters != nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_BOTH_DOCUMENT_IDS_AND_VECTOR, nil)
			response.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
		if len(*searchDoc.DocumentIds) >= 500 {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_LENGTH_OF_DOCUMENT_IDS_BEYOND_500, nil)
			response.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
	} else {
		if args.TermFilters == nil && args.RangeFilters == nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_DELETE_INVALID_PARAMS_SHOULD_HAVE_ONE_OF_DOCUMENT_IDS_OR_FILTER, nil)
			response.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
	}
	serviceStart := time.Now()
	delByQueryResp := handler.docService.deleteByQuery(c.Request.Context(), args)
	serviceCost := time.Since(serviceStart)

	result, err := deleteByQueryResult(delByQueryResp)
	if err != nil {
		response.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}

	response.New(c).JsonSuccess(result)
	if trace {
		log.Trace("handleDocumentDelete total use :[%.4f] service use :[%.4f]",
			time.Since(startTime).Seconds()*1000, serviceCost.Seconds()*1000)
	}
}

// handleIndexFlush
func (handler *DocumentHandler) handleIndexFlush(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexFlush", startTime)

	args := &vearchpb.FlushRequest{}
	var err error
	args.Head, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	indexRequest := &request.IndexRequest{}
	err = c.ShouldBindJSON(indexRequest)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName

	_, err = handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	flushResponse := handler.docService.flush(c.Request.Context(), args)
	result := IndexResponseToContent(flushResponse.Shards)
	response.New(c).JsonSuccess(result)
}

// handleIndexForceMerge build index for gpu
func (handler *DocumentHandler) handleIndexForceMerge(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexForceMerge", startTime)
	args := &vearchpb.ForceMergeRequest{}
	var err error
	args.Head, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	indexRequest := &request.IndexRequest{}
	err = c.ShouldBindJSON(indexRequest)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	args.Head.DbName = indexRequest.DbName
	args.Head.SpaceName = indexRequest.SpaceName
	args.PartitionId = indexRequest.PartitionId

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	if space == nil {
		err := vearchpb.NewError(vearchpb.ErrorEnum_SPACE_NOT_EXIST, nil)
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if args.PartitionId > 0 {
		found := false
		for _, partition := range space.Partitions {
			if partition.Id == args.PartitionId {
				found = true
				break
			}
		}
		if !found {
			err := vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition_id %d not belong to space %s", args.PartitionId, space.Name))
			response.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
	}

	forceMergeResponse := handler.docService.forceMerge(c.Request.Context(), args)
	result := IndexResponseToContent(forceMergeResponse.Shards)

	response.New(c).JsonSuccess(result)
}

// handleIndexRebuild rebuild index
func (handler *DocumentHandler) handleIndexRebuild(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("handleIndexRebuild", startTime)
	args := &vearchpb.IndexRequest{}
	var err error
	args.Head, err = setRequestHeadFromGin(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	indexRequest := &request.IndexRequest{}
	err = c.ShouldBindJSON(indexRequest)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
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
	args.PartitionId = indexRequest.PartitionId

	space, err := handler.docService.getSpace(c.Request.Context(), args.Head)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	if space == nil {
		err := vearchpb.NewError(vearchpb.ErrorEnum_SPACE_NOT_EXIST, nil)
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if args.PartitionId > 0 {
		found := false
		for _, partition := range space.Partitions {
			if partition.Id == args.PartitionId {
				found = true
				break
			}
		}
		if !found {
			err := vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition_id %d not belong to space %s", args.PartitionId, space.Name))
			response.New(c).JsonError(errors.NewErrBadRequest(err))
			return
		}
	}

	indexResponse := handler.docService.rebuildIndex(c.Request.Context(), args)
	result := IndexResponseToContent(indexResponse.Shards)

	response.New(c).JsonSuccess(result)
}
