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
	"time"

	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine/sortorder"
)

// Timeout constants for different types of operations
const (
	// defaultRpcTimeOut is the default timeout for normal RPC operations (10 seconds)
	defaultRpcTimeOut int64 = 10 * 1000
	// bulkOperationTimeout is used for bulk upsert/delete operations (60 seconds)
	bulkOperationTimeout int64 = 60 * 1000
	// indexOperationTimeout is used for long-running index operations like rebuild (5 minutes)
	indexOperationTimeout int64 = 300 * 1000
	// minTimeout is the minimum allowed timeout to prevent too small values (1 second)
	minTimeout int64 = 1 * 1000
)

// docService handles document CRUD operations and routing to partition servers.
// It manages timeouts, space metadata, and request routing.
type docService struct {
	client     *client.Client
	spaceCache *SpaceCache
}

func newDocService(client *client.Client) *docService {
	ds := &docService{
		client: client,
	}
	// Initialize space cache
	ds.spaceCache = NewSpaceCache(ds)
	return ds
}

// setTimeout creates a context with timeout based on configuration and request parameters.
// Priority: request.TimeOutMs > config.Router.RpcTimeOut > defaultRpcTimeOut
// It enforces a minimum timeout of 1 second.
//
// Parameters:
//   - ctx: Parent context
//   - head: Request header containing optional timeout parameter
//
// Returns:
//   - context.Context: New context with timeout
//   - context.CancelFunc: Function to cancel the context
func setTimeout(ctx context.Context, head *vearchpb.RequestHead) (context.Context, context.CancelFunc) {
	timeout := defaultRpcTimeOut
	if config.Conf().Router.RpcTimeOut > 0 {
		timeout = int64(config.Conf().Router.RpcTimeOut)
	}
	if head.TimeOutMs > 0 {
		timeout = head.TimeOutMs
	}

	// Enforce minimum timeout to prevent too small values
	if timeout < minTimeout {
		timeout = minTimeout
	}

	t := time.Duration(timeout) * time.Millisecond
	endTime := time.Now().Add(t)
	ctx = context.WithValue(ctx, entity.RPC_TIME_OUT, endTime)
	return context.WithTimeout(ctx, t)
}

// setTimeoutForOperation sets timeout based on the type of operation being performed.
// Different operations have different default timeouts:
//   - bulk/upsert: 60 seconds
//   - index/rebuild/forcemerge: 5 minutes
//   - default: 10 seconds
//
// Configuration and request-specific timeouts can override these defaults.
//
// Parameters:
//   - ctx: Parent context
//   - head: Request header containing optional timeout parameter
//   - operationType: Type of operation ("bulk", "index", "rebuild", "forcemerge", etc.)
//
// Returns:
//   - context.Context: New context with operation-specific timeout
//   - context.CancelFunc: Function to cancel the context
func setTimeoutForOperation(ctx context.Context, head *vearchpb.RequestHead, operationType string) (context.Context, context.CancelFunc) {
	timeout := defaultRpcTimeOut

	// Set default timeout based on operation type
	switch operationType {
	case "bulk", "upsert":
		timeout = bulkOperationTimeout
	case "index", "rebuild", "forcemerge":
		timeout = indexOperationTimeout
	default:
		timeout = defaultRpcTimeOut
	}

	// Allow config to override
	if config.Conf().Router.RpcTimeOut > 0 {
		timeout = int64(config.Conf().Router.RpcTimeOut)
	}

	// Request-specific timeout has highest priority
	if head.TimeOutMs > 0 {
		timeout = head.TimeOutMs
	}

	// Enforce minimum timeout
	if timeout < minTimeout {
		timeout = minTimeout
	}

	t := time.Duration(timeout) * time.Millisecond
	endTime := time.Now().Add(t)
	ctx = context.WithValue(ctx, entity.RPC_TIME_OUT, endTime)
	return context.WithTimeout(ctx, t)
}

func (docService *docService) getDocs(ctx context.Context, args *vearchpb.GetRequest) *vearchpb.GetResponse {
	ctx, cancel := setTimeout(ctx, args.Head)
	defer cancel()

	requestID := args.Head.Params["request_id"]
	startTime := time.Now()
	log.Debugf("[%s] getDocs started, keys count: %d", requestID, len(args.PrimaryKeys))

	reply := &vearchpb.GetResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.GetDocsHandler).SetHead(args.Head).SetSpace().SetDocsByKey(args.PrimaryKeys).PartitionDocs()
	if request.Err != nil {
		log.Errorf("[%s] getDocs failed: %v, duration: %v", requestID, request.Err, time.Since(startTime))
		return &vearchpb.GetResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Items = items

	log.Debugf("[%s] getDocs completed, items: %d, duration: %v", requestID, len(items), time.Since(startTime))
	return reply
}

func (docService *docService) getDocsByPartition(ctx context.Context, args *vearchpb.GetRequest, partitionId uint32, next *bool) *vearchpb.GetResponse {
	ctx, cancel := setTimeout(ctx, args.Head)
	defer cancel()
	reply := &vearchpb.GetResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	if next != nil && *next {
		request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.GetNextDocsByPartitionHandler).SetHead(args.Head).SetSpace().SetDocsBySpecifyKey(args.PrimaryKeys).PartitionDocsById(partitionId)
	} else {
		request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.GetDocsByPartitionHandler).SetHead(args.Head).SetSpace().SetDocsBySpecifyKey(args.PrimaryKeys).PartitionDocsById(partitionId)
	}
	if request.Err != nil {
		log.Errorf("getDoc args:[%v] error: [%s]", args, request.Err)
		return &vearchpb.GetResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Head.RequestId = args.Head.Params["request_id"]
	reply.Items = items
	return reply
}

func (docService *docService) deleteDocs(ctx context.Context, args *vearchpb.DeleteRequest) *vearchpb.DeleteResponse {
	ctx, cancel := setTimeout(ctx, args.Head)
	defer cancel()

	requestID := args.Head.Params["request_id"]
	startTime := time.Now()
	log.Debugf("[%s] deleteDocs started, keys count: %d", requestID, len(args.PrimaryKeys))

	reply := &vearchpb.DeleteResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.DeleteDocsHandler).SetHead(args.Head).SetSpace().SetDocsByKey(args.PrimaryKeys).SetDocsField().PartitionDocs()
	if request.Err != nil {
		log.Errorf("[%s] deleteDocs failed: %v, duration: %v", requestID, request.Err, time.Since(startTime))
		return &vearchpb.DeleteResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Items = items

	log.Debugf("[%s] deleteDocs completed, items: %d, duration: %v", requestID, len(items), time.Since(startTime))
	return reply
}

func (docService *docService) bulk(ctx context.Context, args *vearchpb.BulkRequest) *vearchpb.BulkResponse {
	ctx, cancel := setTimeoutForOperation(ctx, args.Head, "bulk")
	defer cancel()

	requestID := args.Head.Params["request_id"]
	startTime := time.Now()
	log.Debugf("[%s] bulk started, docs count: %d", requestID, len(args.Docs))

	reply := &vearchpb.BulkResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.BatchHandler).SetHead(args.Head).SetSpace().SetDocs(args.Docs).SetDocsField().UpsertByPartitions(args.Partitions)
	if request.Err != nil {
		log.Errorf("[%s] bulk failed: %v, duration: %v", requestID, request.Err, time.Since(startTime))
		return &vearchpb.BulkResponse{Head: setErrHead(request.Err)}
	}
	reply.Items = request.Execute()
	reply.Head.Params = request.GetMD()

	log.Debugf("[%s] bulk completed, items: %d, duration: %v", requestID, len(reply.Items), time.Since(startTime))
	return reply
}

// utils
func setErrHead(err error) *vearchpb.ResponseHead {
	vErr, ok := err.(*vearchpb.VearchErr)
	if !ok {
		vErr = vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err)
	}
	return &vearchpb.ResponseHead{Err: vErr.GetError()}
}

func newOkHead() *vearchpb.ResponseHead {
	code := vearchpb.ErrorEnum_SUCCESS
	return &vearchpb.ResponseHead{Err: vearchpb.NewError(code, nil).GetError()}
}

// ensureValidHead ensures response has a valid head with error information.
// If head is nil, creates a new OK head. If head.Err is nil, sets it to OK.
//
// Parameters:
//   - head: Response head to validate
//
// Returns:
//   - *vearchpb.ResponseHead: Valid response head with error field set
func ensureValidHead(head *vearchpb.ResponseHead) *vearchpb.ResponseHead {
	if head == nil {
		head = newOkHead()
	}
	if head.Err == nil {
		head.Err = newOkHead().Err
	}
	return head
}

// getSpace retrieves space metadata using cache when available.
// It first checks for alias, then uses the space cache to reduce Master load.
//
// Parameters:
//   - ctx: Context for the request
//   - head: Request header containing db name and space name
//
// Returns:
//   - *entity.Space: The space object
//   - error: Any error encountered during lookup
func (docService *docService) getSpace(ctx context.Context, head *vearchpb.RequestHead) (*entity.Space, error) {
	// Check for alias first
	if alias, err := docService.client.Master().Cache().AliasByCache(ctx, head.SpaceName); err == nil {
		head.SpaceName = alias.SpaceName
	}
	// Use space cache
	return docService.spaceCache.GetSpace(ctx, head)
}

// getSpaceFromMaster retrieves space metadata directly from master server.
// This is called by SpaceCache on cache miss. Do not call directly - use getSpace() instead.
func (docService *docService) getSpaceFromMaster(ctx context.Context, head *vearchpb.RequestHead) (*entity.Space, error) {
	return docService.client.Master().Cache().SpaceByCache(ctx, head.DbName, head.SpaceName)
}

func (docService *docService) getUser(ctx context.Context, userName string) (*entity.User, error) {
	return docService.client.Master().Cache().UserByCache(ctx, userName)
}

func (docService *docService) getRole(ctx context.Context, roleName string) (*entity.Role, error) {
	if value, exists := entity.RoleMap[roleName]; exists {
		role := &value
		return role, nil
	}
	return docService.client.Master().Cache().RoleByCache(ctx, roleName)
}

func (docService *docService) query(ctx context.Context, args *vearchpb.QueryRequest) *vearchpb.SearchResponse {
	ctx, cancel := setTimeout(ctx, args.Head)
	defer cancel()
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.QueryHandler).SetHead(args.Head).SetSpace().QueryByPartitions(args)
	if request.Err != nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}

	searchResponse := request.QueryFieldSortExecute()
	if searchResponse == nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}
	searchResponse.Head = ensureValidHead(searchResponse.Head)
	return searchResponse
}

func (docService *docService) search(ctx context.Context, searchReq *vearchpb.SearchRequest) *vearchpb.SearchResponse {
	ctx, cancel := setTimeout(ctx, searchReq.Head)
	defer cancel()
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(searchReq.Head.Params["request_id"]).SetMethod(client.SearchHandler).SetHead(searchReq.Head).SetSpace().SearchByPartitions(searchReq)
	if request.Err != nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}

	sortOrder := make([]sortorder.Sort, 0)
	if len(searchReq.SortFields) > 0 {
		for _, sortF := range searchReq.SortFields {
			sortOrder = append(sortOrder, &sortorder.SortField{Field: sortF.Field, Desc: sortF.Type})
		}
	}

	desc := sortOrder[0].GetSortOrder()
	searchResponse := request.SearchFieldSortExecute(desc)

	if searchResponse == nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}
	searchResponse.Head = ensureValidHead(searchResponse.Head)
	return searchResponse
}

func (docService *docService) flush(ctx context.Context, args *vearchpb.FlushRequest) *vearchpb.FlushResponse {
	ctx, cancel := setTimeoutForOperation(ctx, args.Head, "index")
	defer cancel()
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.FlushHandler).SetHead(args.Head).SetSpace().CommonByPartitions(0)
	if request.Err != nil {
		return &vearchpb.FlushResponse{Head: setErrHead(request.Err)}
	}

	flushResponse := request.FlushExecute()
	if flushResponse == nil {
		return &vearchpb.FlushResponse{Head: setErrHead(request.Err)}
	}
	flushResponse.Head = ensureValidHead(flushResponse.Head)
	return flushResponse
}

func (docService *docService) forceMerge(ctx context.Context, args *vearchpb.ForceMergeRequest) *vearchpb.ForceMergeResponse {
	ctx, cancel := setTimeoutForOperation(ctx, args.Head, "forcemerge")
	defer cancel()
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.ForceMergeHandler).SetHead(args.Head).SetSpace().CommonByPartitions(args.PartitionId)
	if request.Err != nil {
		return &vearchpb.ForceMergeResponse{Head: setErrHead(request.Err)}
	}

	forceMergeResponse := request.ForceMergeExecute()
	if forceMergeResponse == nil {
		return &vearchpb.ForceMergeResponse{Head: setErrHead(request.Err)}
	}
	forceMergeResponse.Head = ensureValidHead(forceMergeResponse.Head)
	return forceMergeResponse
}

func (docService *docService) rebuildIndex(ctx context.Context, args *vearchpb.IndexRequest) *vearchpb.IndexResponse {
	ctx, cancel := setTimeoutForOperation(ctx, args.Head, "rebuild")
	defer cancel()
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.RebuildIndexHandler).SetHead(args.Head).SetSpace().CommonSetByPartitions(args)
	if request.Err != nil {
		return &vearchpb.IndexResponse{Head: setErrHead(request.Err)}
	}

	indexResponse := request.RebuildIndexExecute()
	if indexResponse == nil {
		return &vearchpb.IndexResponse{Head: setErrHead(request.Err)}
	}
	indexResponse.Head = ensureValidHead(indexResponse.Head)
	return indexResponse
}

func (docService *docService) deleteByQuery(ctx context.Context, args *vearchpb.QueryRequest) *vearchpb.DelByQueryeResponse {
	ctx, cancel := setTimeout(ctx, args.Head)
	defer cancel()
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID(args.Head.Params["request_id"]).SetMethod(client.DeleteByQueryHandler).SetHead(args.Head).SetSpace().QueryByPartitions(args)
	if request.Err != nil {
		return &vearchpb.DelByQueryeResponse{Head: setErrHead(request.Err)}
	}

	delByQueryResponse := request.DelByQueryeExecute()

	if delByQueryResponse == nil {
		return &vearchpb.DelByQueryeResponse{Head: setErrHead(request.Err)}
	}

	return delByQueryResponse
}
