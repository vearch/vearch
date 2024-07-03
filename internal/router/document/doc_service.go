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

const defaultRpcTimeOut int64 = 10 * 1000 // 10 second

type docService struct {
	client *client.Client
}

func newDocService(client *client.Client) *docService {
	return &docService{
		client: client,
	}
}

func setTimeOut(ctx context.Context, head *vearchpb.RequestHead) (context.Context, context.CancelFunc) {
	timeout := defaultRpcTimeOut
	if config.Conf().Router.RpcTimeOut > 0 {
		timeout = int64(config.Conf().Router.RpcTimeOut)
	}
	if head.TimeOutMs > 0 {
		timeout = head.TimeOutMs
	}
	t := time.Duration(timeout) * time.Millisecond
	endTime := time.Now().Add(t)
	ctx = context.WithValue(ctx, entity.RPC_TIME_OUT, endTime)
	return context.WithTimeout(ctx, t)
}

func (docService *docService) getDocs(ctx context.Context, args *vearchpb.GetRequest) *vearchpb.GetResponse {
	ctx, cancel := setTimeOut(ctx, args.Head)
	defer cancel()
	reply := &vearchpb.GetResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.GetDocsHandler).SetHead(args.Head).SetSpace().SetDocsByKey(args.PrimaryKeys).PartitionDocs()
	if request.Err != nil {
		log.Errorf("getDoc args:[%v] error: [%s]", args, request.Err)
		return &vearchpb.GetResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Items = items
	return reply
}

func (docService *docService) getDocsByPartition(ctx context.Context, args *vearchpb.GetRequest, partitionId uint32, next *bool) *vearchpb.GetResponse {
	ctx, cancel := setTimeOut(ctx, args.Head)
	defer cancel()
	reply := &vearchpb.GetResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	if next != nil && *next {
		request.SetMsgID().SetMethod(client.GetNextDocsByPartitionHandler).SetHead(args.Head).SetSpace().SetDocsBySpecifyKey(args.PrimaryKeys).SetSendMap(partitionId)
	} else {
		request.SetMsgID().SetMethod(client.GetDocsByPartitionHandler).SetHead(args.Head).SetSpace().SetDocsBySpecifyKey(args.PrimaryKeys).SetSendMap(partitionId)
	}
	if request.Err != nil {
		log.Errorf("getDoc args:[%v] error: [%s]", args, request.Err)
		return &vearchpb.GetResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Items = items
	return reply
}

func (docService *docService) deleteDocs(ctx context.Context, args *vearchpb.DeleteRequest) *vearchpb.DeleteResponse {
	ctx, cancel := setTimeOut(ctx, args.Head)
	defer cancel()
	reply := &vearchpb.DeleteResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.DeleteDocsHandler).SetHead(args.Head).SetSpace().SetDocsByKey(args.PrimaryKeys).SetDocsField().PartitionDocs()
	if request.Err != nil {
		log.Errorf("delete args:[%v] error: [%s]", args, request.Err)
		return &vearchpb.DeleteResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Items = items
	return reply
}

func (docService *docService) bulk(ctx context.Context, args *vearchpb.BulkRequest) *vearchpb.BulkResponse {
	ctx, cancel := setTimeOut(ctx, args.Head)
	defer cancel()
	reply := &vearchpb.BulkResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.BatchHandler).SetHead(args.Head).SetSpace().SetDocs(args.Docs).SetDocsField().UpsertByPartitions(args.Partitions)
	if request.Err != nil {
		log.Errorf("bulk args:[%v] error: [%s]", args, request.Err)
		return &vearchpb.BulkResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Items = items
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

func (docService *docService) getSpace(ctx context.Context, head *vearchpb.RequestHead) (*entity.Space, error) {
	if alias, err := docService.client.Master().Cache().AliasByCache(ctx, head.SpaceName); err == nil {
		head.SpaceName = alias.SpaceName
	}
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
	ctx, cancel := setTimeOut(ctx, args.Head)
	defer cancel()
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.QueryHandler).SetHead(args.Head).SetSpace().QueryByPartitions(args)
	if request.Err != nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}

	sortOrder := make([]sortorder.Sort, 0)
	if args.SortFields != nil && len(args.SortFields) > 0 {
		for _, sortF := range args.SortFields {
			sortOrder = append(sortOrder, &sortorder.SortField{Field: sortF.Field, Desc: sortF.Type})
		}
	}
	searchResponse := request.QueryFieldSortExecute(sortOrder)

	if searchResponse == nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}
	if searchResponse.Head == nil {
		searchResponse.Head = newOkHead()
	}
	if searchResponse.Head.Err == nil {
		searchResponse.Head.Err = newOkHead().Err
	}

	return searchResponse
}

func (docService *docService) search(ctx context.Context, args *vearchpb.SearchRequest) *vearchpb.SearchResponse {
	ctx, cancel := setTimeOut(ctx, args.Head)
	defer cancel()
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.SearchHandler).SetHead(args.Head).SetSpace().SearchByPartitions(args)
	if request.Err != nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}

	sortOrder := make([]sortorder.Sort, 0)
	if args.SortFields != nil && len(args.SortFields) > 0 {
		for _, sortF := range args.SortFields {
			sortOrder = append(sortOrder, &sortorder.SortField{Field: sortF.Field, Desc: sortF.Type})
		}
	}
	searchResponse := request.SearchFieldSortExecute(sortOrder)

	if searchResponse == nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}
	if searchResponse.Head == nil {
		searchResponse.Head = newOkHead()
	}
	if searchResponse.Head.Err == nil {
		searchResponse.Head.Err = newOkHead().Err
	}

	return searchResponse
}

func (docService *docService) flush(ctx context.Context, args *vearchpb.FlushRequest) *vearchpb.FlushResponse {
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.FlushHandler).SetHead(args.Head).SetSpace().CommonByPartitions()
	if request.Err != nil {
		return &vearchpb.FlushResponse{Head: setErrHead(request.Err)}
	}

	flushResponse := request.FlushExecute()

	if flushResponse == nil {
		return &vearchpb.FlushResponse{Head: setErrHead(request.Err)}
	}
	if flushResponse.Head == nil {
		flushResponse.Head = newOkHead()
	}
	if flushResponse.Head.Err == nil {
		flushResponse.Head.Err = newOkHead().Err
	}

	return flushResponse
}

func (docService *docService) forceMerge(ctx context.Context, args *vearchpb.ForceMergeRequest) *vearchpb.ForceMergeResponse {
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.ForceMergeHandler).SetHead(args.Head).SetSpace().CommonByPartitions()
	if request.Err != nil {
		return &vearchpb.ForceMergeResponse{Head: setErrHead(request.Err)}
	}

	forceMergeResponse := request.ForceMergeExecute()

	if forceMergeResponse == nil {
		return &vearchpb.ForceMergeResponse{Head: setErrHead(request.Err)}
	}
	if forceMergeResponse.Head == nil {
		forceMergeResponse.Head = newOkHead()
	}
	if forceMergeResponse.Head.Err == nil {
		forceMergeResponse.Head.Err = newOkHead().Err
	}

	return forceMergeResponse
}

func (docService *docService) rebuildIndex(ctx context.Context, args *vearchpb.IndexRequest) *vearchpb.IndexResponse {
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.RebuildIndexHandler).SetHead(args.Head).SetSpace().CommonSetByPartitions(args)
	if request.Err != nil {
		return &vearchpb.IndexResponse{Head: setErrHead(request.Err)}
	}

	indexResponse := request.RebuildIndexExecute()

	if indexResponse == nil {
		return &vearchpb.IndexResponse{Head: setErrHead(request.Err)}
	}
	if indexResponse.Head == nil {
		indexResponse.Head = newOkHead()
	}
	if indexResponse.Head.Err == nil {
		indexResponse.Head.Err = newOkHead().Err
	}

	return indexResponse
}

func (docService *docService) deleteByQuery(ctx context.Context, args *vearchpb.QueryRequest) *vearchpb.DelByQueryeResponse {
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.DeleteByQueryHandler).SetHead(args.Head).SetSpace().QueryByPartitions(args)
	if request.Err != nil {
		return &vearchpb.DelByQueryeResponse{Head: setErrHead(request.Err)}
	}

	delByQueryResponse := request.DelByQueryeExecute()

	if delByQueryResponse == nil {
		return &vearchpb.DelByQueryeResponse{Head: setErrHead(request.Err)}
	}

	return delByQueryResponse
}
