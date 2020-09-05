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

	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/sortorder"
)

type docService struct {
	client *client.Client
}

func newDocService(client *client.Client) *docService {
	return &docService{
		client: client,
	}
}

func (docService *docService) getDocs(ctx context.Context, args *vearchpb.GetRequest) *vearchpb.GetResponse {
	reply := &vearchpb.GetResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.GetDocsHandler).SetHead(args.Head).SetSpace().SetDocsByKey(args.PrimaryKeys).PartitionDocs()
	if request.Err != nil {
		return &vearchpb.GetResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Items = items
	return reply
}

func (docService *docService) addDoc(ctx context.Context, args *vearchpb.AddRequest) *vearchpb.AddResponse {
	reply := &vearchpb.AddResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	docs := make([]*vearchpb.Document, 0)
	docs = append(docs, args.Doc)
	request.SetMsgID().SetMethod(client.BatchHandler).SetHead(args.Head).SetSpace().SetDocs(docs).SetDocsField().PartitionDocs()
	if request.Err != nil {
		return &vearchpb.AddResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	if len(items) < 1 {
		return &vearchpb.AddResponse{Head: setErrHead(request.Err)}
	}
	if items[0].Err != nil {
		reply.Head.Err = items[0].Err
	}
	reply.PrimaryKey = items[0].GetDoc().GetPKey()
	return reply
}

func (docService *docService) updateDoc(ctx context.Context, args *vearchpb.UpdateRequest) *vearchpb.UpdateResponse {
	reply := &vearchpb.UpdateResponse{Head: newOkHead()}
	docs := make([]*vearchpb.Document, 0)
	docs = append(docs, args.Doc)
	request := client.NewRouterRequest(ctx, docService.client)
	// request.SetMsgID().SetMethod(client.ReplaceDocHandler).SetHead(args.Head).SetSpace().SetDocs(docs).PartitionDocs()
	request.SetMsgID().SetMethod(client.BatchHandler).SetHead(args.Head).SetSpace().SetDocs(docs).SetDocsField().PartitionDocs()
	if request.Err != nil {
		return &vearchpb.UpdateResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	if len(items) < 1 {
		return &vearchpb.UpdateResponse{Head: setErrHead(request.Err)}
	}
	if items[0].Err != nil {
		reply.Head.Err = items[0].Err
	}
	return reply
}

func (docService *docService) deleteDocs(ctx context.Context, args *vearchpb.DeleteRequest) *vearchpb.DeleteResponse {
	reply := &vearchpb.DeleteResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.DeleteDocsHandler).SetHead(args.Head).SetSpace().SetDocsByKey(args.PrimaryKeys).SetDocsField().PartitionDocs()
	if request.Err != nil {
		return &vearchpb.DeleteResponse{Head: setErrHead(request.Err)}
	}
	items := request.Execute()
	reply.Head.Params = request.GetMD()
	reply.Items = items
	return reply
}

func (docService *docService) bulk(ctx context.Context, args *vearchpb.BulkRequest) *vearchpb.BulkResponse {
	reply := &vearchpb.BulkResponse{Head: newOkHead()}
	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.BatchHandler).SetHead(args.Head).SetSpace().SetDocs(args.Docs).SetDocsField().PartitionDocs()
	if request.Err != nil {
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

func (this *docService) getSpace(ctx context.Context, dbName string, spaceName string) (*entity.Space, error) {
	var err error

	dbID, err := this.client.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		return nil, err
	}
	space, err := this.client.Master().QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		return nil, err
	}

	return space, nil
}

func (docService *docService) search(ctx context.Context, args *vearchpb.SearchRequest) *vearchpb.SearchResponse {
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

func (docService *docService) bulkSearch(ctx context.Context, args []*vearchpb.SearchRequest) *vearchpb.SearchResponse {

	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.BulkSearchHandler).SetHead(args[0].Head).SetSpace().BulkSearchByPartitions(args)
	if request.Err != nil {
		return &vearchpb.SearchResponse{Head: setErrHead(request.Err)}
	}

	sortOrders := make([]sortorder.SortOrder, 0, len(args))
	for _, req := range args {
		sortOrder := make([]sortorder.Sort, 0, len(req.SortFields))
		for _, sortF := range req.SortFields {
			sortOrder = append(sortOrder, &sortorder.SortField{Field: sortF.Field, Desc: sortF.Type})
		}
		sortOrders = append(sortOrders, sortOrder)
	}

	searchResponse := request.BulkSearchSortExecute(sortOrders)

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

func (docService *docService) deleteByQuery(ctx context.Context, args *vearchpb.SearchRequest) *vearchpb.DelByQueryeResponse {

	request := client.NewRouterRequest(ctx, docService.client)
	request.SetMsgID().SetMethod(client.DeleteByQueryHandler).SetHead(args.Head).SetSpace().SearchByPartitions(args)
	if request.Err != nil {
		return &vearchpb.DelByQueryeResponse{Head: setErrHead(request.Err)}
	}

	delByQueryResponse := request.DelByQueryeExecute()

	if delByQueryResponse == nil {
		return &vearchpb.DelByQueryeResponse{Head: setErrHead(request.Err)}
	}
	if delByQueryResponse.Head == nil {
		delByQueryResponse.Head = newOkHead()
	}
	if delByQueryResponse.Head.Err == nil {
		delByQueryResponse.Head.Err = newOkHead().Err
	}

	return delByQueryResponse
}
