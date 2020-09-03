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

package ps

import (
	"context"
	"errors"
	"fmt"
	"github.com/vearch/vearch/util/cbbytes"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/smallnest/rpcx/share"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/ps/engine/gamma"
	"github.com/vearch/vearch/ps/engine/mapping"

	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/server/rpc/handler"
	"go.uber.org/atomic"
)

type limitPlugin struct {
	size  int64
	limit *atomic.Int64
}

func (lp *limitPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	if lp.limit.Load() > lp.size {

		for _, m := range config.Conf().Masters {
			if m.Address == conn.RemoteAddr().Network() || strings.HasPrefix(conn.RemoteAddr().Network(), m.Address+":") {
				log.Info("too many routine:[%d]  but this conn is master so can conn")
				return conn, true
			}
		}

		log.Warn("too many routine:[%d] for limt so skip %s conn", lp.limit.Load(), conn.RemoteAddr().String())
		return conn, false
	}
	return conn, true
}

func ExportToRpcHandler(server *Server) {

	initHandler := &InitHandler{server: server}
	psErrorChange := psErrorChange(server)

	limitPlugin := &limitPlugin{limit: atomic.NewInt64(0), size: 50000}
	server.rpcServer.AddPlugin(limitPlugin)

	if err := server.rpcServer.RegisterName(handler.NewChain(client.UnaryHandler, handler.DefaultPanicHandler, psErrorChange, initHandler, &UnaryHandler{server: server}), ""); err != nil {
		panic(err)
	}

}

type InitHandler struct {
	server *Server
}

func (i *InitHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	if i.server.stopping.Get() {
		return vearchpb.NewError(vearchpb.ErrorEnum_SERVICE_UNAVAILABLE, nil)
	}

	return nil
}

type UnaryHandler struct {
	server *Server
}

func (handler *UnaryHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = vearchpb.NewError(vearchpb.ErrorEnum_RECOVER, errors.New(cast.ToString(r)))
			log.Error(err.Error())
		}
	}()
	if handler.server == nil {
		log.Info("%s", "ps server is nil")
	}
	reply.PartitionID = req.PartitionID
	reply.MessageID = req.MessageID
	reply.Items = req.Items
	reply.SearchRequest = req.SearchRequest
	reply.SearchResponse = req.SearchResponse
	reply.SearchRequests = req.SearchRequests
	reply.SearchResponses = req.SearchResponses
	reply.DelByQueryResponse = req.DelByQueryResponse

	store := handler.server.GetPartition(reply.PartitionID)
	if store == nil {
		msg := fmt.Sprintf("partition not found, partitionId:[%d]", reply.PartitionID)
		log.Error("%s", msg)
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, errors.New(msg))
	}
	var method string
	reqMap := ctx.Value(share.ReqMetaDataKey).(map[string]string)
	method, ok := reqMap[client.HandlerType]
	if !ok {
		err = fmt.Errorf("client type not found in matadata, key [%s]", client.HandlerType)
		return vearchpb.NewError(0, err)
	}
	switch method {
	case client.GetDocsHandler:
		getDocuments(ctx, store, reply.Items)
	case client.DeleteDocsHandler:
		deleteDocs(ctx, store, reply.Items)
	case client.BatchHandler:
		bulk(ctx, store, reply.Items)
	case client.SearchHandler:
		if reply.SearchResponse == nil {
			reply.SearchResponse = &vearchpb.SearchResponse{}
		}
		search(ctx, store, reply.SearchRequest, reply.SearchResponse)
	case client.BulkSearchHandler:
		if reply.SearchResponses == nil || len(reply.SearchResponses) == 0 {
			searchResps := make([]*vearchpb.SearchResponse, 0)
			for i := 0; i < len(reply.SearchRequests); i++ {
				searchReq := reply.SearchRequests[i]
				sortFieldMap := searchReq.SortFieldMap
				topSize := searchReq.TopN
				resp := &vearchpb.SearchResponse{SortFieldMap: sortFieldMap, TopSize: topSize}
				searchResps = append(searchResps, resp)
			}
			reply.SearchResponses = searchResps
		}
		bulkSearch(ctx, store, reply.SearchRequests, reply.SearchResponses)
	case client.ForceMergeHandler:
		farceMerge(ctx, store, reply.Err)
	case client.DeleteByQueryHandler:
		if reply.DelByQueryResponse == nil {
			reply.DelByQueryResponse = &vearchpb.DelByQueryeResponse{DelNum: 0}
		}
		deleteByQuery(ctx, store, reply.SearchRequest, reply.DelByQueryResponse)
	default:
		log.Error("method not found, method: [%s]", method)
		return vearchpb.NewError(vearchpb.ErrorEnum_METHOD_NOT_IMPLEMENT, nil)

	}
	return nil
}

func getDocuments(ctx context.Context, store PartitionStore, items []*vearchpb.Item) {
	for _, item := range items {
		if e := store.GetDocument(ctx, true, item.Doc); e != nil {
			msg := fmt.Sprintf("GetDocument failed, key: [%s], err: [%s]", item.Doc.PKey, e.Error())
			log.Error("%s", msg)
			if vearchErr, ok := e.(*vearchpb.VearchErr); ok {
				item.Err = vearchErr.GetError()
			} else {
				item.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_INTERNAL_ERROR, Msg: msg}
			}
		}
	}
}

func deleteDocs(ctx context.Context, store PartitionStore, items []*vearchpb.Item) {
	wg := sync.WaitGroup{}
	for _, item := range items {
		wg.Add(1)
		go func(item *vearchpb.Item) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					item.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_INTERNAL_ERROR, Msg: cast.ToString(r)}
				}
			}()
			if len(item.Doc.Fields) != 1 {
				msg := fmt.Sprintf("fileds of doc can only have one field--[%s] when delete", mapping.IdField)
				item.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_INTERNAL_ERROR, Msg: msg}
				return
			}
			dataBytes := item.Doc.Fields[0].Value
			docCmd := &vearchpb.DocCmd{Type: vearchpb.OpType_DELETE, Doc: dataBytes}
			if err := store.Write(ctx, docCmd); err != nil {
				log.Error("delete doc failed, err: [%s]", err.Error())
				item.Err = vearchpb.NewError(0, err).GetError()
			}
		}(item)
	}
	wg.Wait()

}

func bulk(ctx context.Context, store PartitionStore, items []*vearchpb.Item) {
	wg := sync.WaitGroup{}
	for _, item := range items {
		wg.Add(1)
		go func(item *vearchpb.Item) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					item.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_INTERNAL_ERROR, Msg: cast.ToString(r)}
				}
			}()
			docGamma := &gamma.Doc{Fields: item.Doc.Fields}
			docBytes := docGamma.Serialize()
			docCmd := &vearchpb.DocCmd{Type: vearchpb.OpType_REPLACE, Doc: docBytes}
			if err := store.Write(ctx, docCmd); err != nil {
				log.Error("Add doc failed, err: [%s]", err.Error())
				item.Err = vearchpb.NewError(0, err).GetError()
			} else {
				item.Doc.Fields = nil
			}
		}(item)
	}
	wg.Wait()
}

func search(ctx context.Context, store PartitionStore, request *vearchpb.SearchRequest, response *vearchpb.SearchResponse) {
	if err := store.Search(ctx, request, response); err != nil {
		log.Error("search doc failed, err: [%s]", err.Error())
		response.Head.Err = vearchpb.NewError(0, err).GetError()
	}
	defer func() {
		if r := recover(); r != nil {
			response.Head.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_INTERNAL_ERROR, Msg: cast.ToString(r)}
		}
	}()
}

func bulkSearch(ctx context.Context, store PartitionStore, request []*vearchpb.SearchRequest, response []*vearchpb.SearchResponse) {
	wg := sync.WaitGroup{}
	for i, req := range request {
		wg.Add(1)
		go func(req *vearchpb.SearchRequest, resp *vearchpb.SearchResponse) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					if resp.Head == nil {
						responseHead := &vearchpb.ResponseHead{Err: &vearchpb.Error{Code: vearchpb.ErrorEnum_INTERNAL_ERROR, Msg: cast.ToString(r)}}
						resp.Head = responseHead
					}
				}
			}()

			if err := store.Search(ctx, req, response[i]); err != nil {
				log.Error("search doc failed, err: [%s]", err.Error())
				response[i].Head.Err = vearchpb.NewError(0, err).GetError()
			}
		}(req, response[i])
	}
	wg.Wait()
}

func farceMerge(ctx context.Context, store PartitionStore, error *vearchpb.Error) {
	err := store.GetEngine().Optimize()
	if err != nil {
		partitionID := store.GetPartition().Id
		pIdStr := strconv.Itoa(int(partitionID))
		error = &vearchpb.Error{Code: vearchpb.ErrorEnum_FORCE_MERGE_BUILD_INDEX_ERR,
			Msg: "build index err, PartitionID :" + pIdStr}
	} else {
		error = nil
	}
}

func deleteByQuery(ctx context.Context, store PartitionStore, req *vearchpb.SearchRequest, resp *vearchpb.DelByQueryeResponse) {
	searchResponse := &vearchpb.SearchResponse{}
	if err := store.Search(ctx, req, searchResponse); err != nil {
		log.Error("deleteByQuery search doc failed, err: [%s]", err.Error())
		head := &vearchpb.ResponseHead{Err: &vearchpb.Error{Code: vearchpb.ErrorEnum_DELETE_BY_QUERY_SERACH_ERR, Msg: "deleteByQuery search doc failed"}}
		resp.Head = head
	} else {
		flatBytes := searchResponse.FlatBytes
		if flatBytes != nil {
			gamma.DeSerialize(flatBytes, searchResponse)
		}

		results := searchResponse.Results
		if results == nil || len(results) == 0 {
			head := &vearchpb.ResponseHead{Err: &vearchpb.Error{Code: vearchpb.ErrorEnum_DELETE_BY_QUERY_SEARCH_ID_IS_0, Msg: "deleteByQuery search id is 0"}}
			resp.Head = head
		} else {
			idIsLongStr := req.Head.Params["idIsLong"]

			idIsLong := false
			if idIsLongStr == "true" {
				idIsLong = true
			}
			docs := make([]*vearchpb.Item, 0)
			for _, result := range results {
				if result == nil || result.ResultItems == nil || len(result.ResultItems) == 0 {
					log.Error("query id is 0")
				} else {
					for _, doc := range result.ResultItems {
						var pKey string
						for _, fv := range doc.Fields {
							name := fv.Name
							switch name {
							case mapping.IdField:
								if idIsLong {
									id := int64(cbbytes.ByteArray2UInt64(fv.Value))
									pKey = strconv.FormatInt(id, 10)
								} else {
									pKey = string(fv.Value)
								}
							}
						}
						if pKey != "" {
							field := &vearchpb.Field{Name: "_id", Value: []byte(pKey)}
							fields := make([]*vearchpb.Field, 0)
							fields = append(fields, field)
							doc := &vearchpb.Document{PKey: pKey, Fields: fields}
							item := &vearchpb.Item{Doc: doc}
							docs = append(docs, item)
						}
					}
				}
			}
			if len(docs) == 0 {
				head := &vearchpb.ResponseHead{Err: &vearchpb.Error{Code: vearchpb.ErrorEnum_DELETE_BY_QUERY_SEARCH_ID_IS_0, Msg: "deleteByQuery search id is 0"}}
				resp.Head = head
			} else {
				deleteDocs(ctx, store, docs)
				for _, item := range docs {
					if item.Err == nil {
						resp.DelNum++
					}
				}
			}
		}
	}
}
