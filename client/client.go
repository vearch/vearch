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

package client

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/vearch/vearch/util"

	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine/sortorder"

	"github.com/smallnest/rpcx/share"
	"github.com/spaolacci/murmur3"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/master/store"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/gamma"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/regularutil"
	"github.com/vearch/vearch/util/uuid"
)

// Client include client of master and ps
type Client struct {
	master *masterClient
	ps     *psClient
}

// NewClient create a new client by config
func NewClient(conf *config.Config) (client *Client, err error) {
	client = &Client{}
	err = client.initPsClient(conf)
	if err != nil {
		return nil, err
	}
	err = client.initMasterClient(conf)
	if err != nil {
		return nil, err
	}
	return client, err
}

func (client *Client) initPsClient(conf *config.Config) error {
	client.ps = &psClient{client: client}
	return nil
}

func (client *Client) initMasterClient(conf *config.Config) error {

	openStore, err := store.OpenStore("etcd", conf.GetEtcdAddress())
	if err != nil {
		return err
	}

	client.master = &masterClient{client: client, Store: openStore, cfg: conf}
	masterServer.init(len(conf.Masters))
	return nil
}

// Master return master client
func (client *Client) Master() *masterClient {
	return client.master
}

// PS return ps client
func (client *Client) PS() *psClient {
	return client.ps
}

// Stop stop client
func (client *Client) Stop() {
	client.master.Stop()
	client.ps.Stop()
}

// Space return space by dbname and space name
func (client *Client) Space(ctx context.Context, dbName, spaceName string) (*entity.Space, error) {
	return client.Master().Cache().SpaceByCache(ctx, dbName, spaceName)
}

const (
	// MessageID the key of message
	MessageID = "message_id"
)

// NewRouterRequest create a new request for router
func NewRouterRequest(ctx context.Context, client *Client) *routerRequest {
	return &routerRequest{ctx: ctx, client: client, md: make(map[string]string)}
}

type routerRequest struct {
	ctx     context.Context
	client  *Client
	md      map[string]string
	head    *vearchpb.RequestHead
	docs    []*vearchpb.Document
	space   *entity.Space
	sendMap map[entity.PartitionID]*vearchpb.PartitionData
	// Err if error else nil
	Err error
}

// GetMD
func (r *routerRequest) GetMD() map[string]string {
	return r.md
}

// SetMsgID
func (r *routerRequest) SetMsgID() *routerRequest {
	r.md[MessageID] = uuid.FlakeUUID()
	return r
}

// GetMsgID
func (r *routerRequest) GetMsgID() string {
	if msgID, ok := r.md[MessageID]; ok {
		return msgID
	} else {
		msgID = uuid.FlakeUUID()
		r.md[MessageID] = msgID
		return msgID
	}
}

// SetMethod set method
func (r *routerRequest) SetMethod(method string) *routerRequest {
	r.md[HandlerType] = method
	return r
}

// SetHead Set head
func (r *routerRequest) SetHead(head *vearchpb.RequestHead) *routerRequest {
	r.head = head
	return r
}

// SetSpace set space by dbName and spaceName
func (r *routerRequest) SetSpace() *routerRequest {
	if r.Err != nil {
		return r
	}
	r.space, r.Err = r.client.Space(r.ctx, r.head.DbName, r.head.SpaceName)
	return r
}

// SetDocs set docs
func (r *routerRequest) SetDocs(docs []*vearchpb.Document) *routerRequest {
	if r.Err != nil {
		return r
	}
	r.docs = docs
	for _, doc := range r.docs {
		if doc == nil {
			r.Err = vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, errors.New("The doc is nil."))
			return r
		}
		if len(doc.Fields) != len(r.space.SpaceProperties) {
			r.Err = vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("The length[%d] of doc.fields is not equal to space[%d].", len(doc.Fields), len(r.space.SpaceProperties)))
			return r
		}
		for _, field := range doc.Fields {
			if _, ok := r.space.SpaceProperties[field.Name]; !ok {
				r.Err = vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("The field[%s] in doc not needed in space.", field.Name))
				return r
			}
		}
	}
	return r
}

// SetDocsField Set _id field into doc
func (r *routerRequest) SetDocsField() *routerRequest {
	if r.Err != nil {
		return r
	}
	IDIsLong := idIsLong(r.space)
	for _, doc := range r.docs {
		key, err := generateUUID(doc.PKey, IDIsLong)
		if err != nil {
			r.Err = err
			return r
		}
		doc.PKey = key
		field := &vearchpb.Field{Name: mapping.IdField}
		if IDIsLong {
			keyInt, _ := strconv.ParseInt(doc.PKey, 10, 64)
			field.Value, _ = cbbytes.ValueToByte(keyInt)
			field.Type = vearchpb.FieldType_LONG
		} else {
			field.Value = []byte(doc.PKey)
			field.Type = vearchpb.FieldType_STRING
		}
		doc.Fields = append(doc.Fields, field)
	}
	return r
}

// SetDocsByKey  return docs by a series of primary key
func (r *routerRequest) SetDocsByKey(keys []string) *routerRequest {
	if r.Err != nil {
		return r
	}
	r.docs, r.Err = setDocs(keys)
	return r
}

// PartitionDocs split docs into different partition
func (r *routerRequest) PartitionDocs() *routerRequest {
	if r.Err != nil {
		return r
	}
	dataMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, doc := range r.docs {
		partitionID := r.space.PartitionId(murmur3.Sum32WithSeed(cbbytes.StringToByte(doc.PKey), 0))
		item := &vearchpb.Item{Doc: doc}
		if d, ok := dataMap[partitionID]; ok {
			d.Items = append(d.Items, item)
		} else {
			items := make([]*vearchpb.Item, 0)
			d = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), Items: items}
			dataMap[partitionID] = d
			d.Items = append(d.Items, item)
		}

	}
	r.sendMap = dataMap
	return r
}

// Execute Execute request
func (r *routerRequest) Execute() []*vearchpb.Item {
	ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	normalIsOrNot := false
	normalField := make(map[string]string)
	if r.md[HandlerType] == BatchHandler {
		retrievalType := r.space.Engine.RetrievalType
		if retrievalType != "" && strings.Compare(retrievalType, "BINARYIVF") != 0 {
			normalIsOrNot = true
		}
		if normalIsOrNot {
			spacePro := r.space.SpaceProperties
			for field, pro := range spacePro {
				format := pro.Format
				if pro.FieldType == entity.FieldType_VECTOR && format != nil &&
					(strings.Compare(*format, "normalization") == 0 ||
						strings.Compare(*format, "normal") == 0) {
					normalField[field] = field
				}
			}
		}
	}
	var wg sync.WaitGroup
	respChain := make(chan *vearchpb.PartitionData, len(r.sendMap))
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		go func(pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%s]", pid, cast.ToString(r))}
					respChain <- d
				}
			}()
			partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, pid)
			if e != nil {
				panic(e.Error())
			}
			nodeID := partition.LeaderID

			if normalIsOrNot && len(normalField) > 0 {
				for _, item := range d.Items {
					if item.Doc != nil {
						for _, field := range item.Doc.Fields {
							if field != nil && field.Name != "" && normalField[field.Name] != "" {
								float32s, _, err := cbbytes.ByteToVectorForFloat32(field.Value)
								if err == nil {
									if err := util.Normalization(float32s); err != nil {
										panic(err.Error())
									} else {
										bs, err := cbbytes.VectorToByte(float32s, "")
										if err != nil {
											log.Error("processVector VectorToByte error: %v", err)
											panic(err.Error())
										} else {
											field.Value = bs
										}
									}
								} else {
									panic(err.Error())
								}
							}
						}
					}
				}
			}
			err := r.client.PS().GetOrCreateRPCClient(ctx, nodeID).Execute(ctx, UnaryHandler, d, replyPartition)
			if err != nil {
				replyPartition.Err = vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError()
			} else {
				respChain <- replyPartition
			}
		}(partitionID, pData)
	}
	wg.Wait()
	close(respChain)
	items := make([]*vearchpb.Item, 0)
	for resp := range respChain {
		setPartitionErr(resp)
		items = append(items, resp.Items...)
	}
	return items
}

func setPartitionErr(d *vearchpb.PartitionData) {
	if d.Err != nil && d.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		for _, item := range d.Items {
			item.Err = d.Err
		}
	}
}

func (r *routerRequest) SearchFieldSortExecute(sortOrder sortorder.SortOrder) *vearchpb.SearchResponse {
	ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	var wg sync.WaitGroup
	sendPartitionMap := r.sendMap
	normalIsOrNot := false
	normalField := make(map[string]string)
	retrievalType := r.space.Engine.RetrievalType
	if retrievalType != "" && strings.Compare(retrievalType, "BINARYIVF") != 0 {
		normalIsOrNot = true
	}
	if normalIsOrNot {
		spacePro := r.space.SpaceProperties
		for field, pro := range spacePro {
			format := pro.Format
			if pro.FieldType == entity.FieldType_VECTOR && format != nil &&
				(strings.Compare(*format, "normalization") == 0 ||
					strings.Compare(*format, "normal") == 0) {
				normalField[field] = field
			}
		}
	}

	var searchReq *vearchpb.SearchRequest
	respChain := make(chan *response.SearchDocResult, len(sendPartitionMap))
	for partitionID, pData := range sendPartitionMap {
		searchReq = pData.SearchRequest
		wg.Add(1)
		go func(partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, sortOrder sortorder.SortOrder) {
			defer wg.Done()
			responseDoc := &response.SearchDocResult{}
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					msg := fmt.Sprintf("[Recover] partitionID: [%v], err: [%s]", partitionID, cast.ToString(r))
					err := &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: msg}
					head := &vearchpb.ResponseHead{Err: err}
					searchResponse := &vearchpb.SearchResponse{Head: head}
					pd.SearchResponse = searchResponse
					responseDoc.PartitionData = pd
					safeSend(respChain, responseDoc)
				}
			}()
			partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, partitionID)
			if e != nil {
				err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "query partition cache err partitionID:" + string(partitionID)}
				head := &vearchpb.ResponseHead{Err: err}
				searchResponse := &vearchpb.SearchResponse{Head: head}
				pd.SearchResponse = searchResponse
				responseDoc.PartitionData = pd
				safeSend(respChain, responseDoc)
				return
			}
			clientType := pd.SearchRequest.Head.ClientType
			//ensure node is alive
			servers := r.client.Master().Cache().serverCache
			nodeID := GetNodeIdsByClientType(clientType, partition, servers)
			rpcClient := r.client.PS().GetOrCreateRPCClient(ctx, nodeID)
			if rpcClient == nil {
				err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "no ps clinet by nodeID:" + string(nodeID)}
				head := &vearchpb.ResponseHead{Err: err}
				searchResponse := &vearchpb.SearchResponse{Head: head}
				pd.SearchResponse = searchResponse
				responseDoc.PartitionData = pd
				safeSend(respChain, responseDoc)
				return
			}

			if normalIsOrNot && len(normalField) > 0 {
				vectorQueryArr := pd.SearchRequest.VecFields
				for _, query := range vectorQueryArr {
					float32s, _, err := cbbytes.ByteToVectorForFloat32(query.Value)
					if err == nil {
						if err := util.Normalization(float32s); err != nil {
							panic(err.Error())
						} else {
							bs, err := cbbytes.VectorToByte(float32s, "")
							if err != nil {
								log.Error("processVector VectorToByte error: %v", err)
								panic(err.Error())
							} else {
								query.Value = bs
							}
						}
					} else {
						panic(err.Error())
					}
				}
			}

			err := rpcClient.Execute(ctx, UnaryHandler, pd, replyPartition)
			if err != nil {
				err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_CALL_PS_RPC_ERR, Msg: "router call ps rpc service err nodeID:" + string(nodeID)}
				head := &vearchpb.ResponseHead{Err: err}
				searchResponse := &vearchpb.SearchResponse{Head: head}
				pd.SearchResponse = searchResponse
				responseDoc.PartitionData = pd
				safeSend(respChain, responseDoc)
				return
			}

			sortFieldMap := pd.SearchRequest.SortFieldMap
			isIsLong := false
			if idIsLong(space) {
				isIsLong = true
			}
			searchResponse := replyPartition.SearchResponse
			sortValueMap := make(map[string][]sortorder.SortValue)
			if searchResponse != nil {
				flatBytes := searchResponse.FlatBytes
				if flatBytes != nil {
					gamma.DeSerialize(flatBytes, searchResponse)
					searchResults := searchResponse.Results
					if searchResults != nil && len(searchResults) > 0 {
						for _, searchResult := range searchResults {
							searchItems := searchResult.ResultItems
							for _, item := range searchItems {
								source, sortValues, pkey, err := GetSource(item, space, isIsLong, sortFieldMap)
								if err != nil {
									err := &vearchpb.Error{Code: vearchpb.ErrorEnum_PARSING_RESULT_ERROR, Msg: "router call ps rpc service err nodeID:" + string(nodeID)}
									replyPartition.SearchResponse.Head.Err = err
								}
								item.PKey = pkey
								item.Source = source
								sortValueMap[item.PKey] = sortValues
							}
							if sortFieldMap != nil && len(sortFieldMap) > 0 {
								quickSort(searchItems, sortValueMap, 0, len(searchItems)-1, sortOrder)
							}
						}
					}
				}
			}
			responseDoc.PartitionData = replyPartition
			responseDoc.SortValueMap = sortValueMap
			safeSend(respChain, responseDoc)
		}(partitionID, pData, r.space, sortOrder)
	}
	//wg.Wait()
	//close(respChain)
	if waitTimeout(&wg, time.Millisecond*time.Duration(config.PSRpcTimeOut)) {
		close(respChain)
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: "more than 800 Millisecond"}
		params := make(map[string]string)
		head := &vearchpb.ResponseHead{Err: err, Params: params}
		response := vearchpb.SearchResponse{Head: head}
		log.Error("rpc cost time error:%v", err)
		return &response
	} else {
		close(respChain)
	}

	var firstResult []*vearchpb.SearchResult
	var sortValueMap map[string][]sortorder.SortValue
	var searchResponse *vearchpb.SearchResponse

	for resp := range respChain {
		if firstResult == nil {
			if resp != nil {
				searchResponse = resp.PartitionData.SearchResponse
				if searchResponse != nil && searchResponse.Results != nil && len(searchResponse.Results) > 0 {
					firstResult = searchResponse.Results
					sortValueMap = resp.SortValueMap
					continue
				}
			}
		} else {
			if resp != nil {
				searchResponse = resp.PartitionData.SearchResponse
				sortValue := resp.SortValueMap
				for PKey, sortValue := range sortValue {
					sortValueMap[PKey] = sortValue
				}
				if searchResponse.Results != nil && len(searchResponse.Results) > 0 {
					result := searchResponse.Results
					err := MergeArrForField(firstResult, result, sortValueMap, sortOrder, searchReq.TopN)
					if err != nil {
						log.Error("msearch merge error:", err)
					}
				}
			}
		}
	}

	searchResponse.Results = firstResult
	return searchResponse
}

func safeSend(ch chan *response.SearchDocResult, value *response.SearchDocResult) (closed bool) {
	defer func() {
		if recover() != nil {
			closed = true
		}
	}()

	ch <- value  // panic if ch is closed
	return false // <=> closed = false; return
}

func isClosed(ch <-chan *response.SearchDocResult) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	ch := make(chan bool)

	go time.AfterFunc(timeout, func() {
		ch <- true
	})

	go func() {
		wg.Wait()
		ch <- false
	}()

	return <-ch
}

func (r *routerRequest) BulkSearchSortExecute(sortOrders []sortorder.SortOrder) *vearchpb.SearchResponse {
	ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	var wg sync.WaitGroup
	sendPartitionMap := r.sendMap
	normalIsOrNot := false
	normalField := make(map[string]string)
	retrievalType := r.space.Engine.RetrievalType
	if retrievalType != "" && strings.Compare(retrievalType, "BINARYIVF") != 0 {
		normalIsOrNot = true
	}
	if normalIsOrNot {
		spacePro := r.space.SpaceProperties
		for field, pro := range spacePro {
			format := pro.Format
			if pro.FieldType == entity.FieldType_VECTOR && format != nil &&
				(strings.Compare(*format, "normalization") == 0 ||
					strings.Compare(*format, "normal") == 0) {
				normalField[field] = field
			}
		}
	}
	respChain := make(chan *response.SearchDocResult, len(sendPartitionMap))
	for partitionID, pData := range sendPartitionMap {
		wg.Add(1)
		go func(partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, sortorders []sortorder.SortOrder) {
			defer wg.Done()
			responseDoc := &response.SearchDocResult{}
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					msg := fmt.Sprintf("[Recover] partitionID: [%v], err: [%s]", partitionID, cast.ToString(r))
					err := &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: msg}
					if pd.SearchResponse == nil {
						pd.SearchResponse = &vearchpb.SearchResponse{}
						responseHead := &vearchpb.ResponseHead{Err: err}
						pd.SearchResponse.Head = responseHead
					}
					responseDoc.PartitionData = pd
					respChain <- responseDoc
				}
			}()
			partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, partitionID)
			if e != nil {
				log.Error("BulkSearchSortExecute PartitionByCache error:%v", r)
			}
			clientType := pd.SearchRequests[0].Head.ClientType
			servers := r.client.Master().Cache().serverCache
			nodeID := GetNodeIdsByClientType(clientType, partition, servers)
			if normalIsOrNot && len(normalField) > 0 {
				for i := 0; i < len(pd.SearchRequests); i++ {
					vectorQueryArr := pd.SearchRequests[i].VecFields
					for _, query := range vectorQueryArr {
						float32s, _, err := cbbytes.ByteToVectorForFloat32(query.Value)
						if err == nil {
							if err := util.Normalization(float32s); err != nil {
								panic(err.Error())
							} else {
								bs, err := cbbytes.VectorToByte(float32s, "")
								if err != nil {
									log.Error("processVector VectorToByte error: %v", err)
									panic(err.Error())
								} else {
									query.Value = bs
								}
							}
						} else {
							panic(err.Error())
						}
					}
				}
			}
			rpcClient := r.client.PS().GetOrCreateRPCClient(ctx, nodeID)
			if rpcClient == nil {
				err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "no ps clinet by nodeID:" + string(nodeID)}
				if pd.SearchResponse == nil {
					pd.SearchResponse = &vearchpb.SearchResponse{}
					responseHead := &vearchpb.ResponseHead{Err: err}
					pd.SearchResponse.Head = responseHead
				}
				responseDoc.PartitionData = pd
				respChain <- responseDoc
				return
			}
			if pd.SearchResponses == nil || len(pd.SearchResponses) == 0 {
				searchResps := make([]*vearchpb.SearchResponse, 0)
				for i := 0; i < len(pd.SearchRequests); i++ {
					searchReq := pd.SearchRequests[i]
					sortFieldMap := searchReq.SortFieldMap
					topSize := searchReq.TopN
					resp := &vearchpb.SearchResponse{SortFieldMap: sortFieldMap, TopSize: topSize}
					searchResps = append(searchResps, resp)
				}
				pd.SearchResponses = searchResps
			}

			err := rpcClient.Execute(ctx, UnaryHandler, pd, replyPartition)
			if err != nil {
				//panic(err)
				err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_CALL_PS_RPC_ERR, Msg: "router call ps rpc service err nodeID:" + string(nodeID)}
				if pd.SearchResponse == nil {
					pd.SearchResponse = &vearchpb.SearchResponse{}
					responseHead := &vearchpb.ResponseHead{Err: err}
					pd.SearchResponse.Head = responseHead
				}
				replyPartition.SearchResponse = pd.SearchResponse
			}

			isIsLong := false
			if idIsLong(space) {
				isIsLong = true
			}
			searchResponses := replyPartition.SearchResponses
			sResponse := &vearchpb.SearchResponse{}

			searchRespLenth := len(searchResponses)
			sortValueMap := make(map[string][]sortorder.SortValue)
			topSizes := make([]int32, 0, searchRespLenth)
			for i := 0; i < searchRespLenth; i++ {
				searchResp := searchResponses[i]
				topSizes = append(topSizes, searchResp.TopSize)
				if searchResp != nil && searchResp.FlatBytes != nil && len(searchResp.FlatBytes) > 0 {
					sortFieldMap := searchResp.SortFieldMap
					gamma.DeSerialize(searchResp.FlatBytes, searchResp)
					searchResults := searchResp.Results
					if searchResults != nil && len(searchResults) > 0 {
						for i, searchResult := range searchResults {
							searchItems := searchResult.ResultItems
							for _, item := range searchItems {
								source, sortValues, pkey, err := GetSource(item, space, isIsLong, sortFieldMap)
								if err != nil {
									err := &vearchpb.Error{Code: vearchpb.ErrorEnum_PARSING_RESULT_ERROR, Msg: "router call ps rpc service err nodeID:" + string(nodeID)}
									replyPartition.SearchResponse.Head.Err = err
								}
								item.PKey = pkey
								item.Source = source
								sortValueMap[item.PKey] = sortValues
							}
							if sortFieldMap != nil && len(sortFieldMap) > 0 {
								quickSort(searchItems, sortValueMap, 0, len(searchItems)-1, sortorders[i])
							}
						}
						sResponse.Results = append(sResponse.Results, searchResults...)
					}
				}
			}
			replyPartition.SearchResponse = sResponse
			responseDoc.PartitionData = replyPartition
			responseDoc.SortValueMap = sortValueMap
			responseDoc.TopSizes = topSizes
			respChain <- responseDoc
		}(partitionID, pData, r.space, sortOrders)
	}
	wg.Wait()
	close(respChain)

	var firstResult []*vearchpb.SearchResult
	var sortValueMap map[string][]sortorder.SortValue
	var searchResponse *vearchpb.SearchResponse

	i := 0
	for resp := range respChain {
		if firstResult == nil {
			if resp != nil && resp.PartitionData != nil {
				searchResponse = resp.PartitionData.SearchResponse
				if searchResponse != nil && searchResponse.Results != nil && len(searchResponse.Results) > 0 {
					firstResult = searchResponse.Results
					sortValueMap = resp.SortValueMap
					continue
				}
			}
		} else {
			if resp != nil {
				topSizes := resp.TopSizes
				searchResponse = resp.PartitionData.SearchResponse
				sortValue := resp.SortValueMap
				for PKey, sortValue := range sortValue {
					sortValueMap[PKey] = sortValue
				}
				if searchResponse.Results != nil && len(searchResponse.Results) > 0 {
					result := searchResponse.Results
					err := BulkMergeArrForField(firstResult, result, sortValueMap, sortOrders, topSizes)
					if err != nil {
						log.Error("BulkSearch merge error:", err)
					}
				}
			}
		}
		i++
	}

	searchResponse.Results = firstResult
	return searchResponse
}

func quickSort(items []*vearchpb.ResultItem, sortValueMap map[string][]sortorder.SortValue, low, high int, so sortorder.SortOrder) {
	if low < high {
		var pivot = partition(items, sortValueMap, low, high, so)
		quickSort(items, sortValueMap, low, pivot, so)
		quickSort(items, sortValueMap, pivot+1, high, so)
	}
}
func partition(arr []*vearchpb.ResultItem, sortValueMap map[string][]sortorder.SortValue, low, high int, so sortorder.SortOrder) int {
	var pivot = arr[low]
	var pivotSort = sortValueMap[pivot.PKey]
	var i = low
	var j = high
	for i < j {
		for so.Compare(sortValueMap[arr[j].PKey], pivotSort) >= 0 && j > low {
			j--
		}
		for so.Compare(sortValueMap[arr[i].PKey], pivotSort) <= 0 && i < high {
			i++
		}
		if i < j {
			arr[i], arr[j] = arr[j], arr[i]
		}
	}
	arr[low], arr[j] = arr[j], pivot
	return j
}

func setDocs(keys []string) (docs []*vearchpb.Document, err error) {
	docs = make([]*vearchpb.Document, 0)
	for _, key := range keys {
		if key == "" {
			return nil, errors.New("key can not be null")
		}
		docs = append(docs, &vearchpb.Document{PKey: key})
	}
	return docs, nil
}

func idIsLong(space *entity.Space) bool {
	idIsLong := false
	idType := space.Engine.IdType
	if strings.EqualFold("long", idType) {
		idIsLong = true
	}
	return idIsLong
}

// GetMD5Encode return md5 value of given data
func GetMD5Encode(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func generateUUID(key string, idIsLong bool) (string, error) {
	if key == "" {
		keyUUID := uuid.FlakeUUID()
		keyMd5 := GetMD5Encode(keyUUID)
		bi := big.NewInt(0)
		before := keyMd5[0:16]
		after := keyMd5[16:32]

		bi.SetString(before, 16)
		beforeInt64 := bi.Int64()
		bi.SetString(after, 16)
		afterInt64 := bi.Int64()

		key64 := beforeInt64 ^ afterInt64
		key = strconv.FormatInt(key64, 10)
	} else {
		if idIsLong {
			result := regularutil.StringCheckNum(key)
			if !result {
				msg := fmt.Errorf("key must be a number, [%s] convert failed", key)
				err := vearchpb.NewError(vearchpb.ErrorEnum_Primary_IS_INVALID, msg)
				return "", err
			}
		}
	}
	return key, nil
}

func (r *routerRequest) SearchByPartitions(searchReq *vearchpb.SearchRequest) *routerRequest {
	if r.Err != nil {
		return r
	}
	sendMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, partitionInfo := range r.space.Partitions {
		partitionID := partitionInfo.Id
		if d, ok := sendMap[partitionID]; ok {
			log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
		} else {
			d = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), SearchRequest: searchReq}
			sendMap[partitionID] = d
		}

	}
	r.sendMap = sendMap
	return r
}

func (r *routerRequest) BulkSearchByPartitions(searchReq []*vearchpb.SearchRequest) *routerRequest {
	if r.Err != nil {
		return r
	}
	sendMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, partitionInfo := range r.space.Partitions {
		partitionID := partitionInfo.Id
		if d, ok := sendMap[partitionID]; ok {
			log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
		} else {
			d = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), SearchRequests: searchReq}
			sendMap[partitionID] = d
		}

	}
	r.sendMap = sendMap
	return r
}

func GetNodeIdsByClientType(clientType string, partition *entity.Partition, servers *cache.Cache) entity.NodeID {
	nodeId := uint64(0)
	switch clientType {
	case "leader":
		nodeId = partition.LeaderID
	case "not_leader":
		if log.IsDebugEnabled() {
			log.Debug("search by partition:%v by not leader model by partition:[%d]", partition.Id)
		}
		noLeaderIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if config.Conf().Global.RaftConsistent {
				if serverExist && partition.ReStatusMap[nodeID] == entity.ReplicasOK && nodeID != partition.LeaderID {
					noLeaderIDs = append(noLeaderIDs, nodeID)
				}
			} else {
				if serverExist {
					noLeaderIDs = append(noLeaderIDs, nodeID)
				}
			}
		}
		nodeId = noLeaderIDs[rand.Intn(len(noLeaderIDs))]
	case "random", "":
		randIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if config.Conf().Global.RaftConsistent {
				if serverExist && partition.ReStatusMap[nodeID] == entity.ReplicasOK {
					randIDs = append(randIDs, nodeID)
				}
			} else {
				if serverExist {
					randIDs = append(randIDs, nodeID)
				}
			}
		}
		nodeId = randIDs[rand.Intn(len(randIDs))]
		if log.IsDebugEnabled() {
			log.Debug("search by partition:%v by random model ID:[%d]", randIDs, nodeId)
		}
	default:
		randIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if config.Conf().Global.RaftConsistent {
				if serverExist && partition.ReStatusMap[nodeID] == entity.ReplicasOK {
					randIDs = append(randIDs, nodeID)
				}
			} else {
				if serverExist {
					randIDs = append(randIDs, nodeID)
				}
			}
		}
		nodeId = randIDs[rand.Intn(len(randIDs))]
		if log.IsDebugEnabled() {
			log.Debug("search by partition:%v by default model ID:[%d]", randIDs, nodeId)
		}
	}
	return nodeId
}

func GetSource(doc *vearchpb.ResultItem, space *entity.Space, idIsLong bool, sortFieldMap map[string]string) (json.RawMessage, []sortorder.SortValue, string, error) {
	source := make(map[string]interface{})
	sortValues := make([]sortorder.SortValue, 0)
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		spaceProperties = spacePro
	}
	var pKey string

	for _, fv := range doc.Fields {
		name := fv.Name
		switch name {
		case mapping.IdField:
			if idIsLong {
				id := int64(cbbytes.ByteArray2UInt64(fv.Value))
				pKey = strconv.FormatInt(id, 10)
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					sortValues = append(sortValues, &sortorder.IntSortValue{
						Val:      id,
						SortName: name,
					})
				}
			} else {
				pKey = string(fv.Value)
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					sortValues = append(sortValues, &sortorder.StringSortValue{
						Val:      pKey,
						SortName: name,
					})
				}
			}
		default:
			field := spaceProperties[name]
			if field == nil {
				log.Error("can not found mappping by field:[%s]", name)
				continue
			}
			switch field.FieldType {
			case entity.FieldType_STRING:
				tempValue := string(fv.Value)
				if field.Array {
					source[name] = strings.Split(tempValue, string([]byte{'\001'}))
				} else {
					source[name] = tempValue
					if sortFieldMap != nil && sortFieldMap[name] != "" {
						sortValues = append(sortValues, &sortorder.StringSortValue{
							Val:      tempValue,
							SortName: name,
						})
					}
				}
			case entity.FieldType_INT:
				intVal := cbbytes.Bytes2Int32(fv.Value)
				source[name] = intVal
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					sortValues = append(sortValues, &sortorder.IntSortValue{
						Val:      int64(intVal),
						SortName: name,
					})
				}
			case entity.FieldType_LONG:
				longVal := cbbytes.Bytes2Int(fv.Value)
				source[name] = longVal
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					sortValues = append(sortValues, &sortorder.IntSortValue{
						Val:      longVal,
						SortName: name,
					})
				}
			case entity.FieldType_BOOL:
				if cbbytes.Bytes2Int(fv.Value) == 0 {
					source[name] = false
				} else {
					source[name] = true
				}
			case entity.FieldType_DATE:
				u := cbbytes.Bytes2Int(fv.Value)
				source[name] = time.Unix(u/1e6, u%1e6)
			case entity.FieldType_FLOAT:
				floatVal := cbbytes.ByteToFloat64(fv.Value)
				source[name] = floatVal
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					sortValues = append(sortValues, &sortorder.FloatSortValue{
						Val:      floatVal,
						SortName: name,
					})
				}
			case entity.FieldType_DOUBLE:
				floatVal := cbbytes.ByteToFloat64(fv.Value)
				source[name] = floatVal
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					sortValues = append(sortValues, &sortorder.FloatSortValue{
						Val:      floatVal,
						SortName: name,
					})
				}
			case entity.FieldType_VECTOR:
				if strings.Compare(space.Engine.RetrievalType, "BINARYIVF") == 0 {
					featureByteC := fv.Value
					dimension := field.Dimension
					if dimension != 0 {
						unit8s, uri, err := cbbytes.ByteToVectorBinary(featureByteC, dimension)
						if err != nil {
							return nil, sortValues, pKey, err
						}
						source[name] = map[string]interface{}{
							"source":  uri,
							"feature": unit8s,
						}
					} else {
						log.Error("GetSource can not found dimension by field:[%s]", name)
					}
				} else {
					float32s, uri, err := cbbytes.ByteToVector(fv.Value)
					if err != nil {
						return nil, sortValues, pKey, err
					}
					source[name] = map[string]interface{}{
						"source":  uri,
						"feature": float32s,
					}
				}

			default:
				log.Warn("can not set value by type:[%v] ", field.FieldType)
			}
		}
	}

	if len(sortValues) == 0 {
		sortValues = append(sortValues, &sortorder.FloatSortValue{
			Val:      doc.Score,
			SortName: "_score",
		})
	}

	var marshal []byte
	var err error
	if len(source) > 0 {
		marshal, err = json.Marshal(source)
	}
	if err != nil {
		return nil, sortValues, pKey, err
	}
	return marshal, sortValues, pKey, nil
}

func MergeArrForField(dest []*vearchpb.SearchResult, src []*vearchpb.SearchResult, firstSortValue map[string][]sortorder.SortValue, so sortorder.SortOrder, size int32) error {

	if len(dest) != len(src) {
		log.Error("dest length:[%d] not equal src length:[%d]", len(dest), len(src))
	}

	if log.IsDebugEnabled() {
		log.Debug("dest length:[%d] , src length:[%d]", len(dest), len(src))
	}

	if len(dest) <= len(src) {
		for index := range dest {
			err := MergeForField(dest[index], src[index], firstSortValue, so, 0, size)
			if err != nil {
				return fmt.Errorf("merge err [%s]")
			}
		}
	} else {
		for index := range src {
			err := MergeForField(dest[index], src[0], firstSortValue, so, 0, size)
			if err != nil {
				return fmt.Errorf("merge err [%s]")
			}
		}
	}

	return nil

}

func BulkMergeArrForField(dest []*vearchpb.SearchResult, src []*vearchpb.SearchResult, firstSortValue map[string][]sortorder.SortValue, soArr []sortorder.SortOrder, sizes []int32) error {

	if len(dest) != len(src) {
		log.Error("dest length:[%d] not equal src length:[%d]", len(dest), len(src))
	}

	if log.IsDebugEnabled() {
		log.Debug("dest length:[%d] , src length:[%d]", len(dest), len(src))
	}

	if len(dest) <= len(src) {
		for index := range dest {
			err := MergeForField(dest[index], src[index], firstSortValue, soArr[index], 0, sizes[index])
			if err != nil {
				return fmt.Errorf("merge err [%s]")
			}
		}
	} else {
		for index := range src {
			err := MergeForField(dest[index], src[0], firstSortValue, soArr[0], 0, sizes[index])
			if err != nil {
				return fmt.Errorf("merge err [%s]")
			}
		}
	}

	return nil

}

func MergeForField(old *vearchpb.SearchResult, other *vearchpb.SearchResult, firstSortValue map[string][]sortorder.SortValue, sortOrder sortorder.SortOrder, from, size int32) (err error) {

	old.Status = SearchStatusMerge(old.Status, other.Status)

	old.TotalHits += other.TotalHits
	if other.MaxScore > old.MaxScore {
		old.MaxScore = other.MaxScore
	}

	if other.MaxTook > old.MaxTook {
		old.MaxTook = other.MaxTook
		old.MaxTookId = other.MaxTookId
	}

	old.Timeout = old.Timeout && other.Timeout

	if len(old.ResultItems) > 0 || len(other.ResultItems) > 0 {
		old.ResultItems = HitsMergeForField(old.ResultItems, firstSortValue, old.PID, other.PID, other.ResultItems, sortOrder, from, size)
	}

	if other.Explain != nil {
		if old.Explain == nil {
			old.Explain = other.Explain //impossibility
		}

		for k, v := range other.Explain {
			old.Explain[k] = v
		}
	}

	return
}

func SearchStatusMerge(old *vearchpb.SearchStatus, other *vearchpb.SearchStatus) *vearchpb.SearchStatus {
	old.Total += other.Total
	old.Failed += other.Failed
	old.Successful += other.Successful
	if other.Msg != "" {
		old.Msg = other.Msg
	}
	return old
}

func HitsMergeForField(dh []*vearchpb.ResultItem, firstSortValue map[string][]sortorder.SortValue, spid, fpid uint32, sh []*vearchpb.ResultItem, sortOrder sortorder.SortOrder, from, size int32) []*vearchpb.ResultItem {

	result := make([]*vearchpb.ResultItem, 0, int(math.Min(float64(len(sh)+len(dh)), float64(from+size))))

	var d, s, c int

	var dd, sd *vearchpb.ResultItem

	for i := 0; i < int(size); i++ {
		dd = nextHits(dh, d)
		sd = nextHits(sh, s)

		if dd == nil && sd == nil {
			break
		}

		if dd == nil {
			s++
			result = append(result, sd)
		} else if sd == nil {
			d++
			result = append(result, dd)
		} else {
			c = sortOrder.Compare(firstSortValue[dd.PKey], firstSortValue[sd.PKey])

			if c == 0 {
				if spid > fpid { // if compare is same , so sort by partition id
					c = -1
				} else {
					c = 1
				}
			}

			if c < 0 {
				d++
				result = append(result, dd)
			} else {
				s++
				result = append(result, sd)
			}
		}
	}

	return result
}

func Compare(old float64, new float64) int {
	c := old - new
	switch {
	case c > 0:
		return 1
	case c < 0:
		return -1
	default:
		return 0
	}
}

func nextHits(dh []*vearchpb.ResultItem, i int) *vearchpb.ResultItem {
	if len(dh) <= i {
		return nil
	}

	return dh[i]
}

func (r *routerRequest) CommonByPartitions() *routerRequest {
	if r.Err != nil {
		return r
	}
	sendMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, partitionInfo := range r.space.Partitions {
		partitionID := partitionInfo.Id
		if d, ok := sendMap[partitionID]; ok {
			log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
		} else {
			d = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID()}
			sendMap[partitionID] = d
		}
	}
	r.sendMap = sendMap
	return r
}

// ForceMergeExecute Execute request
func (r *routerRequest) ForceMergeExecute() *vearchpb.ForceMergeResponse {
	ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		go func(pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%s]", pid, cast.ToString(r))}
					respChain <- d
				}
			}()
			partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, pid)
			if e != nil {
				panic(e.Error())
			}
			responsePartition := r.ReplicaForceMergeExecute(partition, ctx, d, replyPartition)
			respChain <- responsePartition
		}(partitionID, pData)
	}
	wg.Wait()
	close(respChain)
	respShards := new(vearchpb.SearchStatus)
	respShards.Total = int32(partitionLen)
	respShards.Failed = 0
	forceMergeResponse := &vearchpb.ForceMergeResponse{}
	var errMsg strings.Builder
	for resp := range respChain {
		if resp.Err == nil {
			respShards.Successful++
		} else {
			respShards.Failed++
			errMsg.WriteString(resp.Err.Msg)
		}
	}
	respShards.Msg = errMsg.String()
	forceMergeResponse.Shards = respShards
	return forceMergeResponse
}

// FlushExecute Execute request
func (r *routerRequest) FlushExecute() *vearchpb.FlushResponse {
	ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		go func(pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%s]", pid, cast.ToString(r))}
					respChain <- d
				}
			}()
			partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, pid)
			if e != nil {
				panic(e.Error())
			}
			responsePartition := r.LeaderFlushExecute(partition, ctx, d, replyPartition)
			respChain <- responsePartition
		}(partitionID, pData)
	}
	wg.Wait()
	close(respChain)
	respShards := new(vearchpb.SearchStatus)
	respShards.Total = int32(partitionLen)
	respShards.Failed = 0
	flushResponse := &vearchpb.FlushResponse{}
	var errMsg strings.Builder
	for resp := range respChain {
		if resp.Err == nil {
			respShards.Successful++
		} else {
			respShards.Failed++
			errMsg.WriteString(resp.Err.Msg)
		}
	}
	respShards.Msg = errMsg.String()
	flushResponse.Shards = respShards
	return flushResponse
}

// replicaForceMergeExecute Execute request
func (r *routerRequest) ReplicaForceMergeExecute(partition *entity.Partition, ctx context.Context, d *vearchpb.PartitionData, replyPartition *vearchpb.PartitionData) *vearchpb.PartitionData {
	var wgOther sync.WaitGroup
	nodeIds := partition.Replicas
	replicaNum := len(nodeIds)
	respChain := make(chan *vearchpb.PartitionData, replicaNum)
	senderResp := new(vearchpb.PartitionData)
	for i := 0; i < replicaNum; i++ {
		wgOther.Add(1)
		go func(nodeId entity.NodeID) {
			defer wgOther.Done()
			err := r.client.PS().GetOrCreateRPCClient(ctx, nodeId).Execute(ctx, UnaryHandler, d, replyPartition)
			if err != nil {
				replyPartition.Err = vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError()
			} else {
				respChain <- replyPartition
			}
		}(nodeIds[i])
	}
	wgOther.Wait()
	close(respChain)

	for res := range respChain {
		if res.Err != nil {
			senderResp.Err = res.Err
			break
		}
		senderResp.Err = res.Err
	}

	return senderResp
}

// leaderFlushExecute Execute request
func (r *routerRequest) LeaderFlushExecute(partition *entity.Partition, ctx context.Context, d *vearchpb.PartitionData, replyPartition *vearchpb.PartitionData) *vearchpb.PartitionData {
	leaderId := partition.LeaderID
	err := r.client.PS().GetOrCreateRPCClient(ctx, leaderId).Execute(ctx, UnaryHandler, d, replyPartition)
	if err != nil {
		replyPartition.Err = vearchpb.NewError(0, err).GetError()
	}
	return replyPartition
}

// DelByQueryeExecute Execute request
func (r *routerRequest) DelByQueryeExecute() *vearchpb.DelByQueryeResponse {
	ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		go func(pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%s]", pid, cast.ToString(r))}
					respChain <- d
				}
			}()
			partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, pid)
			if e != nil {
				panic(e.Error())
			}
			nodeID := partition.LeaderID
			err := r.client.PS().GetOrCreateRPCClient(ctx, nodeID).Execute(ctx, UnaryHandler, d, replyPartition)
			if err != nil {
				replyPartition.Err = vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError()
			} else {
				respChain <- replyPartition
			}
		}(partitionID, pData)
	}
	wg.Wait()
	close(respChain)

	var delNum int32
	for resp := range respChain {
		delNum = delNum + resp.DelByQueryResponse.DelNum
	}
	delByQueryResponse := &vearchpb.DelByQueryeResponse{DelNum: delNum}
	return delByQueryResponse
}
