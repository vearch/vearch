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
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/patrickmn/go-cache"
	"github.com/shopspring/decimal"
	"github.com/smallnest/rpcx/share"
	"github.com/spaolacci/murmur3"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/internal/config"
	"github.com/vearch/vearch/internal/engine/sdk/go/gamma"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/entity/response"
	"github.com/vearch/vearch/internal/master/store"
	"github.com/vearch/vearch/internal/pkg/algorithm"
	"github.com/vearch/vearch/internal/pkg/cbbytes"
	"github.com/vearch/vearch/internal/pkg/log"
	vmap "github.com/vearch/vearch/internal/pkg/map"
	"github.com/vearch/vearch/internal/pkg/number"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/ps/engine/mapping"
	"github.com/vearch/vearch/internal/ps/engine/sortorder"
)

// Client include client of master and ps
type Client struct {
	master *masterClient
	ps     *psClient
}

// NewClient create a new client by config
func NewClient(conf *config.Config) (client *Client, err error) {
	client = &Client{}
	err = client.initPsClient()
	if err != nil {
		return nil, err
	}
	err = client.initMasterClient(conf)
	if err != nil {
		return nil, err
	}
	return client, err
}

func (client *Client) initPsClient() error {
	client.ps = &psClient{client: client}
	client.ps.initFaultylist()
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
	r.md[MessageID] = uuid.NewString()
	return r
}

// GetMsgID
func (r *routerRequest) GetMsgID() string {
	msgID, ok := r.md[MessageID]
	if ok {
		return msgID
	}
	msgID = uuid.NewString()
	r.md[MessageID] = msgID
	return msgID
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
			r.Err = vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, errors.New("the doc is nil"))
			return r
		}
		for _, field := range doc.Fields {
			if _, ok := r.space.SpaceProperties[field.Name]; !ok {
				r.Err = vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("the field[%s] in doc not needed in space", field.Name))
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
	for _, doc := range r.docs {
		key, err := generateUUID(doc.PKey)
		if err != nil {
			r.Err = err
			return r
		}
		doc.PKey = key
		field := &vearchpb.Field{Name: mapping.IdField}

		field.Value = []byte(doc.PKey)
		field.Type = vearchpb.FieldType_STRING
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

// SetDocsBySpecifyKey  return docs by a series of primary key with long type
func (r *routerRequest) SetDocsBySpecifyKey(keys []string) *routerRequest {
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
		partitionID := r.space.PartitionId(murmur3.Sum32WithSeed([]byte(doc.PKey), 0))
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

// Docs in specify partition
func (r *routerRequest) SetSendMap(partitionId uint32) *routerRequest {
	if r.Err != nil {
		return r
	}
	dataMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, doc := range r.docs {
		item := &vearchpb.Item{Doc: doc}
		if d, ok := dataMap[partitionId]; ok {
			d.Items = append(d.Items, item)
		} else {
			items := make([]*vearchpb.Item, 0)
			d = &vearchpb.PartitionData{PartitionID: partitionId, MessageID: r.GetMsgID(), Items: items}
			dataMap[partitionId] = d
			d.Items = append(d.Items, item)
		}

	}
	r.sendMap = dataMap
	return r
}

// Execute Execute request
func (r *routerRequest) Execute() []*vearchpb.Item {
	isNormal := false
	normalField := make(map[string]string)
	if r.md[HandlerType] == BatchHandler {
		indexType := r.space.Index.Type
		if indexType != "" && indexType != "BINARYIVF" {
			isNormal = true
		}

		if isNormal {
			spacePro := r.space.SpaceProperties
			for field, pro := range spacePro {
				format := pro.Format
				if pro.FieldType == vearchpb.FieldType_VECTOR && format != nil && (*format == "normalization" || *format == "normal") {
					normalField[field] = field
				}
			}
		}
	}
	var wg sync.WaitGroup

	respChain := make(chan *vearchpb.PartitionData, len(r.sendMap))
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, vmap.CopyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
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

			if isNormal && len(normalField) > 0 {
				for _, item := range d.Items {
					if item.Doc == nil {
						continue
					}
					for _, field := range item.Doc.Fields {
						if field == nil || field.Name == "" || normalField[field.Name] == "" {
							continue
						}
						float32s, err := cbbytes.ByteToVectorForFloat32(field.Value)
						if err != nil {
							log.Panic(err.Error())
						}
						if err := number.Normalization(float32s); err != nil {
							log.Panic(err.Error())
						}
						bs, err := cbbytes.VectorToByte(float32s)
						if err != nil {
							log.Error("processVector VectorToByte error: %v", err)
							log.Panic(err.Error())
						}
						field.Value = bs
					}
				}
			}
			nodeID := partition.LeaderID
			err := r.client.PS().GetOrCreateRPCClient(ctx, nodeID).Execute(ctx, UnaryHandler, d, replyPartition)
			if err != nil {
				d.Err = vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError()
				respChain <- d
			} else {
				respChain <- replyPartition
			}
		}(c, partitionID, pData)
	}
	wg.Wait()
	close(respChain)
	tmpItems := make([]*vearchpb.Item, 0)
	for resp := range respChain {
		setPartitionErr(resp)
		tmpItems = append(tmpItems, resp.Items...)
	}
	docIndexMap := make(map[string]int, len(r.docs))
	for i, doc := range r.docs {
		docIndexMap[doc.PKey] = i
	}
	items := make([]*vearchpb.Item, len(tmpItems))
	for _, item := range tmpItems {
		realIndex := docIndexMap[item.Doc.PKey]
		items[realIndex] = item
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

func str2decimalFloat(str string) decimal.Decimal {
	decimalFloat := decimal.NewFromFloat(0.0)
	if str != "" {
		vFloat, _ := strconv.ParseFloat(str, 64)
		decimalFloat = decimal.NewFromFloat(vFloat)
	}
	return decimalFloat
}

func (r *routerRequest) searchFromPartition(ctx context.Context, partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, respChain chan *response.SearchDocResult, isNormal bool, normalField map[string]string) {
	pidCacheStart := time.Now()
	responseDoc := &response.SearchDocResult{}
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("[Recover] partitionID: [%v], err: [%s]", partitionID, cast.ToString(r))
			err := &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: msg}
			head := &vearchpb.ResponseHead{Err: err}
			searchResponse := &vearchpb.SearchResponse{Head: head}
			pd.SearchResponse = searchResponse
			responseDoc.PartitionData = pd
			respChain <- responseDoc
		}
	}()

	partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, partitionID)
	if e != nil {
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "query partition cache err partitionID:" + fmt.Sprint(partitionID)}
		head := &vearchpb.ResponseHead{Err: err}
		searchResponse := &vearchpb.SearchResponse{Head: head}
		pd.SearchResponse = searchResponse
		responseDoc.PartitionData = pd
		respChain <- responseDoc
		return
	}

	replyPartition := new(vearchpb.PartitionData)
	if replyPartition.SearchResponse == nil {
		params := make(map[string]string)
		head := &vearchpb.ResponseHead{Params: params}
		replyPartition.SearchResponse = &vearchpb.SearchResponse{Head: head}
	}

	if replyPartition.SearchResponse.Head == nil {
		params := make(map[string]string)
		replyPartition.SearchResponse.Head = &vearchpb.ResponseHead{Params: params}
	}

	if replyPartition.SearchResponse.Head.Params == nil {
		params := make(map[string]string)
		replyPartition.SearchResponse.Head.Params = params
	}

	pidCacheEnd := time.Now()
	if config.LogInfoPrintSwitch {
		pidCacheTime := pidCacheEnd.Sub(pidCacheStart).Seconds() * 1000
		pidCacheTimeStr := strconv.FormatFloat(pidCacheTime, 'f', -1, 64)
		replyPartition.SearchResponse.Head.Params["pidCacheTime"] = pidCacheTimeStr
	}

	clientType := pd.SearchRequest.Head.ClientType
	// ensure node is alive
	servers := r.client.Master().Cache().serverCache

	rpcEnd, rpcStart := time.Now(), time.Now()
	nodeID := GetNodeIdsByClientType(clientType, partition, servers, r.client)

	for len(partition.Replicas) > r.client.PS().faultyList.ItemCount() {
		if r.client.PS().TestFaulty(nodeID) {
			nodeID = GetNodeIdsByClientType(clientType, partition, servers, r.client)
			continue
		}
		nodeIdEnd := time.Now()
		if config.LogInfoPrintSwitch {
			nodeIdTime := nodeIdEnd.Sub(pidCacheEnd).Seconds() * 1000
			nodeIdTimeStr := strconv.FormatFloat(nodeIdTime, 'f', -1, 64)
			replyPartition.SearchResponse.Head.Params["nodeIdTime"] = nodeIdTimeStr
		}
		rpcClient := r.client.PS().GetOrCreateRPCClient(ctx, nodeID)
		rpcClientEnd := time.Now()
		if config.LogInfoPrintSwitch {
			rpcClientTime := rpcClientEnd.Sub(nodeIdEnd).Seconds() * 1000
			rpcClientTimeStr := strconv.FormatFloat(rpcClientTime, 'f', -1, 64)
			replyPartition.SearchResponse.Head.Params["rpcClientTime"] = rpcClientTimeStr
		}
		if rpcClient == nil {
			err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "no ps client by nodeID:" + fmt.Sprint(nodeID)}
			head := &vearchpb.ResponseHead{Err: err}
			searchResponse := &vearchpb.SearchResponse{Head: head}
			pd.SearchResponse = searchResponse
			responseDoc.PartitionData = pd
			respChain <- responseDoc
			return
		}

		if isNormal && len(normalField) > 0 {
			vectorQueryArr := pd.SearchRequest.VecFields
			proMap := space.SpaceProperties
			for _, query := range vectorQueryArr {
				docField := proMap[query.Name]
				if docField != nil {
					dimension := docField.Dimension
					float32s, err := cbbytes.ByteToVectorForFloat32(query.Value)
					if err != nil {
						panic(err.Error())
					}
					max := len(float32s)
					dPage := max / dimension
					if max >= dPage {
						end := int(0)
						normalVector := make([]float32, 0, max)
						for i := 1; i <= dPage; i++ {
							qu := i * dimension
							if i != dPage {
								norma := float32s[i-1+end : qu]
								if err := number.Normalization(norma); err != nil {
									panic(err.Error())
								}
								normalVector = append(normalVector, norma...)
							} else {
								norma := float32s[i-1+end:]
								if err := number.Normalization(norma); err != nil {
									panic(err.Error())
								}
								normalVector = append(normalVector, norma...)
							}
							end = qu - i
						}
						bs, err := cbbytes.VectorToByte(normalVector)
						if err != nil {
							log.Error("processVector VectorToByte error: %v", err)
							panic(err.Error())
						}
						query.Value = bs
					}
				} else {
					float32s, err := cbbytes.ByteToVectorForFloat32(query.Value)
					if err != nil {
						panic(err.Error())
					}
					if err := number.Normalization(float32s); err != nil {
						panic(err.Error())
					}
					bs, err := cbbytes.VectorToByte(float32s)
					if err != nil {
						log.Error("processVector VectorToByte error: %v", err)
						panic(err.Error())
					}
					query.Value = bs
				}
			}
		}
		rpcStart = time.Now()
		if config.LogInfoPrintSwitch {
			normalTime := rpcStart.Sub(rpcClientEnd).Seconds() * 1000
			normalTimeStr := strconv.FormatFloat(normalTime, 'f', -1, 64)
			rpcBeforeTime := rpcStart.Sub(pidCacheStart).Seconds() * 1000
			rpcBeforeTimeStr := strconv.FormatFloat(rpcBeforeTime, 'f', -1, 64)
			replyPartition.SearchResponse.Head.Params["normalTime"] = normalTimeStr
			replyPartition.SearchResponse.Head.Params["rpcBeforeTime"] = rpcBeforeTimeStr
		}

		err := rpcClient.Execute(ctx, UnaryHandler, pd, replyPartition)
		rpcEnd = time.Now()
		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "connect: connection refused") {
			r.client.PS().AddFaulty(nodeID, time.Second*30)
			nodeID = GetNodeIdsByClientType(clientType, partition, servers, r.client)
		} else {
			log.Error("rpc err [%v], nodeID %v", err, nodeID)
			r.client.PS().AddFaulty(nodeID, time.Second*5)
			break
		}
	}

	sortFieldMap := pd.SearchRequest.SortFieldMap
	searchResponse := replyPartition.SearchResponse
	sortValueMap := make(map[string][]sortorder.SortValue)
	if searchResponse != nil {
		if config.LogInfoPrintSwitch {
			rpcCostTime := rpcEnd.Sub(rpcStart).Seconds() * 1000
			rpcCostTimeStr := strconv.FormatFloat(rpcCostTime, 'f', -1, 64)

			if searchResponse.Head.Params != nil {
				searchResponse.Head.Params["rpcCostTime"] = rpcCostTimeStr
			} else {
				costTimeMap := make(map[string]string)
				costTimeMap["rpcCostTime"] = rpcCostTimeStr
				responseHead := &vearchpb.ResponseHead{Params: costTimeMap}
				searchResponse.Head = responseHead
			}
		}

		flatBytes := searchResponse.FlatBytes
		if flatBytes != nil {
			deSerializeStartTime := time.Now()
			gamma.DeSerialize(flatBytes, searchResponse)
			deSerializeEndTime := time.Now()
			if config.LogInfoPrintSwitch {
				deSerializeCostTime := deSerializeEndTime.Sub(deSerializeStartTime).Seconds() * 1000
				deSerializeCostTimeStr := strconv.FormatFloat(deSerializeCostTime, 'f', -1, 64)
				searchResponse.Head.Params["deSerializeCostTime"] = deSerializeCostTimeStr
			}
			for i, searchResult := range searchResponse.Results {
				for _, item := range searchResult.ResultItems {
					source, sortValues, pkey, err := GetSource(item, space, sortFieldMap, pd.SearchRequest.SortFields)
					if err != nil {
						err := &vearchpb.Error{Code: vearchpb.ErrorEnum_SEARCH_RESPONSE_PARSE_ERR, Msg: "router call ps rpc service err nodeID:" + fmt.Sprint(nodeID)}
						replyPartition.SearchResponse.Head.Err = err
					}
					item.PKey = pkey
					item.Source = source
					index := strconv.Itoa(i)
					sortValueMap[item.PKey+"_"+index] = sortValues
				}
			}
			if config.LogInfoPrintSwitch {
				fieldParsingTime := time.Since(deSerializeEndTime).Seconds() * 1000
				fieldParsingTimeStr := strconv.FormatFloat(fieldParsingTime, 'f', -1, 64)
				searchResponse.Head.Params["fieldParsingTime"] = fieldParsingTimeStr
			}
		}
	}
	if config.LogInfoPrintSwitch {
		rpcTotalTime := time.Since(pidCacheStart).Seconds() * 1000
		rpcTotalTimeStr := strconv.FormatFloat(rpcTotalTime, 'f', -1, 64)
		searchResponse.Head.Params["rpcTotalTime"] = rpcTotalTimeStr
	}
	responseDoc.PartitionData = replyPartition
	responseDoc.SortValueMap = sortValueMap
	respChain <- responseDoc
}

func (r *routerRequest) SearchFieldSortExecute(sortOrder sortorder.SortOrder) *vearchpb.SearchResponse {
	startTime := time.Now()
	var wg sync.WaitGroup
	sendPartitionMap := r.sendMap
	isNormal := false
	normalField := make(map[string]string)
	indexType := r.space.Index.Type
	if indexType != "" && indexType != "BINARYIVF" {
		isNormal = true
	}
	if isNormal {
		spacePro := r.space.SpaceProperties
		for field, pro := range spacePro {
			format := pro.Format
			if pro.FieldType == vearchpb.FieldType_VECTOR && format != nil && (*format == "normalization" || *format == "normal") {
				normalField[field] = field
			}
		}
	}

	normalEndTime := time.Now()
	normalCostTimeStr := ""
	if config.LogInfoPrintSwitch {
		normalCostTime := normalEndTime.Sub(startTime).Seconds() * 1000
		normalCostTimeStr = strconv.FormatFloat(normalCostTime, 'f', -1, 64)
	}

	var searchReq *vearchpb.SearchRequest
	respChain := make(chan *response.SearchDocResult, len(sendPartitionMap))
	for partitionID, pData := range sendPartitionMap {
		searchReq = pData.SearchRequest
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, vmap.CopyMap(r.md))
		go func(ctx context.Context, partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, sortOrder sortorder.SortOrder, respChain chan *response.SearchDocResult, normalIsOrNot bool, normalField map[string]string) {
			defer wg.Done()
			r.searchFromPartition(ctx, partitionID, pd, space, respChain, normalIsOrNot, normalField)
		}(c, partitionID, pData, r.space, sortOrder, respChain, isNormal, normalField)
	}
	wg.Wait()
	close(respChain)

	partitionCostTimeStr := ""
	if config.LogInfoPrintSwitch {
		partitionCostTime := time.Since(normalEndTime).Seconds() * 1000
		partitionCostTimeStr = strconv.FormatFloat(partitionCostTime, 'f', -1, 64)
	}

	var result []*vearchpb.SearchResult
	var sortValueMap map[string][]sortorder.SortValue
	var searchResponse *vearchpb.SearchResponse

	rpcCostTime, deSerializeCostTime, fieldParsingTime, gammaCostTime, serializeCostTime, pidCacheTime, nodeIdTime, rpcClientTime, normalTime, rpcBeforeTime, rpcTotalTime := decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0), decimal.NewFromFloat(0.0)
	mergeStartTime := time.Now()
	for r := range respChain {
		if result == nil && r != nil {
			searchResponse = r.PartitionData.SearchResponse
			if config.LogInfoPrintSwitch && searchResponse != nil && searchResponse.Head != nil && searchResponse.Head.Params != nil {
				rpcCostTime = str2decimalFloat(searchResponse.Head.Params["rpcCostTime"])
				deSerializeCostTime = str2decimalFloat(searchResponse.Head.Params["deSerializeCostTime"])
				fieldParsingTime = str2decimalFloat(searchResponse.Head.Params["fieldParsingTime"])
				gammaCostTime = str2decimalFloat(searchResponse.Head.Params["gammaCostTime"])
				serializeCostTime = str2decimalFloat(searchResponse.Head.Params["serializeCostTime"])
				pidCacheTime = str2decimalFloat(searchResponse.Head.Params["pidCacheTime"])
				nodeIdTime = str2decimalFloat(searchResponse.Head.Params["nodeIdTime"])
				rpcClientTime = str2decimalFloat(searchResponse.Head.Params["rpcClientTime"])
				normalTime = str2decimalFloat(searchResponse.Head.Params["normalTime"])
				rpcBeforeTime = str2decimalFloat(searchResponse.Head.Params["rpcBeforeTime"])
				rpcTotalTime = str2decimalFloat(searchResponse.Head.Params["rpcTotalTime"])
			}
			if searchResponse != nil && searchResponse.Results != nil && len(searchResponse.Results) > 0 {
				result = searchResponse.Results
				sortValueMap = r.SortValueMap
				continue
			}
		}

		sortValue := r.SortValueMap
		for PKey, sortValue := range sortValue {
			sortValueMap[PKey] = sortValue
		}
		var err error
		if config.LogInfoPrintSwitch && searchResponse.Head != nil && searchResponse.Head.Params != nil {
			rpcCostTimeStr := searchResponse.Head.Params["rpcCostTime"]
			deSerializeCostTimeStr := searchResponse.Head.Params["deSerializeCostTime"]
			fieldParsingTimeStr := searchResponse.Head.Params["fieldParsingTime"]
			gammaCostTimeStr := searchResponse.Head.Params["gammaCostTime"]
			serializeCostTimeStr := searchResponse.Head.Params["serializeCostTime"]
			pidCacheTimeStr := searchResponse.Head.Params["pidCacheTime"]
			nodeIdTimeStr := searchResponse.Head.Params["nodeIdTime"]
			rpcClientTimeStr := searchResponse.Head.Params["rpcClientTime"]
			normalTimeStr := searchResponse.Head.Params["normalTime"]
			rpcBeforeTimeStr := searchResponse.Head.Params["rpcBeforeTime"]
			rpcTotalTimeStr := searchResponse.Head.Params["rpcTotalTime"]
			if rpcCostTimeStr != "" {
				rpcCostTime1, _ := strconv.ParseFloat(rpcCostTimeStr, 64)
				rpcCostTime2 := rpcCostTime.Add(decimal.NewFromFloat(rpcCostTime1))
				rpcCostTime = rpcCostTime2.Div(decimal.NewFromFloat(float64(2)))

			}
			if deSerializeCostTimeStr != "" {
				deSerializeCostTime1, _ := strconv.ParseFloat(deSerializeCostTimeStr, 64)
				deSerializeCostTime2 := deSerializeCostTime.Add(decimal.NewFromFloat(deSerializeCostTime1))
				deSerializeCostTime = deSerializeCostTime2.Div(decimal.NewFromFloat(float64(2)))
			}
			if fieldParsingTimeStr != "" {
				fieldParsingTime1, _ := strconv.ParseFloat(fieldParsingTimeStr, 64)
				fieldParsingTime2 := fieldParsingTime.Add(decimal.NewFromFloat(fieldParsingTime1))
				fieldParsingTime = fieldParsingTime2.Div(decimal.NewFromFloat(float64(2)))
			}
			if gammaCostTimeStr != "" {
				gammaCostTime1, _ := strconv.ParseFloat(gammaCostTimeStr, 64)
				gammaCostTime2 := gammaCostTime.Add(decimal.NewFromFloat(gammaCostTime1))
				gammaCostTime = gammaCostTime2.Div(decimal.NewFromFloat(float64(2)))
			}
			if serializeCostTimeStr != "" {
				serializeCostTime1, _ := strconv.ParseFloat(serializeCostTimeStr, 64)
				serializeCostTime2 := serializeCostTime.Add(decimal.NewFromFloat(serializeCostTime1))
				serializeCostTime = serializeCostTime2.Div(decimal.NewFromFloat(float64(2)))
			}
			if pidCacheTimeStr != "" {
				pidCacheTime1, _ := strconv.ParseFloat(pidCacheTimeStr, 64)
				pidCacheTime2 := pidCacheTime.Add(decimal.NewFromFloat(pidCacheTime1))
				pidCacheTime = pidCacheTime2.Div(decimal.NewFromFloat(float64(2)))
			}
			if nodeIdTimeStr != "" {
				nodeIdTime1, _ := strconv.ParseFloat(nodeIdTimeStr, 64)
				nodeIdTime2 := nodeIdTime.Add(decimal.NewFromFloat(nodeIdTime1))
				nodeIdTime = nodeIdTime2.Div(decimal.NewFromFloat(float64(2)))
			}
			if rpcClientTimeStr != "" {
				rpcClientTime1, _ := strconv.ParseFloat(rpcClientTimeStr, 64)
				rpcClientTime2 := rpcClientTime.Add(decimal.NewFromFloat(rpcClientTime1))
				rpcClientTime = rpcClientTime2.Div(decimal.NewFromFloat(float64(2)))
			}
			if normalTimeStr != "" {
				normalTimeF, _ := strconv.ParseFloat(normalTimeStr, 64)
				normalTime = decimal.NewFromFloat(normalTimeF)
			}
			if rpcBeforeTimeStr != "" {
				rpcBeforeTimeF, _ := strconv.ParseFloat(rpcBeforeTimeStr, 64)
				rpcBeforeTime = decimal.NewFromFloat(rpcBeforeTimeF)
			}
			if rpcTotalTimeStr != "" {
				rpcTotalTimeF, _ := strconv.ParseFloat(rpcTotalTimeStr, 64)
				rpcTotalTime = decimal.NewFromFloat(rpcTotalTimeF)
			}
		}
		if err = AddMergeResultArr(result, r.PartitionData.SearchResponse.Results); err != nil {
			log.Error("msearch AddMergeResultArr error:", err)
		}
	}

	mergeCostTimeStr := ""
	if config.LogInfoPrintSwitch {
		mergeCostTime := time.Since(mergeStartTime).Seconds() * 1000
		mergeCostTimeStr = strconv.FormatFloat(mergeCostTime, 'f', -1, 64)
	}

	if len(result) > 1 {
		var wg sync.WaitGroup
		respChain := make(chan map[string]*vearchpb.SearchResult, len(result))
		for i, resp := range result {
			wg.Add(1)
			index := strconv.Itoa(i)
			high := len(resp.ResultItems) - 1
			go func(result *vearchpb.SearchResult, sortValueMap map[string][]sortorder.SortValue, low, high int, so sortorder.SortOrder, index string) {
				quickSort(result.ResultItems, sortValueMap, low, high, so, index)
				sortMap := make(map[string]*vearchpb.SearchResult)
				sortMap[index] = result
				respChain <- sortMap
				wg.Done()
			}(resp, sortValueMap, 0, high, sortOrder, index)
		}
		wg.Wait()
		close(respChain)
		for resp := range respChain {
			for indexStr, resu := range resp {
				index, _ := strconv.Atoi(indexStr)
				result[index] = resu
			}
		}
	} else {
		for i, resp := range result {
			index := strconv.Itoa(i)
			quickSort(resp.ResultItems, sortValueMap, 0, len(resp.ResultItems)-1, sortOrder, index)
		}
	}

	for _, resp := range result {
		if resp.ResultItems != nil && len(resp.ResultItems) > 0 && searchReq.TopN > 0 {
			len := len(resp.ResultItems)
			if int32(len) > searchReq.TopN {
				resp.ResultItems = resp.ResultItems[0:searchReq.TopN]
			}
		}
	}

	if searchResponse == nil {
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_CALL_PS_RPC_ERR, Msg: "search result is null"}
		searchResponse = &vearchpb.SearchResponse{}
		responseHead := &vearchpb.ResponseHead{Err: err}
		searchResponse.Head = responseHead
	}
	sortCostTime := time.Since(mergeStartTime).Seconds() * 1000
	sortCostTimeStr := strconv.FormatFloat(sortCostTime, 'f', -1, 64)
	if config.LogInfoPrintSwitch && searchResponse.Head != nil && searchResponse.Head.Params != nil {
		searchResponse.Head.Params["mergeCostTime"] = mergeCostTimeStr
		searchResponse.Head.Params["rpcCostTime"] = rpcCostTime.String()
		searchResponse.Head.Params["deSerializeCostTime"] = deSerializeCostTime.String()
		searchResponse.Head.Params["fieldParsingTime"] = fieldParsingTime.String()
		searchResponse.Head.Params["normalCostTime"] = normalCostTimeStr
		searchResponse.Head.Params["sortCostTime"] = sortCostTimeStr
		searchResponse.Head.Params["partitionCostTime"] = partitionCostTimeStr
		executeCostTime := time.Since(startTime).Seconds() * 1000
		executeCostTimeStr := strconv.FormatFloat(executeCostTime, 'f', -1, 64)
		searchResponse.Head.Params["executeCostTime"] = executeCostTimeStr
		searchResponse.Head.Params["serializeCostTime"] = serializeCostTime.String()
		searchResponse.Head.Params["gammaCostTime"] = gammaCostTime.String()
		searchResponse.Head.Params["pidCacheTime"] = pidCacheTime.String()
		searchResponse.Head.Params["nodeIdTime"] = nodeIdTime.String()
		searchResponse.Head.Params["rpcClientTime"] = rpcClientTime.String()
		searchResponse.Head.Params["normalTime"] = normalTime.String()
		searchResponse.Head.Params["rpcBeforeTime"] = rpcBeforeTime.String()
		searchResponse.Head.Params["rpcTotalTime"] = rpcTotalTime.String()
	}
	searchResponse.Results = result
	return searchResponse
}

func (r *routerRequest) queryFromPartition(ctx context.Context, partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, respChain chan *response.SearchDocResult) {
	responseDoc := &response.SearchDocResult{}
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("[Recover] partitionID: [%v], err: [%s]", partitionID, cast.ToString(r))
			err := &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: msg}
			head := &vearchpb.ResponseHead{Err: err}
			searchResponse := &vearchpb.SearchResponse{Head: head}
			pd.SearchResponse = searchResponse
			responseDoc.PartitionData = pd
			respChain <- responseDoc
		}
	}()

	partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, partitionID)
	if e != nil {
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "query partition cache err partitionID:" + fmt.Sprint(partitionID)}
		head := &vearchpb.ResponseHead{Err: err}
		searchResponse := &vearchpb.SearchResponse{Head: head}
		pd.SearchResponse = searchResponse
		responseDoc.PartitionData = pd
		respChain <- responseDoc
		return
	}

	replyPartition := new(vearchpb.PartitionData)
	if replyPartition.SearchResponse == nil {
		params := make(map[string]string)
		head := &vearchpb.ResponseHead{Params: params}
		replyPartition.SearchResponse = &vearchpb.SearchResponse{Head: head}
	}

	if replyPartition.SearchResponse.Head == nil {
		params := make(map[string]string)
		replyPartition.SearchResponse.Head = &vearchpb.ResponseHead{Params: params}
	}

	if replyPartition.SearchResponse.Head.Params == nil {
		params := make(map[string]string)
		replyPartition.SearchResponse.Head.Params = params
	}

	clientType := pd.QueryRequest.Head.ClientType
	// ensure node is alive
	servers := r.client.Master().Cache().serverCache

	nodeID := GetNodeIdsByClientType(clientType, partition, servers, r.client)

	for len(partition.Replicas) > r.client.PS().faultyList.ItemCount() {
		if r.client.PS().TestFaulty(nodeID) {
			nodeID = GetNodeIdsByClientType(clientType, partition, servers, r.client)
			continue
		}

		rpcClient := r.client.PS().GetOrCreateRPCClient(ctx, nodeID)
		if rpcClient == nil {
			err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "no ps client by nodeID:" + fmt.Sprint(nodeID)}
			head := &vearchpb.ResponseHead{Err: err}
			searchResponse := &vearchpb.SearchResponse{Head: head}
			pd.SearchResponse = searchResponse
			responseDoc.PartitionData = pd
			respChain <- responseDoc
			return
		}

		err := rpcClient.Execute(ctx, UnaryHandler, pd, replyPartition)
		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "connect: connection refused") {
			r.client.PS().AddFaulty(nodeID, time.Second*30)
			nodeID = GetNodeIdsByClientType(clientType, partition, servers, r.client)
		} else {
			log.Error("rpc err [%v], nodeID %v", err, nodeID)
			r.client.PS().AddFaulty(nodeID, time.Second*5)
			break
		}
	}

	sortFieldMap := pd.QueryRequest.SortFieldMap
	searchResponse := replyPartition.SearchResponse
	sortValueMap := make(map[string][]sortorder.SortValue)
	if searchResponse != nil {
		flatBytes := searchResponse.FlatBytes
		if flatBytes != nil {
			gamma.DeSerialize(flatBytes, searchResponse)
			for i, searchResult := range searchResponse.Results {
				for _, item := range searchResult.ResultItems {
					source, sortValues, pkey, err := GetSource(item, space, sortFieldMap, pd.QueryRequest.SortFields)
					if err != nil {
						err := &vearchpb.Error{Code: vearchpb.ErrorEnum_QUERY_RESPONSE_PARSE_ERR, Msg: "router call ps rpc service err nodeID:" + fmt.Sprint(nodeID)}
						replyPartition.SearchResponse.Head.Err = err
					}
					item.PKey = pkey
					item.Source = source
					index := strconv.Itoa(i)
					sortValueMap[item.PKey+"_"+index] = sortValues
				}
			}
		}
	}
	responseDoc.PartitionData = replyPartition
	responseDoc.SortValueMap = sortValueMap
	respChain <- responseDoc
}

func (r *routerRequest) QueryFieldSortExecute(sortOrder sortorder.SortOrder) *vearchpb.SearchResponse {
	var wg sync.WaitGroup
	sendPartitionMap := r.sendMap

	var searchReq *vearchpb.QueryRequest
	respChain := make(chan *response.SearchDocResult, len(sendPartitionMap))
	for partitionID, pData := range sendPartitionMap {
		searchReq = pData.QueryRequest
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, vmap.CopyMap(r.md))
		go func(ctx context.Context, partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, sortOrder sortorder.SortOrder, respChain chan *response.SearchDocResult) {
			defer wg.Done()
			r.queryFromPartition(ctx, partitionID, pd, space, respChain)
		}(c, partitionID, pData, r.space, sortOrder, respChain)
	}
	wg.Wait()
	close(respChain)

	var result []*vearchpb.SearchResult
	var sortValueMap map[string][]sortorder.SortValue
	var searchResponse *vearchpb.SearchResponse

	for r := range respChain {
		if result == nil && r != nil {
			searchResponse = r.PartitionData.SearchResponse
			if searchResponse != nil && searchResponse.Results != nil && len(searchResponse.Results) > 0 {
				result = searchResponse.Results
				sortValueMap = r.SortValueMap
				continue
			}
		}

		sortValue := r.SortValueMap
		for PKey, sortValue := range sortValue {
			sortValueMap[PKey] = sortValue
		}
		var err error
		if err = AddMergeResultArr(result, r.PartitionData.SearchResponse.Results); err != nil {
			log.Error("query AddMergeResultArr error:", err)
		}
	}

	if len(result) > 1 {
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_CALL_PS_RPC_ERR, Msg: "the document_ids of query should be a one-dimensional array"}
		searchResponse = &vearchpb.SearchResponse{}
		responseHead := &vearchpb.ResponseHead{Err: err}
		searchResponse.Head = responseHead
	}

	for i, resp := range result {
		index := strconv.Itoa(i)
		quickSort(resp.ResultItems, sortValueMap, 0, len(resp.ResultItems)-1, sortOrder, index)

		if resp.ResultItems != nil && len(resp.ResultItems) > 0 && searchReq.Limit > 0 {
			len := len(resp.ResultItems)
			if int32(len) > searchReq.Limit {
				resp.ResultItems = resp.ResultItems[0:searchReq.Limit]
			}
		}
	}

	if searchResponse == nil {
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_CALL_PS_RPC_ERR, Msg: "query result is null"}
		searchResponse = &vearchpb.SearchResponse{}
		responseHead := &vearchpb.ResponseHead{Err: err}
		searchResponse.Head = responseHead
	}
	searchResponse.Results = result
	return searchResponse
}

func quickSort(items []*vearchpb.ResultItem, sortValueMap map[string][]sortorder.SortValue, low, high int, so sortorder.SortOrder, index string) {
	if low < high {
		var pivot = partition(items, sortValueMap, low, high, so, index)
		quickSort(items, sortValueMap, low, pivot, so, index)
		quickSort(items, sortValueMap, pivot+1, high, so, index)
	}
}

func partition(arr []*vearchpb.ResultItem, sortValueMap map[string][]sortorder.SortValue, low, high int, so sortorder.SortOrder, index string) int {
	var pivot = arr[low]
	var pivotSort = sortValueMap[pivot.PKey+"_"+index]
	var i = low
	var j = high
	for i < j {
		for so.Compare(sortValueMap[arr[j].PKey+"_"+index], pivotSort) >= 0 && j > low {
			j--
		}

		for so.Compare(sortValueMap[arr[i].PKey+"_"+index], pivotSort) <= 0 && i < high {
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

// GetMD5Encode return md5 value of given data
func GetMD5Encode(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func generateUUID(key string) (string, error) {
	if key == "" {
		keyUUID := uuid.NewString()
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
	}
	return key, nil
}

func (r *routerRequest) QueryByPartitions(queryReq *vearchpb.QueryRequest) *routerRequest {
	if r.Err != nil {
		return r
	}
	sendMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, partitionInfo := range r.space.Partitions {
		partitionID := partitionInfo.Id
		if d, ok := sendMap[partitionID]; ok {
			log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
		} else {
			d = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), QueryRequest: queryReq}
			sendMap[partitionID] = d
		}
	}
	r.sendMap = sendMap
	return r
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
		if _, ok := sendMap[partitionID]; ok {
			log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
		} else {
			sendMap[partitionID] = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), SearchRequests: searchReq}
		}
	}
	r.sendMap = sendMap
	return r
}

var replicaRoundRobin = algorithm.NewRoundRobin[entity.PartitionID, entity.NodeID]()

func GetNodeIdsByClientType(clientType string, partition *entity.Partition, servers *cache.Cache, client *Client) entity.NodeID {
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
				if serverExist && nodeID != partition.LeaderID {
					noLeaderIDs = append(noLeaderIDs, nodeID)
				}
			}
		}
		nodeId = replicaRoundRobin.Next(partition.Id, noLeaderIDs)
	case "random", "":
		randIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if !serverExist {
				continue
			}
			if config.Conf().Global.RaftConsistent {
				if partition.ReStatusMap[nodeID] == entity.ReplicasOK {
					randIDs = append(randIDs, nodeID)
				}
			} else {
				randIDs = append(randIDs, nodeID)
			}
		}
		nodeId = replicaRoundRobin.Next(partition.Id, randIDs)
		if log.IsDebugEnabled() {
			log.Debug("search by partition:%v by random model ID:[%d]", randIDs, nodeId)
		}
	case "least_connection":
		leastId := uint64(0)
		most := 1<<32 - 1
		least := -1
		randIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if !serverExist {
				continue
			}
			if config.Conf().Global.RaftConsistent {
				if partition.ReStatusMap[nodeID] == entity.ReplicasOK {
					randIDs = append(randIDs, nodeID)
					var ctx context.Context
					concurrent := client.PS().GetOrCreateRPCClient(ctx, nodeID).GetConcurrent()
					if concurrent > least {
						least = concurrent
					}
					if concurrent < most {
						most = concurrent
						leastId = nodeID
					}
				}
			} else {
				randIDs = append(randIDs, nodeID)
				var ctx context.Context
				concurrent := client.PS().GetOrCreateRPCClient(ctx, nodeID).GetConcurrent()
				if concurrent > least {
					least = concurrent
				}
				if concurrent < most {
					most = concurrent
					leastId = nodeID
				}
			}
		}
		if least > 10 {
			nodeId = leastId
		} else {
			nodeId = replicaRoundRobin.Next(partition.Id, randIDs)
		}
		if log.IsDebugEnabled() {
			log.Debug("search by partition:%v by least connection model ID:[%d]", randIDs, nodeId)
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
		nodeId = replicaRoundRobin.Next(partition.Id, randIDs)
		if log.IsDebugEnabled() {
			log.Debug("search by partition:%v by default model ID:[%d]", randIDs, nodeId)
		}
	}
	return nodeId
}

func GetSource(doc *vearchpb.ResultItem, space *entity.Space, sortFieldMap map[string]string, sortFields []*vearchpb.SortField) (json.RawMessage, []sortorder.SortValue, string, error) {
	source := make(map[string]interface{})
	sortValues := make([]sortorder.SortValue, len(sortFields))
	if sortFieldMap != nil && sortFieldMap["_score"] != "" {
		for i, v := range sortFields {
			if v.Field == "_score" {
				sortValues[i] = &sortorder.FloatSortValue{
					Val:      doc.Score,
					SortName: "_score",
				}
				break
			}
		}
	}
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
		spaceProperties = spacePro
	}
	var pKey string

	for _, fv := range doc.Fields {
		name := fv.Name
		switch name {
		case mapping.IdField:
			pKey = string(fv.Value)
			if sortFieldMap != nil && sortFieldMap[name] != "" {
				for i, v := range sortFields {
					if v.Field == name {
						sortValues[i] = &sortorder.StringSortValue{
							Val:      pKey,
							SortName: name,
						}
						break
					}
				}
			}
		default:
			field := spaceProperties[name]
			if field == nil {
				log.Error("can not found mappping by field:[%s]", name)
				continue
			}
			switch field.FieldType {
			case vearchpb.FieldType_STRING:
				tempValue := string(fv.Value)
				source[name] = tempValue
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					for i, v := range sortFields {
						if v.Field == name {
							sortValues[i] = &sortorder.StringSortValue{
								Val:      tempValue,
								SortName: name,
							}
							break
						}
					}
				}
			case vearchpb.FieldType_STRINGARRAY:
				tempValue := string(fv.Value)
				source[name] = strings.Split(tempValue, string([]byte{'\001'}))
			case vearchpb.FieldType_INT:
				intVal := cbbytes.Bytes2Int32(fv.Value)
				source[name] = intVal
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					for i, v := range sortFields {
						if v.Field == name {
							sortValues[i] = &sortorder.IntSortValue{
								Val:      int64(intVal),
								SortName: name,
							}
							break
						}
					}
				}
			case vearchpb.FieldType_LONG:
				longVal := cbbytes.Bytes2Int(fv.Value)
				source[name] = longVal
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					for i, v := range sortFields {
						if v.Field == name {
							sortValues[i] = &sortorder.IntSortValue{
								Val:      longVal,
								SortName: name,
							}
							break
						}
					}
				}
			case vearchpb.FieldType_BOOL:
				if cbbytes.Bytes2Int(fv.Value) == 0 {
					source[name] = false
				} else {
					source[name] = true
				}
			case vearchpb.FieldType_DATE:
				u := cbbytes.Bytes2Int(fv.Value)
				source[name] = time.Unix(u/1e6, u%1e6)
			case vearchpb.FieldType_FLOAT:
				floatVal := cbbytes.ByteToFloat64(fv.Value)
				source[name] = floatVal

				if sortFieldMap != nil && sortFieldMap[name] != "" {
					for i, v := range sortFields {
						if v.Field == name {
							sortValues[i] = &sortorder.FloatSortValue{
								Val:      floatVal,
								SortName: name,
							}
							break
						}
					}
				}
			case vearchpb.FieldType_DOUBLE:
				floatVal := cbbytes.ByteToFloat64(fv.Value)
				source[name] = floatVal
				if sortFieldMap != nil && sortFieldMap[name] != "" {
					for i, v := range sortFields {
						if v.Field == name {
							sortValues[i] = &sortorder.FloatSortValue{
								Val:      floatVal,
								SortName: name,
							}
							break
						}
					}
				}

			case vearchpb.FieldType_VECTOR:
				if space.Index.Type == "BINARYIVF" {
					featureByteC := fv.Value
					dimension := field.Dimension
					unit8s, err := cbbytes.ByteToVectorBinary(featureByteC, dimension)
					if err != nil {
						return nil, sortValues, pKey, err
					}
					source[name] = unit8s
				} else {
					float32s, err := cbbytes.ByteToVectorForFloat32(fv.Value)
					if err != nil {
						return nil, sortValues, pKey, err
					}
					source[name] = float32s
				}

			default:
				log.Warn("can not set value by type:[%v] ", field.FieldType)
			}
		}
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

func AddMergeResultArr(dest []*vearchpb.SearchResult, src []*vearchpb.SearchResult) error {
	if len(dest) != len(src) {
		log.Error("dest length:[%d] not equal src length:[%d]", len(dest), len(src))
	}

	if log.IsDebugEnabled() {
		log.Debug("dest length:[%d] , src length:[%d]", len(dest), len(src))
	}

	if len(dest) <= len(src) {
		for index := range dest {
			AddMerge(dest[index], src[index])
		}
	} else {
		for index := range src {
			AddMerge(dest[index], src[0])
		}
	}

	return nil

}

func AddMerge(sr *vearchpb.SearchResult, other *vearchpb.SearchResult) {
	Merge(sr.Status, other.Status)

	sr.TotalHits += other.TotalHits
	if other.MaxScore > sr.MaxScore {
		sr.MaxScore = other.MaxScore
	}

	if other.MaxTook > sr.MaxTook {
		sr.MaxTook = other.MaxTook
		sr.MaxTookId = other.MaxTookId
	}

	sr.Timeout = sr.Timeout && other.Timeout

	if len(sr.ResultItems) > 0 || len(other.ResultItems) > 0 {
		sr.ResultItems = append(sr.ResultItems, other.ResultItems...)
	}
}

func Merge(ss *vearchpb.SearchStatus, other *vearchpb.SearchStatus) {
	ss.Total += other.Total
	ss.Failed += other.Failed
	ss.Successful += other.Successful
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

func (r *routerRequest) CommonByPartitions() *routerRequest {
	if r.Err != nil {
		return r
	}
	sendMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, partitionInfo := range r.space.Partitions {
		partitionID := partitionInfo.Id
		if _, ok := sendMap[partitionID]; ok {
			log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
		} else {
			sendMap[partitionID] = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID()}
		}
	}
	r.sendMap = sendMap
	return r
}

func (r *routerRequest) CommonSetByPartitions(args *vearchpb.IndexRequest) *routerRequest {
	if r.Err != nil {
		return r
	}
	sendMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, partitionInfo := range r.space.Partitions {
		partitionID := partitionInfo.Id
		if _, ok := sendMap[partitionID]; ok {
			log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
		} else {
			sendMap[partitionID] = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), IndexRequest: args}
		}
	}
	r.sendMap = sendMap
	return r
}

// ForceMergeExecute Execute request
func (r *routerRequest) ForceMergeExecute() *vearchpb.ForceMergeResponse {
	// ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, vmap.CopyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
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
		}(c, partitionID, pData)
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

// RebuildIndexExecute Execute request
func (r *routerRequest) RebuildIndexExecute() *vearchpb.IndexResponse {
	// ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, vmap.CopyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
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
			responsePartition := r.ReplicaRebuildIndexExecute(partition, ctx, d, replyPartition)
			respChain <- responsePartition
		}(c, partitionID, pData)
	}
	wg.Wait()
	close(respChain)
	respShards := new(vearchpb.SearchStatus)
	respShards.Total = int32(partitionLen)
	respShards.Failed = 0
	indexResponse := &vearchpb.IndexResponse{}
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
	indexResponse.Shards = respShards
	return indexResponse
}

// FlushExecute Execute request
func (r *routerRequest) FlushExecute() *vearchpb.FlushResponse {
	// ctx := context.WithValue(r.ctx, share.ReqMetaDataKey, r.md)
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, vmap.CopyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
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
		}(c, partitionID, pData)
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

// ReplicaForceMergeExecute Execute request
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

// ReplicaRebuildIndexExecute Execute request
func (r *routerRequest) ReplicaRebuildIndexExecute(partition *entity.Partition, ctx context.Context, d *vearchpb.PartitionData, replyPartition *vearchpb.PartitionData) *vearchpb.PartitionData {
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
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, vmap.CopyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
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
		}(c, partitionID, pData)
	}
	wg.Wait()
	close(respChain)

	delByQueryResponse := &vearchpb.DelByQueryeResponse{}
	for resp := range respChain {
		if len(resp.DelByQueryResponse.IdsStr) > 0 {
			delByQueryResponse.IdsStr = append(delByQueryResponse.IdsStr, resp.DelByQueryResponse.IdsStr...)
		}
	}
	delByQueryResponse.DelNum = int32(len(delByQueryResponse.IdsStr))
	return delByQueryResponse
}
