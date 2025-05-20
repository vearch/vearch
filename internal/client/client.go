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
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/patrickmn/go-cache"
	"github.com/smallnest/rpcx/share"
	"github.com/spaolacci/murmur3"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/engine/sdk/go/gamma"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/entity/request"
	"github.com/vearch/vearch/v3/internal/entity/response"
	"github.com/vearch/vearch/v3/internal/master/store"
	"github.com/vearch/vearch/v3/internal/pkg/atomic"
	"github.com/vearch/vearch/v3/internal/pkg/cbbytes"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/number"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
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
	etcdStore, err := store.OpenStore("etcd", conf.GetEtcdAddress())
	if err != nil {
		return err
	}

	client.master = &masterClient{client: client, Store: etcdStore, cfg: conf}
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
func (r *routerRequest) SetMsgID(requstId string) *routerRequest {
	r.md[MessageID] = requstId
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
		field := &vearchpb.Field{Name: entity.IdField}

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

// UpsertByPartitions split docs by specify partitons
func (r *routerRequest) UpsertByPartitions(partitions []uint32) *routerRequest {
	if r.Err != nil {
		return r
	}
	partitionID := uint32(0)
	dataMap := make(map[entity.PartitionID]*vearchpb.PartitionData)
	// partition by specify rule
	if r.space.PartitionRule != nil {
		for _, doc := range r.docs {
			found := false
			for _, field := range doc.Fields {
				if field.Name == r.space.PartitionRule.Field {
					found = true
					pids, err := r.space.PartitionIdsByRangeField(field.Value, field.Type)
					if err != nil {
						r.Err = err
						return r
					}
					if len(pids) == 1 {
						partitionID = pids[0]
					} else {
						hash_index := murmur3.Sum32WithSeed([]byte(doc.PKey), 0) % uint32(len(pids))
						partitionID = pids[hash_index]
					}
					break
				}
			}
			if !found {
				r.Err = vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("document must have partition rule field"))
				return r
			}

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
	} else {
		// partition by hash or specify pids
		for _, doc := range r.docs {
			if len(partitions) == 0 {
				partitionID = r.space.PartitionId(murmur3.Sum32WithSeed([]byte(doc.PKey), 0))
			} else {
				hash_index := murmur3.Sum32WithSeed([]byte(doc.PKey), 0) % uint32(len(partitions))
				partitionID = partitions[hash_index]
			}
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
	}

	r.sendMap = dataMap
	return r
}

func copyMap(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
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
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, copyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%v]", pid, r)}
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
				for _, nodeID := range partition.Replicas {
					if nodeID == 0 {
						continue
					}
					if r.client.PS().TestFaulty(nodeID) {
						continue
					}
					replyPartition = new(vearchpb.PartitionData)
					err = r.client.PS().GetOrCreateRPCClient(ctx, nodeID).Execute(ctx, UnaryHandler, d, replyPartition)
					if err == nil {
						break
					}
				}
				if err != nil {
					d.Err = vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError()
					respChain <- d
				} else {
					d.Err = nil
					respChain <- replyPartition
				}
			} else {
				respChain <- replyPartition
			}
		}(c, partitionID, pData)
	}
	wg.Wait()
	close(respChain)

	docIndexMap := make(map[string][]int, len(r.docs))
	for i, doc := range r.docs {
		docIndexMap[doc.PKey] = append(docIndexMap[doc.PKey], i)
	}

	items := make([]*vearchpb.Item, len(r.docs))
	for resp := range respChain {
		setPartitionErr(resp)
		for _, item := range resp.Items {
			if item == nil || item.Doc == nil {
				log.Error("item or doc is nil")
				continue
			}
			realIndex := docIndexMap[item.Doc.PKey]
			if len(realIndex) == 1 {
				items[realIndex[0]] = item
			} else {
				for _, index := range realIndex {
					items[index] = item
				}
			}
		}
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

func (r *routerRequest) replicasFaultyNum(replicas []uint64) int {
	faultyNodeNum := 0
	for _, nodeID := range replicas {
		if r.client.PS().TestFaulty(nodeID) {
			faultyNodeNum++
		}
	}
	return faultyNodeNum
}

func (r *routerRequest) searchFromPartition(ctx context.Context, partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, respChain chan *response.SearchDocResult, isNormal bool, normalField map[string]string, desc bool) {
	start := time.Now()
	responseDoc := &response.SearchDocResult{}
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("searchFromPartition partitionID: [%v], err: [%v]", partitionID, r)
			err := &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: msg}
			head := &vearchpb.ResponseHead{Err: err}
			pd.SearchResponse = &vearchpb.SearchResponse{Head: head}
			responseDoc.PartitionData = pd
			respChain <- responseDoc
		}
	}()

	trace := config.Trace
	if trace_info, ok := pd.SearchRequest.Head.Params["trace"]; ok {
		if trace_info == "true" {
			trace = true
		}
	}

	var partitionIDstr string
	if trace {
		partitionIDstr = strconv.FormatUint(uint64(partitionID), 10)
	}

	partition, e := r.client.Master().Cache().PartitionByCache(ctx, r.space.Name, partitionID)
	if e != nil {
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "query partition cache err partitionID:" + fmt.Sprint(partitionID)}
		pd.SearchResponse = &vearchpb.SearchResponse{Head: &vearchpb.ResponseHead{Err: err, RequestId: pd.MessageID}}
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

	getPartitionEnd := time.Now()
	if trace {
		getPartitionTime := getPartitionEnd.Sub(start).Seconds() * 1000
		getPartitionTimeStr := strconv.FormatFloat(getPartitionTime, 'f', 4, 64)
		replyPartition.SearchResponse.Head.Params["getPartition_"+partitionIDstr] = getPartitionTimeStr
	}

	clientType := pd.SearchRequest.Head.ClientType
	// ensure node is alive
	serverCache := r.client.Master().Cache().serverCache

	rpcEnd, rpcStart := time.Now(), time.Now()
	nodeID := GetNodeIdsByClientType(clientType, partition, serverCache, r.client)

	faultyNodeNum := r.replicasFaultyNum(partition.Replicas)
	retryTime := 0
	var retry_err error
	if len(partition.Replicas) <= faultyNodeNum || nodeID == 0 {
		msg := fmt.Sprintf("nodeID %v partitionID: %d is faulty, replica_num=%d, faultyNodeNum=%d", nodeID, partitionID, len(partition.Replicas), faultyNodeNum)
		log.Error(msg)
		retry_err = fmt.Errorf(msg)
	}
	for len(partition.Replicas) > faultyNodeNum && retryTime < len(partition.Replicas) {
		nodeIdEnd := time.Now()
		if trace {
			getNodeIdTime := nodeIdEnd.Sub(getPartitionEnd).Seconds() * 1000
			getNodeIdTimeStr := strconv.FormatFloat(getNodeIdTime, 'f', 4, 64)
			replyPartition.SearchResponse.Head.Params["getNodeId_"+partitionIDstr] = getNodeIdTimeStr
		}
		rpcClient := r.client.PS().GetOrCreateRPCClient(ctx, nodeID)
		rpcClientEnd := time.Now()
		if trace {
			getRpcClientTime := rpcClientEnd.Sub(nodeIdEnd).Seconds() * 1000
			getRpcClientTimeStr := strconv.FormatFloat(getRpcClientTime, 'f', 4, 64)
			replyPartition.SearchResponse.Head.Params["getRpcClient_"+partitionIDstr] = getRpcClientTimeStr
		}
		if rpcClient == nil {
			err := &vearchpb.Error{Code: vearchpb.ErrorEnum_ROUTER_NO_PS_CLIENT, Msg: "no ps client by nodeID:" + fmt.Sprint(nodeID)}
			pd.SearchResponse = &vearchpb.SearchResponse{Head: &vearchpb.ResponseHead{Err: err}}
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

			if trace {
				normalFieldTime := rpcStart.Sub(rpcClientEnd).Seconds() * 1000
				normalFieldTimeStr := strconv.FormatFloat(normalFieldTime, 'f', 4, 64)
				replyPartition.SearchResponse.Head.Params["normalField_"+partitionIDstr] = normalFieldTimeStr
			}
		}
		rpcStart = time.Now()
		retry_err = rpcClient.Execute(ctx, UnaryHandler, pd, replyPartition)
		rpcEnd = time.Now()
		if retry_err == nil {
			break
		}

		log.Error("nodeID %v partitionID: %d rpc err [%v], retryTime: %d, len(partition.Replicas)=%d, faultyNodeNum: %d", nodeID, partitionID, retry_err, retryTime, len(partition.Replicas), faultyNodeNum)
		if strings.Contains(retry_err.Error(), "connect: connection refused") {
			r.client.PS().AddFaulty(nodeID, time.Second*30)
		} else {
			break
		}
		nodeID = GetNodeIdsByClientType(clientType, partition, serverCache, r.client)

		faultyNodeNum = r.replicasFaultyNum(partition.Replicas)
		retryTime++
	}

	if retry_err != nil {
		var err *vearchpb.Error
		if vErr, ok := retry_err.(*vearchpb.VearchErr); ok {
			err = &vearchpb.Error{Code: vErr.GetError().Code, Msg: retry_err.Error()}
		} else {
			err = &vearchpb.Error{Code: vearchpb.ErrorEnum_INTERNAL_ERROR, Msg: retry_err.Error()}
		}
		pd.SearchResponse = &vearchpb.SearchResponse{Head: &vearchpb.ResponseHead{Err: err}}
		responseDoc.PartitionData = pd
		respChain <- responseDoc
		return
	}

	searchResponse := replyPartition.SearchResponse
	if searchResponse != nil {
		if trace {
			rpcExecute := rpcEnd.Sub(rpcStart).Seconds() * 1000
			rpcExecuteStr := strconv.FormatFloat(rpcExecute, 'f', 4, 64)

			if searchResponse.Head.Params != nil {
				searchResponse.Head.Params["rpcExecute_"+partitionIDstr] = rpcExecuteStr
			} else {
				costTimeMap := make(map[string]string)
				costTimeMap["rpcExecute_"+partitionIDstr] = rpcExecuteStr
				responseHead := &vearchpb.ResponseHead{Params: costTimeMap}
				searchResponse.Head = responseHead
			}
		}

		flatBytes := searchResponse.FlatBytes
		if flatBytes != nil {
			deSerializeStartTime := time.Now()
			sr := &vearchpb.SearchResponse{}
			gamma.DeSerialize(flatBytes, sr)
			searchResponse.Results = sr.Results
			deSerializeEndTime := time.Now()
			if trace {
				deSerialize := deSerializeEndTime.Sub(deSerializeStartTime).Seconds() * 1000
				deSerializeStr := strconv.FormatFloat(deSerialize, 'f', 4, 64)
				searchResponse.Head.Params["deSerialize_"+partitionIDstr] = deSerializeStr
			}
		}
	}
	if trace {
		searchFromPartition := time.Since(start).Seconds() * 1000
		searchFromPartitionStr := strconv.FormatFloat(searchFromPartition, 'f', 4, 64)
		searchResponse.Head.Params["searchFromPartition_"+partitionIDstr] = searchFromPartitionStr
	}
	responseDoc.PartitionData = replyPartition
	responseDoc.Desc = desc
	respChain <- responseDoc
}

func (r *routerRequest) SearchFieldSortExecute(desc bool) *vearchpb.SearchResponse {
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

	trace := config.Trace
	for _, pData := range sendPartitionMap {
		if pData.SearchRequest == nil {
			continue
		}
		if bTrace, ok := pData.SearchRequest.Head.Params["trace"]; ok {
			if bTrace == "true" {
				trace = true
				break
			}
		}
	}

	startSearchPartitonsTime := time.Now()
	var searchReq *vearchpb.SearchRequest
	respChain := make(chan *response.SearchDocResult, len(sendPartitionMap))
	for partitionID, pData := range sendPartitionMap {
		searchReq = pData.SearchRequest
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, copyMap(r.md))
		go func(ctx context.Context, partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, respChain chan *response.SearchDocResult, isNormal bool, normalField map[string]string) {
			defer wg.Done()
			r.searchFromPartition(ctx, partitionID, pd, space, respChain, isNormal, normalField, desc)
		}(c, partitionID, pData, r.space, respChain, isNormal, normalField)
	}
	wg.Wait()
	close(respChain)

	searchPartitionsStr := ""
	if trace {
		searchPartitions := time.Since(startSearchPartitonsTime).Seconds() * 1000
		searchPartitionsStr = strconv.FormatFloat(searchPartitions, 'f', 4, 64)
	}

	var result []*vearchpb.SearchResult
	var searchResponse *vearchpb.SearchResponse

	mergeStartTime := time.Now()
	var finalErr *vearchpb.Error
	for r := range respChain {
		if r != nil && r.PartitionData.Err != nil {
			finalErr = r.PartitionData.Err
			continue
		}
		if result == nil && r != nil {
			searchResponse = r.PartitionData.SearchResponse
			if searchResponse != nil && len(searchResponse.Results) > 0 {
				result = searchResponse.Results
				continue
			}
		}

		if len(result) <= len(r.PartitionData.SearchResponse.Results) {
			for i := range result {
				AddMergeSort(result[i], r.PartitionData.SearchResponse.Results[i], int(searchReq.TopN), desc)
			}
		} else {
			for i := range r.PartitionData.SearchResponse.Results {
				AddMergeSort(result[i], r.PartitionData.SearchResponse.Results[0], int(searchReq.TopN), desc)
			}
		}
	}

	for _, resp := range result {
		quickSort(resp.ResultItems, desc, 0, len(resp.ResultItems)-1)
		if resp.ResultItems != nil && len(resp.ResultItems) > 0 && searchReq.TopN > 0 {
			len := len(resp.ResultItems)
			if int32(len) > searchReq.TopN {
				resp.ResultItems = resp.ResultItems[0:searchReq.TopN]
			}
		}
	}

	if searchResponse == nil {
		searchResponse = &vearchpb.SearchResponse{}
		responseHead := &vearchpb.ResponseHead{Err: finalErr}
		searchResponse.Head = responseHead
	}
	mergeAndSort := time.Since(mergeStartTime).Seconds() * 1000
	mergeAndSortStr := strconv.FormatFloat(mergeAndSort, 'f', 4, 64)
	if trace && searchResponse.Head != nil && searchResponse.Head.Params != nil {
		searchResponse.Head.Params["mergeAndSort"] = mergeAndSortStr
		searchResponse.Head.Params["searchPartitions"] = searchPartitionsStr
		searchExecute := time.Since(startTime).Seconds() * 1000
		searchExecuteStr := strconv.FormatFloat(searchExecute, 'f', 4, 64)
		searchResponse.Head.Params["searchExecute"] = searchExecuteStr
	}
	searchResponse.Results = result
	searchResponse.Head.RequestId = r.GetMsgID()
	return searchResponse
}

func (r *routerRequest) queryFromPartition(ctx context.Context, partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, respChain chan *response.SearchDocResult) {
	responseDoc := &response.SearchDocResult{}
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("queryFromPartition partitionID: [%v], err: [%v]", partitionID, r)
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
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_PARTITION_NOT_EXIST, Msg: "query partition cache err partitionID:" + fmt.Sprint(partitionID)}
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

	faultyNodeNum := r.replicasFaultyNum(partition.Replicas)
	retryTime := 0
	var retry_err error
	if len(partition.Replicas) <= faultyNodeNum || nodeID == 0 {
		msg := fmt.Sprintf("nodeID %v partitionID: %d is faulty, replica_num=%d, faultyNodeNum=%d", nodeID, partitionID, len(partition.Replicas), faultyNodeNum)
		log.Error(msg)
		retry_err = fmt.Errorf(msg)
	}
	for len(partition.Replicas) > faultyNodeNum && retryTime < len(partition.Replicas) {
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

		retry_err = rpcClient.Execute(ctx, UnaryHandler, pd, replyPartition)
		if retry_err == nil {
			break
		}

		log.Error("nodeID %v partitionID: %d rpc err [%v], retryTime: %d, len(partition.Replicas)=%d, faultyNodeNum: %d", nodeID, partitionID, retry_err, retryTime, len(partition.Replicas), faultyNodeNum)
		if strings.Contains(retry_err.Error(), "connect: connection refused") {
			r.client.PS().AddFaulty(nodeID, time.Second*30)
		} else {
			break
		}
		nodeID = GetNodeIdsByClientType(clientType, partition, servers, r.client)

		faultyNodeNum = r.replicasFaultyNum(partition.Replicas)
		retryTime++
	}

	if retry_err != nil {
		var err *vearchpb.Error
		if vErr, ok := retry_err.(*vearchpb.VearchErr); ok {
			err = &vearchpb.Error{Code: vErr.GetError().Code, Msg: retry_err.Error()}
		} else {
			err = &vearchpb.Error{Code: vearchpb.ErrorEnum_INTERNAL_ERROR, Msg: retry_err.Error()}
		}
		head := &vearchpb.ResponseHead{Err: err}
		searchResponse := &vearchpb.SearchResponse{Head: head}
		pd.SearchResponse = searchResponse
		responseDoc.PartitionData = pd
		respChain <- responseDoc
		return
	}

	searchResponse := replyPartition.SearchResponse
	if searchResponse != nil {
		flatBytes := searchResponse.FlatBytes
		if flatBytes != nil {
			gamma.DeSerialize(flatBytes, searchResponse)
		}
	}
	responseDoc.PartitionData = replyPartition
	respChain <- responseDoc
}

func (r *routerRequest) QueryFieldSortExecute() *vearchpb.SearchResponse {
	var wg sync.WaitGroup
	sendPartitionMap := r.sendMap

	var searchReq *vearchpb.QueryRequest
	respChain := make(chan *response.SearchDocResult, len(sendPartitionMap))
	for partitionID, pData := range sendPartitionMap {
		searchReq = pData.QueryRequest
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, copyMap(r.md))
		go func(ctx context.Context, partitionID entity.PartitionID, pd *vearchpb.PartitionData, space *entity.Space, respChain chan *response.SearchDocResult) {
			defer wg.Done()
			r.queryFromPartition(ctx, partitionID, pd, space, respChain)
		}(c, partitionID, pData, r.space, respChain)
	}
	wg.Wait()
	close(respChain)

	var result []*vearchpb.SearchResult
	var searchResponse *vearchpb.SearchResponse
	var head vearchpb.ResponseHead

	var finalErr *vearchpb.Error
	for r := range respChain {
		if r != nil && r.PartitionData.Err != nil {
			finalErr = r.PartitionData.Err
			continue
		}
		if result == nil && r != nil {
			searchResponse = r.PartitionData.SearchResponse
			if searchResponse != nil && searchResponse.Head != nil && searchResponse.Head.Err != nil {
				head.Err = searchResponse.Head.Err
				continue
			}
			if searchResponse != nil && searchResponse.Results != nil && len(searchResponse.Results) > 0 {
				result = searchResponse.Results
				continue
			}
		}

		if err := AddMergeResultArr(result, r.PartitionData.SearchResponse.Results); err != nil {
			log.Error("query AddMergeResultArr error:", err)
		}
	}

	if result == nil && head.Err != nil {
		err := &vearchpb.Error{Code: head.Err.Code, Msg: head.Err.Msg}
		searchResponse = &vearchpb.SearchResponse{}
		responseHead := &vearchpb.ResponseHead{Err: err}
		searchResponse.Head = responseHead
		return searchResponse
	}

	if searchResponse == nil {
		searchResponse = &vearchpb.SearchResponse{}
		responseHead := &vearchpb.ResponseHead{Err: finalErr}
		searchResponse.Head = responseHead
	}

	if len(result) == 0 {
		searchResponse = &vearchpb.SearchResponse{}
		responseHead := &vearchpb.ResponseHead{}
		searchResponse.Head = responseHead
		return searchResponse
	}

	if len(result) > 1 {
		err := &vearchpb.Error{Code: vearchpb.ErrorEnum_PARAM_ERROR, Msg: "the document_ids of query should be a one-dimensional array"}
		searchResponse = &vearchpb.SearchResponse{}
		responseHead := &vearchpb.ResponseHead{Err: err}
		searchResponse.Head = responseHead
		return searchResponse
	}

	if len(searchReq.DocumentIds) == 0 {
		for _, resp := range result {
			if resp.ResultItems != nil && len(resp.ResultItems) > 0 && searchReq.Limit > 0 {
				len := len(resp.ResultItems)
				if int32(len) > searchReq.Limit {
					resp.ResultItems = resp.ResultItems[0:searchReq.Limit]
				}
			}
		}
	} else {
		// order by document_ids
		orderMap := make(map[string]int)
		for i, name := range searchReq.DocumentIds {
			orderMap[name] = i
		}
		for _, item := range result[0].ResultItems {
			for _, field := range item.Fields {
				if field != nil && field.Name == "_id" {
					item.PKey = string(field.Value)
				}
			}
		}
		sort.Slice(result[0].ResultItems, func(i, j int) bool {
			return orderMap[result[0].ResultItems[i].PKey] < orderMap[result[0].ResultItems[j].PKey]
		})
	}

	searchResponse.Results = result
	return searchResponse
}

func quickSort(items []*vearchpb.ResultItem, desc bool, low, high int) {
	if low < high {
		var pivot = partition(items, desc, low, high)
		quickSort(items, desc, low, pivot)
		quickSort(items, desc, pivot+1, high)
	}
}

func partition(items []*vearchpb.ResultItem, desc bool, low, high int) int {
	var pivot = items[low]
	var i = low
	var j = high
	for i < j {
		if desc {
			for j > low && items[j].Score <= pivot.Score {
				j--
			}
			for i < high && items[i].Score > pivot.Score {
				i++
			}
		} else {
			for j > low && items[j].Score >= pivot.Score {
				j--
			}
			for i < high && items[i].Score < pivot.Score {
				i++
			}
		}
		if i < j {
			items[i], items[j] = items[j], items[i]
		}
	}

	items[low], items[j] = items[j], pivot
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
	if queryReq.PartitionId != 0 {
		partitionID := uint32(queryReq.PartitionId)
		if d, ok := sendMap[partitionID]; ok {
			log.Error("db Id:%s , space :%s, have multiple partitionID:%d", queryReq.Head.DbName, queryReq.Head.SpaceName, partitionID)
		} else {
			d = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), QueryRequest: queryReq}
			sendMap[partitionID] = d
		}
	} else {
		for _, partitionInfo := range r.space.Partitions {
			partitionID := partitionInfo.Id
			if d, ok := sendMap[partitionID]; ok {
				log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
			} else {
				d = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), QueryRequest: queryReq}
				sendMap[partitionID] = d
			}
		}
	}

	r.sendMap = sendMap
	return r
}

type RoundRobin[K comparable, V any] struct {
	counterMap sync.Map
}

func newRoundRobin[K comparable, V any]() *RoundRobin[K, V] {
	return &RoundRobin[K, V]{}
}

func (rr *RoundRobin[K, V]) Next(k K, v []V) V {
	if len(v) <= 0 {
		var zeroValue V
		return zeroValue
	}

	var ix uint64
	counter, ok := rr.counterMap.LoadOrStore(k, atomic.NewCounter(1))
	if ok {
		// loaded
		newValue := counter.(*atomic.AtomicCounter).Incr()
		ix = (newValue - 1) % uint64(len(v))
	}
	return v[ix]
}

func (r *routerRequest) SearchByPartitions(searchReq *vearchpb.SearchRequest) *routerRequest {
	if r.Err != nil {
		return r
	}
	r.sendMap = make(map[entity.PartitionID]*vearchpb.PartitionData)
	for _, p := range r.space.Partitions {
		if _, ok := r.sendMap[p.Id]; ok {
			log.Error("db Id:%d , space Id:%d, have multiple partitionID:%d", p.DBId, p.SpaceId, p.Id)
			continue
		}
		r.sendMap[p.Id] = &vearchpb.PartitionData{PartitionID: p.Id, MessageID: r.GetMsgID(), SearchRequest: searchReq}
	}
	return r
}

var replicaRoundRobin = newRoundRobin[entity.PartitionID, entity.NodeID]()

func GetNodeIdsByClientType(clientType string, partition *entity.Partition, servers *cache.Cache, client *Client) entity.NodeID {
	nodeId := uint64(0)
	switch clientType {
	case request.Leader:
		nodeId = partition.LeaderID
	case request.NotLeader:
		noLeaderIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if !serverExist {
				continue
			}
			if client.PS().TestFaulty(nodeID) {
				continue
			}
			if config.Conf().Global.RaftConsistent {
				if partition.ReStatusMap[nodeID] == entity.ReplicasOK && nodeID != partition.LeaderID {
					noLeaderIDs = append(noLeaderIDs, nodeID)
				}
			} else {
				if nodeID != partition.LeaderID {
					noLeaderIDs = append(noLeaderIDs, nodeID)
				}
			}
		}
		nodeId = replicaRoundRobin.Next(partition.Id, noLeaderIDs)
	case request.Random, "":
		randIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if !serverExist {
				continue
			}
			if client.PS().TestFaulty(nodeID) {
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
	case request.LeastConnection:
		leastId := uint64(0)
		most := 1<<32 - 1
		least := -1
		randIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if !serverExist {
				continue
			}
			if client.PS().TestFaulty(nodeID) {
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
	default:
		randIDs := make([]entity.NodeID, 0)
		for _, nodeID := range partition.Replicas {
			_, serverExist := servers.Get(cast.ToString(nodeID))
			if !serverExist {
				continue
			}
			if client.PS().TestFaulty(nodeID) {
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
	}
	return nodeId
}

func AddMergeResultArr(dest []*vearchpb.SearchResult, src []*vearchpb.SearchResult) error {
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

func mergeSortedArrays(arr1, arr2 []*vearchpb.ResultItem, topN int, desc bool) []*vearchpb.ResultItem {
	m, n := len(arr1), len(arr2)
	merged := make([]*vearchpb.ResultItem, 0, m+n)

	if m == 0 {
		if topN > 0 && n > topN {
			return arr2[:topN]
		} else {
			return arr2
		}
	}
	if n == 0 {
		if topN > 0 && m > topN {
			return arr1[:topN]
		} else {
			return arr1
		}
	}

	i, j := 0, 0
	if desc {
		for i < m && j < n {
			if arr1[i].Score > arr2[j].Score {
				merged = append(merged, arr1[i])
				i++
			} else {
				merged = append(merged, arr2[j])
				j++
			}
		}
	} else {
		for i < m && j < n {
			if arr1[i].Score < arr2[j].Score {
				merged = append(merged, arr1[i])
				i++
			} else {
				merged = append(merged, arr2[j])
				j++
			}
		}
	}

	// Append remaining elements from arr1
	for i < m {
		merged = append(merged, arr1[i])
		i++
	}

	// Append remaining elements from arr2
	for j < n {
		merged = append(merged, arr2[j])
		j++
	}

	if len(merged) > topN {
		return merged[:topN]
	}

	return merged
}

func AddMergeSort(sr *vearchpb.SearchResult, other *vearchpb.SearchResult, topN int, desc bool) {
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
		sr.ResultItems = mergeSortedArrays(sr.ResultItems, other.ResultItems, topN, desc)
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
	if args.PartitionId != 0 {
		sendMap[args.PartitionId] = &vearchpb.PartitionData{PartitionID: args.PartitionId, MessageID: r.GetMsgID(), IndexRequest: args}
	} else {
		for _, partitionInfo := range r.space.Partitions {
			partitionID := partitionInfo.Id
			if _, ok := sendMap[partitionID]; ok {
				log.Error("db Id:%d, space Id:%d, have multiple partitionID:%d", partitionInfo.DBId, partitionInfo.SpaceId, partitionID)
			} else {
				sendMap[partitionID] = &vearchpb.PartitionData{PartitionID: partitionID, MessageID: r.GetMsgID(), IndexRequest: args}
			}
		}
	}

	r.sendMap = sendMap
	return r
}

// ForceMergeExecute Execute request
func (r *routerRequest) ForceMergeExecute() *vearchpb.ForceMergeResponse {
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, copyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%v]", pid, r)}
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
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, copyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%v]", pid, r)}
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
	var wg sync.WaitGroup
	partitionLen := len(r.sendMap)
	respChain := make(chan *vearchpb.PartitionData, partitionLen)
	for partitionID, pData := range r.sendMap {
		wg.Add(1)
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, copyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%v]", pid, r)}
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
		c := context.WithValue(r.ctx, share.ReqMetaDataKey, copyMap(r.md))
		go func(ctx context.Context, pid entity.PartitionID, d *vearchpb.PartitionData) {
			defer wg.Done()
			replyPartition := new(vearchpb.PartitionData)
			defer func() {
				if r := recover(); r != nil {
					log.Error("recover info: %v", r)
					d.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_RECOVER, Msg: fmt.Sprintf("[Recover] partitionID: [%v], err: [%v]", pid, r)}
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
		if resp.Err != nil || resp.DelByQueryResponse == nil {
			log.Error("err: %s", resp.Err)
			continue
		}
		if len(resp.DelByQueryResponse.IdsStr) > 0 {
			delByQueryResponse.IdsStr = append(delByQueryResponse.IdsStr, resp.DelByQueryResponse.IdsStr...)
		}
	}
	delByQueryResponse.DelNum = int32(len(delByQueryResponse.IdsStr))
	return delByQueryResponse
}
