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
	"encoding/json"
	"github.com/smallnest/rpcx/protocol"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/util/server/rpc"
	"math/rand"
	"time"

	"github.com/vearch/vearch/proto/response"

	"fmt"
	"runtime/debug"
	"sync"

	"github.com/smallnest/rpcx/share"
	"github.com/spf13/cast"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb"
)

type partitionSender struct {
	spaceSender *spaceSender
	slot        entity.SlotID
	pid         entity.PartitionID

	nodeIds []entity.NodeID
}

func (this *partitionSender) getDoc(id string) (*response.DocResult, error) {

	partition, err := this.initPartition()
	if err != nil { // must use it to get partition
		return nil, err
	}

	objRequest, err := request.NewObjRequest(this.spaceSender.Ctx, this.pid, id)
	if err != nil {
		return nil, err
	}

	value, _, e := this.getOrCreate(partition, this.spaceSender.clientType).Execute(GetDocHandler, objRequest)

	if e != nil {
		return nil, e
	}
	return value.(*response.DocResult), e

}

func (this *partitionSender) getDocs(ids []string) (response.DocResults, error) {
	partition, err := this.initPartition()
	if err != nil {
		return nil, err
	}
	reqs, err := request.NewObjRequest(this.spaceSender.Ctx, this.pid, ids)
	if err != nil { // must use it to get paition
		return nil, err
	}
	value, _, e := this.getOrCreate(partition, this.spaceSender.clientType).Execute(GetDocsHandler, reqs)

	if e != nil {
		return nil, e
	}
	return value.(response.DocResults), e

}

func (this *partitionSender) DeleteByQuery(req *request.SearchRequest) *response.Response {
	partition, err := this.initPartition()
	if err != nil { // must use it to get paition
		return &response.Response{Resp: 0, Err: err}
	}
	result, _, err := this.getOrCreate(partition, this.spaceSender.clientType).Execute(DeleteByQueryHandler, req.Clone(partition.Id, this.spaceSender.db, this.spaceSender.space))

	return &response.Response{Resp: result, Err: err}
}

func (this *partitionSender) search(req *request.SearchRequest) (*response.SearchResponse, error) {
	partition, err := this.initPartition()
	if err != nil { // must use it to get paition
		return nil, err
	}
	result, _, err := this.getOrCreate(partition, this.spaceSender.clientType).Execute(SearchHandler, req.Clone(partition.Id, this.spaceSender.db, this.spaceSender.space))
	if err != nil {
		return nil, err
	}
	searchResponse := result.(*response.SearchResponse)
	searchResponse.PID = this.pid //set partition id to result

	return searchResponse, err
}

func (this *partitionSender) mSearch(req *request.SearchRequest) (response.SearchResponses, error) {
	partition, err := this.initPartition()
	if err != nil { // must use it to get paition
		return nil, err
	}
	result, _, err := this.getOrCreate(partition, this.spaceSender.clientType).Execute(MSearchHandler, req.Clone(partition.Id, this.spaceSender.db, this.spaceSender.space))
	if err != nil {
		return nil, err
	}
	searchResponses := *(result.(*response.SearchResponses))
	for _, searchResponse := range searchResponses {
		searchResponse.PID = req.PartitionID //set partition id to result
	}
	return searchResponses, nil
}

func (this *partitionSender) streamSearch(req *request.SearchRequest, dsr *response.DocStreamResult) {

	partition, err := this.initPartition()
	if err != nil { // must use it to get paition
		dsr.AddErr(err)
		return
	}

	sc := func(msg *protocol.Message) error {
		result := &response.DocResult{}
		if err := json.Unmarshal(msg.Payload, result); err != nil {
			return err
		}
		dsr.AddDoc(result)
		return nil
	}
	_, _, err = this.getOrCreate(partition, this.spaceSender.clientType).StreamExecute(StreamSearchHandler, req.Clone(partition.Id, this.spaceSender.db, this.spaceSender.space), sc)
	if err != nil {
		dsr.AddErr(err)
	}
	return
}

func (this *partitionSender) flush(clientType ClientType) error {
	partition, err := this.initPartition()
	if err != nil { // must use it to get paition
		return err
	}
	reqs, err := request.NewObjRequest(this.spaceSender.Ctx, partition.Id, nil)
	if err != nil { // must use it to get paition
		return err
	}
	_, _, err = this.getOrCreate(partition, clientType).Execute(FlushHandler, reqs)
	if err != nil {
		return err
	}
	return nil
}

func (this *partitionSender) forceMerge(clientType ClientType) error {
	partition, err := this.initPartition()
	if err != nil { // must use it to get paition
		return err
	}
	reqs, err := request.NewObjRequest(this.spaceSender.Ctx, partition.Id, nil)
	if err != nil { // must use it to get paition
		return err
	}
	_, _, err = this.getOrCreate(partition, clientType).Execute(ForceMergeHandler, reqs)
	if err != nil {
		return err
	}
	return nil
}

func (this *partitionSender) batch(docs []*pspb.DocCmd) (*response.WriteResponse, error) {
	partition, err := this.initPartition()
	if err != nil { // must use it to get paition
		return nil, err
	}

	reqs, err := request.NewObjRequest(this.spaceSender.Ctx, this.pid, docs)
	if err != nil { // must use it to get paition
		return nil, err
	}

	result, _, err := this.getOrCreate(partition, this.spaceSender.clientType).Execute(BatchHandler, reqs)
	if err != nil {
		return nil, err
	}

	if err := this.needRefresh(); err != nil {
		log.Error(err.Error())
	}

	return result.(*response.WriteResponse), err
}

func (this *partitionSender) write(cmd *pspb.DocCmd) (*response.DocResult, error) {
	partition, err := this.initPartition()
	if err != nil { // must use it to get paition
		return nil, err
	}

	reqs, err := request.NewObjRequest(this.spaceSender.Ctx, partition.Id, cmd)
	if err != nil { // must use it to get paition
		return nil, err
	}
	result, _, err := this.getOrCreate(partition, this.spaceSender.clientType).Execute(WriteHandler, reqs)
	if err != nil {
		return nil, err
	}

	if err := this.needRefresh(); err != nil {
		log.Error(err.Error())
	}

	return result.(*response.DocResult), err
}

func (this *partitionSender) needRefresh() error {
	metaDatakeysI := this.spaceSender.Ctx.GetContext().Value(share.ReqMetaDataKey)
	if metaDatakeysI == nil {
		return nil
	}
	metaDatakeys := metaDatakeysI.(map[string]string)
	if metaDatakeys == nil {
		return nil
	}
	refresh, ok := metaDatakeys["refresh"]
	if ok {
		if refresh == "false" {
			return nil
		} else if refresh == "true" || refresh == "" {
			err := this.flush(LEADER)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//every method need to use it in partitionsender
func (this *partitionSender) initPartition() (*entity.Partition, error) {
	masterClient := this.spaceSender.ps.Client().Master()
	space, err := masterClient.cliCache.SpaceByCache(this.spaceSender.Ctx.GetContext(), this.spaceSender.db, this.spaceSender.space)
	if err != nil {
		return nil, err
	}

	if !*space.Enabled {
		masterClient.cliCache.DeleteSpaceCache(context.Background(), this.spaceSender.db, this.spaceSender.space)
		if space, err = masterClient.cliCache.SpaceByCache(context.Background(), this.spaceSender.db, this.spaceSender.space); err != nil {
			return nil, err
		}

		if !*space.Enabled {
			return nil, fmt.Errorf("the db:[%d] space[%s] is not enabled ", space.DBId, space.Name)
		}
	}

	if this.pid == 0 {
		this.pid = space.PartitionId(this.slot)
	}

	return masterClient.cliCache.PartitionByCache(this.spaceSender.Ctx.GetContext(), space.Name, this.pid)
}

func (this *partitionSender) getOrCreate(partition *entity.Partition, clientType ClientType) *partitionSender {
	this.nodeIds = make([]entity.NodeID, 0)
	if clientType == LEADER {
		this.nodeIds = append(this.nodeIds, partition.LeaderID)
	} else if clientType == RANDOM {
		this.nodeIds = append(this.nodeIds, partition.Replicas[rand.Intn(len(partition.Replicas))])
	} else if clientType == ALL {
		this.nodeIds = partition.Replicas
	}
	return this
}

func (this *partitionSender) Execute(servicePath string, request request.Request) (interface{}, int, error) {
	var wg sync.WaitGroup
	replicaNum := len(this.nodeIds)
	senderResp := new(response.Response)
	respChain := make(chan *response.Response, replicaNum)

	for i := 0; i < replicaNum; i++ {
		wg.Add(1)
		go func(nodeId entity.NodeID) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error(string(debug.Stack()))
					log.Error(cast.ToString(r))
					resp := response.Response{Resp: nil, Status: pkg.ERRCODE_MASTER_SERVER_IS_NOT_RUNNING, Err: fmt.Errorf("recover err:[%s]", cast.ToString(r))}
					respChain <- &resp
				}
			}()

			sleepTime := baseSleepTime
			var (
				resps  interface{}
				status int
				e      error
			)
			rpcClient := this.spaceSender.ps.getOrCreateRpcClient(request.Context().GetContext(), nodeId)
			rpcClient.lock.RLock()
			defer rpcClient.lock.RUnlock()
			if rpcClient.client == nil {
				resp := response.Response{Resp: nil, Status: pkg.ERRCODE_MASTER_SERVER_IS_NOT_RUNNING, Err: pkg.ErrMasterServerIsNotRunning}
				respChain <- &resp
				return
			}

			for i := 0; i < adaptRetry; i++ {
				resps, status, e = rpcClient.Execute(servicePath, request)
				if status == pkg.ERRCODE_PARTITION_NO_LEADER {
					sleepTime = 2 * sleepTime
					time.Sleep(sleepTime)
					log.Debug("%s invoke no leader retry, PartitionID: %d, PartitionRpcAddr: %s", servicePath, request.GetPartitionID(), rpcClient.client.GetAddress(0))
					continue
				} else if status == pkg.ERRCODE_PARTITION_NOT_LEADER {
					addrs := new(entity.Replica)
					err := json.Unmarshal([]byte(e.Error()), addrs)
					if err != nil {
						resp := response.Response{Resp: resps, Status: status, Err: e}
						respChain <- &resp
						return
					}
					rpcClient = this.spaceSender.ps.getOrCreateRpcClient(request.Context().GetContext(), addrs.NodeID)
					log.Debug("%s invoke not leader retry, PartitionID: %d, PartitionRpcAddr: %s", servicePath, request.GetPartitionID(), rpcClient.client.GetAddress(0))
					continue
				}
				if e != nil {
					log.Error("rpc client execute err.NodeID: %d,PartitionServer Address:%s,Error:%s", nodeId, rpcClient.client.GetAddress(-1), nodeId, e.Error())
				}
				resp := response.Response{Resp: resps, Status: status, Err: e}
				respChain <- &resp
				return
			}
			resp := response.Response{Resp: resps, Status: status, Err: e}
			respChain <- &resp
			return
		}(this.nodeIds[i])
	}

	wg.Wait()
	close(respChain)

	for res := range respChain {
		if res.Err != nil {
			senderResp.Resp = res.Resp
			senderResp.Status = res.Status
			senderResp.Err = res.Err
			break
		}
		senderResp.Resp = res.Resp
		senderResp.Status = res.Status
		senderResp.Err = res.Err
	}

	return senderResp.Resp, senderResp.Status, senderResp.Err
}

func (this *partitionSender) StreamExecute(servicePath string, request request.Request, sc server.StreamCallback) (interface{}, int, error) {
	nodeId := this.nodeIds[0]
	rpcClient := this.spaceSender.ps.getOrCreateRpcClient(request.Context().GetContext(), nodeId)
	rpcClient.lock.RLock()
	defer rpcClient.lock.RUnlock()
	if rpcClient.client == nil {
		return nil, pkg.ERRCODE_MASTER_SERVER_IS_NOT_RUNNING, pkg.ErrMasterServerIsNotRunning
	}
	sleepTime := baseSleepTime

	var (
		resps  interface{}
		status int
		e      error
	)

	for i := 0; i < adaptRetry; i++ {
		resps, status, e = rpcClient.StreamExecute(servicePath, request, sc)
		if status == pkg.ERRCODE_PARTITION_NO_LEADER {
			sleepTime = 2 * sleepTime
			time.Sleep(sleepTime)
			log.Debug("%s invoke no leader retry, PartitionID: %d, PartitionRpcAddr: %s", servicePath, request.GetPartitionID(), rpcClient.client.GetAddress(0))
			continue
		} else if status == pkg.ERRCODE_PARTITION_NOT_LEADER {
			addrs := new(entity.Replica)
			err := json.Unmarshal([]byte(e.Error()), addrs)
			if err != nil {
				resp := &response.Response{Resp: resps, Status: status, Err: err}
				return resp, pkg.ERRCODE_PARTITION_NOT_LEADER, err
			}
			time.Sleep(200 * time.Millisecond)
			rpcClient = this.spaceSender.ps.getOrCreateRpcClient(request.Context().GetContext(), addrs.NodeID)
			log.Debug("%s invoke not leader retry, PartitionID: %d, PartitionRpcAddr: %s", servicePath, request.GetPartitionID(), rpcClient.client.GetAddress(0))
			continue
		}
		break
	}
	return response.Response{Resp: resps, Status: status, Err: e}, pkg.ErrCode(e), e
}
