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
	"fmt"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/pkg/log"
	server "github.com/vearch/vearch/internal/pkg/server/rpc"
	"github.com/vearch/vearch/internal/pkg/vjson"
	"github.com/vearch/vearch/internal/proto/vearchpb"
)

// ClientType decide the method to choose raft
type ClientType int

const (
	LEADER ClientType = iota
	NOTLEADER
	RANDOM
	ALL
)

const (
	spaceRetry    = 3
	adaptRetry    = 3
	baseSleepTime = 200 * time.Millisecond
)

const (
	HandlerType  = "type"
	UnaryHandler = "UnaryHandler"

	SearchHandler        = "SearchHandler"
	QueryHandler         = "QueryHandler"
	DeleteByQueryHandler = "DeleteByQueryHandler"

	GetDocHandler                 = "GetDocHandler"
	GetDocsHandler                = "GetDocsHandler"
	GetDocsByPartitionHandler     = "GetDocsByPartitionHandler"
	GetNextDocsByPartitionHandler = "GetNextDocsByPartitionHandler"
	DeleteDocsHandler             = "DeleteDocsHandler"
	BatchHandler                  = "BatchHandler"
	ForceMergeHandler             = "ForceMergeHandler"
	RebuildIndexHandler           = "RebuildIndexHandler"
	FlushHandler                  = "FlushHandler"

	CreatePartitionHandler = "CreatePartitionHandler"
	DeletePartitionHandler = "DeletePartitionHandler"
	DeleteReplicaHandler   = "DeleteReplicaHandler"
	UpdatePartitionHandler = "UpdatePartitionHandler"
	StatsHandler           = "StatsHandler"
	IsLiveHandler          = "IsLiveHandler"
	PartitionInfoHandler   = "PartitionInfoHandler"
	ChangeMemberHandler    = "ChangeMemberHandler"
	EngineCfgHandler       = "EngineCfgHandler"
)

type psClient struct {
	client     *Client
	faultyList *cache.Cache
}

func (ps *psClient) Client() *Client {
	return ps.client
}

// when psclient stop, it will remove all client
func (ps *psClient) Stop() {
	ps.Client().Master().cliCache.Range(func(key, value interface{}) bool {
		value.(*rpcClient).close()
		ps.Client().Master().cliCache.Delete(key)
		return true
	})
}

func (ps *psClient) GetOrCreateRPCClient(ctx context.Context, nodeID entity.NodeID) *rpcClient {
	value, ok := ps.Client().Master().cliCache.Load(nodeID)
	if ok {
		return value.(*rpcClient).lastUse()
	}

	ps.Client().Master().cliCache.lock.Lock()
	defer ps.Client().Master().cliCache.lock.Unlock()

	value, ok = ps.Client().Master().cliCache.Load(nodeID)
	if ok {
		return value.(*rpcClient).lastUse()
	}

	log.Info("psClient not in psClientCache, make new psClient, nodeID:[%d]", nodeID)
	psServer, err := ps.Client().Master().cliCache.ServerByCache(ctx, nodeID)
	if err != nil {
		log.Error("Master().ServerByCache() err, can not get ps server from master, err: %s", err.Error())
		return nilClient
	}

	client, err := server.NewRpcClient(psServer.Ip + ":" + cast.ToString(psServer.RpcPort))
	if err != nil {
		log.Error("server.NewRpcClient() err, can not new rpc Client, err: %s", err.Error())
		return nilClient
	}

	if client != nil {
		c := &rpcClient{client: client, useTime: time.Now().UnixNano()}
		ps.Client().Master().cliCache.Store(nodeID, c)
		return c.lastUse()
	}

	return nilClient
}

func (ps *psClient) initFaultylist() {
	ps.faultyList = cache.New(time.Second*30, time.Second*5)
}

func (ps *psClient) AddFaulty(nodeID uint64, d time.Duration) {
	ps.faultyList.Set(fmt.Sprint(nodeID), nodeID, d)
}

func (ps *psClient) TestFaulty(nodeID uint64) bool {
	_, b := ps.faultyList.Get(fmt.Sprint(nodeID))
	return b
}

var nilClient = &rpcClient{}

type rpcClient struct {
	client  *server.RpcClient
	useTime int64
	_lock   sync.RWMutex
}

func (r *rpcClient) close() {
	r._lock.Lock()
	defer r._lock.Unlock()
	if e := r.client.Close(); e != nil {
		log.Error(e.Error())
	}
	r.client = nil
}

func (r *rpcClient) lastUse() *rpcClient {
	r.useTime = time.Now().UnixNano()
	return r
}

func (r *rpcClient) Execute(ctx context.Context, servicePath string, args interface{}, reply *vearchpb.PartitionData) error {
	if r == nilClient {
		return vearchpb.NewError(vearchpb.ErrorEnum_CREATE_RPCCLIENT_FAILED, nil)
	}
	return r.client.Execute(ctx, servicePath, args, reply)
}

func (r *rpcClient) GetConcurrent() int {
	if r == nilClient {
		return -1
	}
	return r.client.GetConcurrent()
}

// Execute add retry to handle no leader and not leader situation
func Execute(addr, servicePath string, args *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	ctx := context.Background()
	sleepTime := baseSleepTime
	for i := 0; i < adaptRetry; i++ {
		err = execute(ctx, addr, servicePath, args, reply)
		if err == nil {
			return nil
		}
		if reply.Err != nil && reply.Err.Code == vearchpb.ErrorEnum_PARTITION_NO_LEADER {
			sleepTime = 2 * sleepTime
			time.Sleep(sleepTime)
			log.Warn("%s invoke no leader retry, PartitionID: %d, PartitionRpcAddr: %s", servicePath, args.PartitionID, addr)
			continue
		} else if reply.Err != nil && reply.Err.Code == vearchpb.ErrorEnum_PARTITION_NOT_LEADER {
			addrs := new(entity.Replica)
			err = vjson.Unmarshal([]byte(reply.Err.Msg), addrs)
			if err != nil {
				return err
			}
			addr = addrs.RpcAddr
			log.Debug("%s invoke not leader retry, PartitionID: %d, PartitionRpcAddr: %s", servicePath, args.PartitionID, addr)
			continue
		}
	}
	return err
}

// execute not use cache or pool, it only conn once and close client
func execute(ctx context.Context, addr, servicePath string, args *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	client, err := server.NewRpcClient(addr)
	if err != nil {
		log.Error("NewRpcClient() err, err:[%s]", err.Error())
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Error("close client err : %s", err.Error())
		}
	}()
	return client.Execute(ctx, servicePath, args, reply)
}
