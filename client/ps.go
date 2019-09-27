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
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/util/cbjson"
	server "github.com/vearch/vearch/util/server/rpc"
	"github.com/vearch/vearch/util/uuid"
	"sync"
	"time"

	"bytes"
	"github.com/spf13/cast"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util/server/rpc/handler"
)

type ClientType int

const (
	LEADER ClientType = iota
	RANDOM
	ALL
)

const (
	spaceRetry    = 3
	adaptRetry    = 3
	baseSleepTime = 200 * time.Millisecond
)

const (
	//doc handler
	SearchHandler        = "SearchHandler"
	DeleteByQueryHandler = "DeleteByQueryHandler"
	MSearchHandler       = "MSearchHandler"
	StreamSearchHandler  = "StreamSearchHandler"
	GetDocHandler        = "GetDocHandler"
	GetDocsHandler       = "GetDocsHandler"
	WriteHandler         = "WriteHandler"
	BatchHandler         = "BatchHandler"
	FlushHandler         = "FlushHandler"
	ForceMergeHandler    = "ForceMergeHandler"

	//admin handler
	CreatePartitionHandler = "CreatePartitionHandler"
	DeletePartitionHandler = "DeletePartitionHandler"
	UpdatePartitionHandler = "UpdatePartitionHandler"
	StatsHandler           = "StatsHandler"
	IsLiveHandler          = "IsLiveHandler"
	MaxMinZoneFieldHandler = "MaxMinHandler"
	PartitionInfoHandler   = "PartitionInfoHandler"
)

type psClient struct {
	client *Client
}

func (this *psClient) Client() *Client {
	return this.client
}

func (this *psClient) B() *sender {
	return this.Be(context.Background())
}

func (this *psClient) Be(ctx context.Context) *sender {
	return this.Beg(ctx, uuid.FlakeUUID())
}

func (this *psClient) Beg(ctx context.Context, msgId string) *sender {
	return this.Begin(ctx, 0, msgId, "")
}

func (this *psClient) Begin(ctx context.Context, partitionNum int, msgId, routerAddr string) *sender {
	sender := sender{ps: this}
	sender.Ctx = &request.RequestContext{
		MessageId: msgId,
	}
	sender.Ctx.SetContext(ctx)
	return &sender
}

//when psclient stop , it will remove all client
func (this *psClient) Stop() {
	this.Client().Master().cliCache.Range(func(key, value interface{}) bool {
		value.(*rpcClient).close()
		this.Client().Master().cliCache.Delete(key)
		return true
	})
}

type sender struct {
	ps  *psClient
	Ctx *request.RequestContext
}

func (this *sender) MultipleSpace(dbSpaces [][2]string) *multipleSpaceSender {
	senders := make([]*spaceSender, 0, len(dbSpaces))
	for _, item := range dbSpaces {
		senders = append(senders, &spaceSender{sender: this, db: item[0], space: item[1]})
	}
	return &multipleSpaceSender{senders: senders}
}

func (this *sender) Space(db, space string) *spaceSender {
	return &spaceSender{sender: this, db: db, space: space}
}

func (this *sender) Admin(partitionServerRpcAddr string) *adminSender {
	return &adminSender{sender: this, addr: partitionServerRpcAddr}
}

var nilClient = &rpcClient{}

type rpcClient struct {
	client  *server.RpcClient
	useTime int64
	lock    sync.RWMutex
}

func (this *rpcClient) close() {
	this.lock.Lock()
	defer this.lock.Unlock()
	if e := this.client.Close(); e != nil {
		log.Error(e.Error())
	}
	this.client = nil
}

func (this *rpcClient) lastUse() *rpcClient {
	this.useTime = time.Now().UnixNano()
	return this
}

// ExecuteErrorChangeRetry add retry to handle no leader and not leader situation
func Execute(addr, servicePath string, request request.Request) (interface{}, int, error) {
	sleepTime := baseSleepTime
	var (
		response interface{}
		status   int
		e        error
	)
	for i := 0; i < adaptRetry; i++ {
		response, status, e = execute(addr, servicePath, request)
		if status == pkg.ERRCODE_PARTITION_NO_LEADER {
			sleepTime = 2 * sleepTime
			time.Sleep(sleepTime)
			log.Debug("%s invoke no leader retry, PartitionID: %d, PartitionRpcAddr: %s", servicePath, request.GetPartitionID(), addr)
			continue
		} else if status == pkg.ERRCODE_PARTITION_NOT_LEADER {
			addrs := new(entity.Replica)
			err := cbjson.Unmarshal([]byte(e.Error()), addrs)
			if err != nil {
				return response, status, e
			}
			addr = addrs.RpcAddr
			log.Debug("%s invoke not leader retry, PartitionID: %d, PartitionRpcAddr: %s", servicePath, request.GetPartitionID(), addr)
			continue
		}
		return response, status, e
	}
	return response, status, e
}

//this execute not use cache or pool , it only conn once and close client
func execute(addr, servicePath string, request request.Request) (interface{}, int, error) {

	client, err := server.NewRpcClient(addr)
	if err != nil {
		log.Error("NewRpcClient() err, err:[%s]", err.Error())
		return nil, pkg.ERRCODE_INTERNAL_ERROR, err
	}

	defer func() {
		if err := client.Close(); err != nil {
			log.Error("close client err : %s", err.Error())
		}
	}()

	response, err := client.Execute(servicePath, &handler.RpcRequest{MessageId: request.Context().MessageId, Arg: request, Ctx: request.Context().GetContext()})

	if err != nil {
		return nil, pkg.ErrCode(err), err
	}

	if response.Error != "" {
		return nil, response.Status, fmt.Errorf(response.Error)
	}

	return response.Result, pkg.ERRCODE_SUCCESS, nil
}

func (this *rpcClient) Execute(servicePath string, request request.Request) (interface{}, int, error) {
	if this == nilClient {
		return nil, pkg.ERRCODE_INTERNAL_ERROR, fmt.Errorf("create client err , it is nil")
	}

	rpcResponse, err := this.client.Execute(servicePath, &handler.RpcRequest{MessageId: request.Context().MessageId, Arg: request, Ctx: request.Context().GetContext()})

	if err != nil {
		return nil, pkg.ErrCode(err), err
	}

	if rpcResponse.Error != "" {
		return rpcResponse.Result, rpcResponse.Status, fmt.Errorf(rpcResponse.Error)
	}

	return rpcResponse.Result, pkg.ERRCODE_SUCCESS, nil
}

func (this *rpcClient) StreamExecute(servicePath string, request request.Request, sc server.StreamCallback) (interface{}, int, error) {
	if this == nilClient {
		return nil, pkg.ERRCODE_INTERNAL_ERROR, fmt.Errorf("create client err , it is nil")
	}
	rpcResponse, err := this.client.StreamExecute(request.Context().GetContext(), servicePath, &handler.RpcRequest{MessageId: request.Context().MessageId, Arg: request, Ctx: request.Context().GetContext()}, sc)
	if err != nil {
		return nil, pkg.ErrCode(err), err
	}

	if rpcResponse.Error != "" {
		return rpcResponse.Result, rpcResponse.Status, fmt.Errorf(rpcResponse.Error)
	}

	return rpcResponse.Result, pkg.ERRCODE_SUCCESS, nil
}

func (ps *psClient) getOrCreateRpcClient(ctx context.Context, nodeId entity.NodeID) *rpcClient {
	value, ok := ps.Client().Master().cliCache.Load(nodeId)
	if ok {
		return value.(*rpcClient).lastUse()
	}
	ps.Client().Master().cliCache.lock.Lock()
	defer ps.Client().Master().cliCache.lock.Unlock()

	value, ok = ps.Client().Master().cliCache.Load(nodeId)
	if ok {
		return value.(*rpcClient).lastUse()
	}

	log.Info("psClient not in psClientCache, make new psClient, nodeId:[%d]", nodeId)
	psServer, err := ps.Client().Master().cliCache.ServerByCache(ctx, nodeId)
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
		ps.Client().Master().cliCache.Store(nodeId, c)
		return c.lastUse()
	}

	return nilClient
}

func newSearchResponseWithError(dbName, spaceName string, pid uint32, err error) *response.SearchResponse {
	bb := bytes.Buffer{}
	bb.WriteString("db:[")
	bb.WriteString(dbName)
	bb.WriteString("], ")
	bb.WriteString("space:[")
	bb.WriteString(spaceName)
	bb.WriteString("], ")
	bb.WriteString("partitionID:[")
	bb.WriteString(cast.ToString(pid))
	bb.WriteString("], err:")
	bb.WriteString(err.Error())

	return &response.SearchResponse{
		Status: &response.SearchStatus{
			Failed: 1,
			Errors: response.IndexErrMap{err.Error(): fmt.Errorf(bb.String())},
		},
	}
}
