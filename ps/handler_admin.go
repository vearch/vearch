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
	"encoding/json"
	"fmt"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/util/metrics/mserver"
	"time"

	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util/server/rpc/handler"
)

func ExportToRpcAdminHandler(server *Server) {

	initAdminHandler := &InitAdminHandler{server: server}

	storeHandler := &SetStoreHandler{server: server}

	psErrorChange := psErrorChange(server)

	if err := server.rpcServer.RegisterName(handler.NewChain(client.CreatePartitionHandler, server.monitor, handler.DefaultPanicHadler, nil, initAdminHandler, &CreatePartitionHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.DeletePartitionHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initAdminHandler, &DeletePartitionHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.UpdatePartitionHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initAdminHandler, storeHandler, new(UpdatePartitionHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.StatsHandler, server.monitor, handler.DefaultPanicHadler, nil, initAdminHandler, &StatsHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.IsLiveHandler, server.monitor, handler.DefaultPanicHadler, nil, initAdminHandler, new(IsLiveHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.PartitionInfoHandler, server.monitor, handler.DefaultPanicHadler, nil, initAdminHandler, storeHandler, new(PartitionSizeHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.ChangeMemberHandler, server.monitor, handler.DefaultPanicHadler, nil, initAdminHandler, storeHandler, &ChangeMemberHandler{server: server}), ""); err != nil {
		panic(err)
	}

}

type InitAdminHandler struct {
	server *Server
}

func (i *InitAdminHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	if i.server.stopping.Get() {
		return pkg.ErrGeneralServiceUnavailable
	}
	arg := req.Arg.(request.Request)
	rCtx := arg.Context()
	if rCtx.Timeout > 0 {
		ctx, cancel := context.WithTimeout(req.Ctx, time.Duration(rCtx.Timeout)*time.Second)
		arg.SetContext(ctx)
		req.Cancel = cancel
	} else {
		arg.SetContext(req.Ctx)
	}
	return nil
}

type SetStoreHandler struct {
	server *Server
}

func (s *SetStoreHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.Arg.(request.Request)
	store := s.server.GetPartition(reqs.GetPartitionID())
	if store == nil {
		return pkg.ErrPartitionNotExist
	}
	reqs.Context().SetStore(store)
	return nil
}

type CreatePartitionHandler struct {
	server *Server
}

func (c *CreatePartitionHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {

	reqs := req.GetArg().(*request.ObjRequest)

	reqObj := &struct {
		Space       *entity.Space
		PartitionId uint32
	}{}

	if err := reqs.Decode(reqObj); err != nil {
		return err
	}

	c.server.partitions.Range(func(key, value interface{}) bool {
		fmt.Print(key, value)
		return true
	})

	if partitionStore := c.server.GetPartition(reqObj.PartitionId); partitionStore != nil {
		return pkg.ErrPartitionDuplicate
	}

	if err := c.server.CreatePartition(req.Ctx, reqObj.Space, reqObj.PartitionId); err != nil {
		c.server.DeletePartition(reqObj.PartitionId)
		return err
	}
	return nil
}

type DeletePartitionHandler struct {
	server *Server
}

func (d *DeletePartitionHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {

	log.Debug("DeletePartitionHandler method start, req: %v", req)
	defer func() {
		log.Debug("DeletePartitionHandler method end, req: %v, resp: %v", req, resp)
	}()
	reqs := req.GetArg().(*request.ObjRequest)
	d.server.DeletePartition(reqs.PartitionID)
	log.Info("Partition delete partitionID: %v", reqs.PartitionID)

	return nil
}

type UpdatePartitionHandler int

func (*UpdatePartitionHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {

	reqs := req.GetArg().(*request.ObjRequest)

	space := new(entity.Space)

	if err := reqs.Decode(space); err != nil {
		return fmt.Errorf("parse space obj err : %s", err.Error())
	}

	store := reqs.GetStore().(PartitionStore)

	if store.GetVersion() > space.Version {
		return fmt.Errorf("partition[%d] schema version more new %d %d", store.GetPartition().Id, store.GetVersion(), space.Version)
	}

	err := store.UpdateSpace(req.Ctx, space)
	if err != nil {
		return err
	}

	return nil
}

type IsLiveHandler int

func (*IsLiveHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	resp.Result = true
	return nil
}

type PartitionSizeHandler int

func (mm *PartitionSizeHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {

	pid := req.Arg.(*request.ObjRequest).PartitionID

	store := req.Arg.(*request.ObjRequest).GetStore().(PartitionStore)

	docNum, err := store.GetEngine().Reader().DocCount(req.Ctx)
	if err != nil {
		return err
	}

	size, err := store.GetEngine().Reader().Capacity(req.Ctx)
	if err != nil {
		return err
	}

	value := &entity.PartitionInfo{
		PartitionID: pid,
		DocNum:      docNum,
		Size:        size,
		Path:        store.GetPartition().Path,
		Unreachable: store.GetUnreachable(uint64(pid)),
		Status:      store.GetPartition().GetStatus(),
	}

	resp.Result, err = response.NewObjResponse(value)

	return nil
}

type StatsHandler struct {
	server *Server
}

func (sh *StatsHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	stats := mserver.NewServerStats()
	stats.ActiveConn = len(sh.server.rpcServer.ActiveClientConn())
	resp.Result = stats
	return nil
}

type ChangeMemberHandler struct {
	server *Server
}

func (ch *ChangeMemberHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.GetArg().(*request.ObjRequest)

	reqObj := new(entity.ChangeMember)

	if err := reqs.Decode(reqObj); err != nil {
		return err
	}

	store := reqs.GetStore().(PartitionStore)

	if !store.IsLeader() {
		return pkg.ErrPartitionNotLeader
	}

	server, err := ch.server.client.Master().QueryServer(reqs.Context().GetContext(), reqObj.NodeID)
	if err != nil {
		log.Error("get server info err %s", err.Error())
		return err
	}

	if reqObj.Method == proto.ConfAddNode {
		ch.server.raftResolver.AddNode(reqObj.NodeID, server.Replica())
	} else if reqObj.Method == proto.ConfRemoveNode {
		ch.server.raftResolver.DeleteNode(reqObj.NodeID)
	}

	return store.ChangeMember(reqObj.Method, server)
}

// it when has happen , redirect some other to response and send err to status
func psErrorChange(server *Server) handler.ErrorChangeFun {
	return func(ctx context.Context, err error, req *handler.RpcRequest, response *handler.RpcResponse) error {
		if err == pkg.ErrPartitionNotLeader || err == raft.ErrNotLeader {
			id, _ := req.Arg.(request.Request).Context().GetStore().(PartitionStore).GetLeader()
			if id == 0 {
				response.Status = pkg.ERRCODE_PARTITION_NO_LEADER
				response.Error = fmt.Sprintf("partition:[%d] no leader", req.Arg.(request.Request).GetPartitionID())
			} else {
				response.Status = pkg.ERRCODE_PARTITION_NOT_LEADER
				bytes, err := json.Marshal(server.raftResolver.ToReplica(id))
				if err != nil {
					log.Error("find raft resolver err[%s]", err.Error())
					return err
				}
				response.Error = string(bytes)
			}

			return nil
		}
		return err
	}
}
