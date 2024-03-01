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
	"errors"
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/vearch/vearch/internal/client"
	"github.com/vearch/vearch/internal/engine/sdk/go/gamma"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/ps/engine"
	"github.com/vearch/vearch/internal/util/cbjson"
	"github.com/vearch/vearch/internal/util/errutil"
	"github.com/vearch/vearch/internal/util/log"
	"github.com/vearch/vearch/internal/util/metrics/mserver"
	"github.com/vearch/vearch/internal/util/server/rpc/handler"
)

func ExportToRpcAdminHandler(server *Server) {
	initAdminHandler := &InitAdminHandler{server: server}

	psErrorChange := psErrorChange(server)

	if err := server.rpcServer.RegisterName(handler.NewChain(client.CreatePartitionHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &CreatePartitionHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.DeletePartitionHandler, handler.DefaultPanicHandler, psErrorChange, initAdminHandler, &DeletePartitionHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.DeleteReplicaHandler, handler.DefaultPanicHandler, psErrorChange, initAdminHandler, &DeleteReplicaHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.UpdatePartitionHandler, handler.DefaultPanicHandler, psErrorChange, initAdminHandler, &UpdatePartitionHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.IsLiveHandler, handler.DefaultPanicHandler, nil, initAdminHandler, new(IsLiveHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.PartitionInfoHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &PartitionInfoHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.StatsHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &StatsHandler{server: server}), ""); err != nil {
		panic(err)
	}
	if err := server.rpcServer.RegisterName(handler.NewChain(client.ChangeMemberHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &ChangeMemberHandler{server: server}), ""); err != nil {
		panic(err)
	}
	if err := server.rpcServer.RegisterName(handler.NewChain(client.EngineCfgHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &EngineCfgHandler{server: server}), ""); err != nil {
		panic(err)
	}
}

type InitAdminHandler struct {
	server *Server
}

func (i *InitAdminHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	if i.server.stopping.Get() {
		return vearchpb.NewError(vearchpb.ErrorEnum_SERVICE_UNAVAILABLE, nil)
	}
	return nil
}

type CreatePartitionHandler struct {
	server *Server
}

func (c *CreatePartitionHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	space := new(entity.Space)
	err := cbjson.Unmarshal(req.Data, space)
	if err != nil {
		log.Error("Create partition failed, err: [%s]", err.Error())
		return vearchpb.NewError(vearchpb.ErrorEnum_RPC_PARAM_ERROR, err)
	}
	c.server.partitions.Range(func(key, value interface{}) bool {
		fmt.Print(key, value)
		return true
	})

	if partitionStore := c.server.GetPartition(req.PartitionID); partitionStore != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_DUPLICATE, nil)
	}

	if err := c.server.CreatePartition(ctx, space, req.PartitionID); err != nil {
		c.server.DeletePartition(req.PartitionID)
		return err
	}
	return nil
}

type DeletePartitionHandler struct {
	server *Server
}

func (d *DeletePartitionHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	d.server.DeletePartition(req.PartitionID)
	return nil
}

type DeleteReplicaHandler struct {
	server *Server
}

func (d *DeleteReplicaHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	d.server.DeleteReplica(req.PartitionID)
	return nil
}

type UpdatePartitionHandler struct {
	server *Server
}

func (handler *UpdatePartitionHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}

	space := new(entity.Space)
	if err := cbjson.Unmarshal(req.Data, space); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_RPC_PARAM_ERROR, err)
	}

	store := handler.server.GetPartition(req.PartitionID)
	if store == nil {
		msg := fmt.Sprintf("partition not found, partitionId:[%d],nodeID:[%d]",
			req.PartitionID, handler.server.nodeID)
		log.Error("%s", msg)
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, errors.New(msg))
	}

	err := store.UpdateSpace(ctx, space)
	if err != nil {
		return err
	}

	return nil
}

type IsLiveHandler int

func (*IsLiveHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	return nil
}

type PartitionInfoHandler struct {
	server *Server
}

func (pih *PartitionInfoHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	pid := req.PartitionID

	stores := make([]PartitionStore, 0, 1)

	if pid != 0 {
		store := pih.server.GetPartition(pid)
		stores = append(stores, store)
	} else {
		pih.server.RangePartition(func(id entity.PartitionID, store PartitionStore) {
			stores = append(stores, store)
		})
	}

	pis := make([]*entity.PartitionInfo, 0, 1)
	for _, store := range stores {
		status := &engine.EngineStatus{}
		err := store.GetEngine().EngineStatus(status)
		if err != nil {
			return err
		}

		value := &entity.PartitionInfo{}
		value.PartitionID = pid
		value.DocNum = uint64(status.DocNum)
		value.Unreachable = store.GetUnreachable(uint64(pid))
		value.Status = store.GetPartition().GetStatus()
		value.IndexStatus = int(status.IndexStatus)
		value.IndexNum = int(status.MinIndexedNum)
		value.MaxDocid = int(status.MaxDocid)
		if req.Type == vearchpb.OpType_GET {
			value.Path = store.GetPartition().Path
			value.RaftStatus = store.Status()

			// size, err := store.GetEngine().Reader().Capacity(ctx)
			// if err != nil {
			// 	return err
			// }
			// value.Size = size
		}

		pis = append(pis, value)
	}
	if reply.Data, err = cbjson.Marshal(pis); err != nil {
		log.Error("marshal partition info failed, err: [%v]", err)
		return err
	}
	return nil
}

type StatsHandler struct {
	server *Server
}

func (sh *StatsHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	stats := mserver.NewServerStats()
	stats.ActiveConn = len(sh.server.rpcServer.ActiveClientConn())
	stats.PartitionInfos = make([]*entity.PartitionInfo, 0, 1)
	sh.server.RangePartition(func(pid entity.PartitionID, store PartitionStore) {
		defer func() {
			if e := recover(); e != nil {
				log.Error("go partiton has err:[%v]", e)
			}
		}()

		pi := &entity.PartitionInfo{PartitionID: pid}
		stats.PartitionInfos = append(stats.PartitionInfos, pi)

		docNum, err := store.GetEngine().Reader().DocCount(ctx)
		if err != nil {
			err = fmt.Errorf("got docCount from engine err:[%s]", err.Error())
			pi.Error = err.Error()
			return
		}

		// size, err := store.GetEngine().Reader().Capacity(ctx)
		// if err != nil {
		// 	err = fmt.Errorf("got capacity from engine err:[%s]", err.Error())
		// 	pi.Error = err.Error()
		// 	return
		// }

		pi.DocNum = docNum
		pi.Size = 0
		pi.Path = store.GetPartition().Path
		pi.Unreachable = store.GetUnreachable(uint64(pid))
		pi.Status = store.GetPartition().GetStatus()
		index_status, index_num, max_docid := store.GetEngine().IndexInfo()
		pi.IndexStatus = index_status
		pi.IndexNum = index_num
		pi.MaxDocid = max_docid
		pi.RaftStatus = store.Status()
	})

	if values, err := sonic.Marshal(stats); err != nil {
		log.Error("marshal partition info failed, err: [%v]", err)
		return err
	} else {
		reply.Data = values
	}
	return nil
}

type ChangeMemberHandler struct {
	server *Server
}

func (ch *ChangeMemberHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}

	reqObj := new(entity.ChangeMember)
	if err := cbjson.Unmarshal(req.Data, reqObj); err != nil {
		return err
	}

	store := ch.server.GetPartition(req.PartitionID)
	if store == nil {
		msg := fmt.Sprintf("partition not found, partitionId:[%d]", req.PartitionID)
		log.Error("%s", msg)
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, errors.New(msg))
	}

	if !store.IsLeader() {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_LEADER, nil)
	}

	server, err := ch.server.client.Master().QueryServer(ctx, reqObj.NodeID)
	if server == nil && reqObj.Method == proto.ConfRemoveNode {
		failServer := ch.server.client.Master().QueryFailServerByNodeID(ctx, reqObj.NodeID)
		if failServer != nil && failServer.Node != nil {
			server = failServer.Node
			log.Debug("get server by failserver record %v.", server)
			err = nil
		}
	}
	if err != nil {
		log.Error("get server info err %s", err.Error())
		return err
	}

	if reqObj.Method == proto.ConfAddNode {
		ch.server.raftResolver.AddNode(reqObj.NodeID, server.Replica())
	}

	if err := store.ChangeMember(reqObj.Method, server); err != nil {
		return err
	}

	if reqObj.Method == proto.ConfRemoveNode {
		ch.server.raftResolver.DeleteNode(reqObj.NodeID)
	}
	return nil
}

// it when has happen , redirect some other to response and send err to status
func psErrorChange(server *Server) handler.ErrorChangeFun {
	return func(ctx context.Context, err error, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
		if vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code == vearchpb.ErrorEnum_PARTITION_NOT_LEADER || err == raft.ErrNotLeader {
			store := server.GetPartition(reply.PartitionID)
			if store == nil {
				msg := fmt.Sprintf("partition not found, partitionId:[%d]", reply.PartitionID)
				log.Error("%s", msg)
				return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, errors.New(msg))
			}
			id, _ := store.GetLeader()
			if id == 0 {
				reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_PARTITION_NO_LEADER}
			} else {
				bytes, err := json.Marshal(server.raftResolver.ToReplica(id))
				if err != nil {
					log.Error("find raft resolver err[%s]", err.Error())
					return err
				}
				reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_PARTITION_NOT_LEADER, Msg: string(bytes)}
			}

			return nil
		}
		return err
	}
}

type EngineCfgHandler struct {
	server *Server
}

func (ch *EngineCfgHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	defer errutil.CatchError(&err)
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	// get store engine
	log.Debug("request pid [%+v]", req.PartitionID)
	partitonStore := ch.server.GetPartition(req.PartitionID)
	if partitonStore == nil {
		log.Debug("partitonStore is nil.")
		return fmt.Errorf("partition (%v), partitonStore is nil ", req.PartitionID)
	}
	engine := partitonStore.GetEngine()
	if engine == nil {
		return fmt.Errorf("partition (%v), engine is nil ", req.PartitionID)
	}
	if req.Type == vearchpb.OpType_CREATE {
		cacheCfg := new(entity.EngineCfg)
		if err := cbjson.Unmarshal(req.Data, cacheCfg); err != nil {
			errutil.ThrowError(err)
			return err
		}
		// invoke c interface
		log.Debug("cache cfg info is [%+v]", cacheCfg)
		cfg := &gamma.Config{}
		var CacheInfos []*gamma.CacheInfo
		if cacheCfg.CacheModels != nil {
			for _, model := range cacheCfg.CacheModels {
				cf := &gamma.CacheInfo{Name: model.Name, CacheSize: model.CacheSize}
				CacheInfos = append(CacheInfos, cf)
			}
		}
		cfg.CacheInfos = CacheInfos
		err := engine.SetEngineCfg(cfg)
		if err != nil {
			log.Debug("cache info set error [%+v]", err)
		}
	} else if req.Type == vearchpb.OpType_GET {
		// invoke c interface
		log.Debug("invoke cfg info is get")
		cfg := &gamma.Config{}
		err := engine.GetEngineCfg(cfg)
		if err != nil {
			log.Debug("cache info set error [%+v]", err)
		}
		var cacheModels []*entity.CacheModel
		if cfg.CacheInfos != nil {
			for _, cf := range cfg.CacheInfos {
				model := &entity.CacheModel{Name: cf.Name, CacheSize: cf.CacheSize}
				cacheModels = append(cacheModels, model)
			}
		}
		cacheCfg := new(entity.EngineCfg)
		cacheCfg.CacheModels = cacheModels
		data, _ := sonic.Marshal(cacheCfg)
		reply.Data = data
	}
	return nil
}
