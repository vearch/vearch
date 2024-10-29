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
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	"github.com/vearch/vearch/v3/internal/pkg/runtime/os"
	"github.com/vearch/vearch/v3/internal/pkg/server/rpc/handler"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
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
	if err := server.rpcServer.RegisterName(handler.NewChain(client.BackupHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &BackupHandler{server: server}), ""); err != nil {
		panic(err)
	}
	if err := server.rpcServer.RegisterName(handler.NewChain(client.ResourceLimitHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &ResourceLimitHandler{server: server}), ""); err != nil {
		panic(err)
	}
}

type InitAdminHandler struct {
	server *Server
}

func (i *InitAdminHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	if i.server.stopping {
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
	err := vjson.Unmarshal(req.Data, space)
	if err != nil {
		log.Error("Create partition failed, err: [%s]", err.Error())
		return vearchpb.NewError(vearchpb.ErrorEnum_RPC_PARAM_ERROR, err)
	}
	c.server.partitions.Range(func(key, value interface{}) bool {
		log.Debug("key %v, value %v", key, value)
		return true
	})

	if partitionStore := c.server.GetPartition(req.PartitionID); partitionStore != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_EXIST, nil)
	}

	if err := c.server.CreatePartition(ctx, space, req.PartitionID); err != nil {
		c.server.DeletePartition(req.PartitionID)
		log.Error(err)
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
	if err := vjson.Unmarshal(req.Data, space); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_RPC_PARAM_ERROR, err)
	}

	store := handler.server.GetPartition(req.PartitionID)
	if store == nil {
		msg := fmt.Sprintf("partition not found, partitionId:[%d], nodeID:[%d], node ip:[%s]",
			req.PartitionID, handler.server.nodeID, handler.server.ip)
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
		status := &entity.EngineStatus{}
		err := store.GetEngine().GetEngineStatus(status)
		if err != nil {
			return err
		}

		value := &entity.PartitionInfo{}
		value.PartitionID = store.GetPartition().Id
		value.DocNum = uint64(status.DocNum)
		value.Unreachable = store.GetUnreachable(uint64(store.GetPartition().Id))
		value.Status = store.GetPartition().GetStatus()
		value.IndexStatus = int(status.IndexStatus)
		value.BackupStatus = int(status.BackupStatus)
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
	if reply.Data, err = vjson.Marshal(pis); err != nil {
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

	if values, err := vjson.Marshal(stats); err != nil {
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
	if err := vjson.Unmarshal(req.Data, reqObj); err != nil {
		return err
	}

	store := ch.server.GetPartition(req.PartitionID)
	if store == nil {
		msg := fmt.Sprintf("partition not found, partitionId:[%d], nodeID:[%d], node ip:[%s]", req.PartitionID, ch.server.nodeID, ch.server.ip)
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

// redirect some other to response and send err to status when happen
func psErrorChange(server *Server) handler.ErrorChangeFun {
	return func(ctx context.Context, err error, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
		if vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code == vearchpb.ErrorEnum_PARTITION_NOT_LEADER || err == raft.ErrNotLeader {
			store := server.GetPartition(req.PartitionID)
			if store == nil {
				msg := fmt.Sprintf("partition not found, partitionId:[%d], nodeID:[%d], node ip:[%s]", req.PartitionID, server.nodeID, server.ip)
				log.Error("%s", msg)
				return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, errors.New(msg))
			}
			id, _ := store.GetLeader()
			if id == 0 {
				reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_PARTITION_NO_LEADER}
			} else {
				bytes, err := vjson.Marshal(server.raftResolver.ToReplica(id))
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
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, fmt.Errorf("partition (%v), partitonStore is nil ", req.PartitionID))
	}
	engine := partitonStore.GetEngine()
	if engine == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, fmt.Errorf("partition (%v), engine is nil ", req.PartitionID))
	}
	if req.Type == vearchpb.OpType_CREATE {
		err := engine.SetEngineCfg(req.Data)
		if err != nil {
			log.Debug("cache info set error [%+v]", err)
		}
	} else if req.Type == vearchpb.OpType_GET {
		// invoke c interface
		log.Debug("invoke cfg info is get")
		cfg := &entity.EngineConfig{}
		err := engine.GetEngineCfg(cfg)
		if err != nil {
			log.Debug("cache info set error [%+v]", err)
		}
		data, _ := vjson.Marshal(cfg)
		reply.Data = data
	}
	return nil
}

type BackupHandler struct {
	server *Server
}

func (bh *BackupHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	defer errutil.CatchError(&err)
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	// get store engine
	log.Debug("request pid [%+v]", req.PartitionID)

	partitonStore := bh.server.GetPartition(req.PartitionID)
	if partitonStore == nil {
		log.Debug("partitonStore is nil.")
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, fmt.Errorf("partition (%v), partitonStore is nil ", req.PartitionID))
	}
	e := partitonStore.GetEngine()
	if e == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, fmt.Errorf("partition (%v), engine is nil ", req.PartitionID))
	}

	status := &entity.EngineStatus{}
	err = e.GetEngineStatus(status)

	if err != nil {
		log.Error("get engine status error [%+v]", err)
		return
	}
	if status.BackupStatus != 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("backup status %d", status.BackupStatus))
	}

	backup := new(entity.BackupSpace)
	if err := vjson.Unmarshal(req.Data, backup); err != nil {
		errutil.ThrowError(err)
		return err
	}
	if backup.Command != "create" && backup.Command != "restore" {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unknow command %s", backup.Command))
	}
	space := partitonStore.GetSpace()
	dbName, err := bh.server.client.Master().QueryDBId2Name(ctx, space.DBId)
	if err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("find db by id err: %s, data: %d", err.Error(), space.DBId))
	}

	engineConfig := entity.EngineConfig{}
	err = e.GetEngineCfg(&engineConfig)
	if err != nil {
		log.Error("get engine config error [%+v]", err)
		return
	}

	backupFileName := *engineConfig.Path + "/backup/raw_data.json.zst"

	minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
		Secure: backup.S3Param.UseSSL,
	})
	if err != nil {
		log.Error("failed to create minio client: %+v", err)
		return
	}
	bucketName := backup.S3Param.BucketName
	objectName := fmt.Sprintf("%s/%s/%d.json.zst", dbName, space.Name, backup.Part)
	log.Info("objectName %s", objectName)

	if backup.Command == "create" {
		err = e.BackupSpace(backup.Command)
		if err != nil {
			log.Error("backup error [%+v]", err)
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err)
		}

		go func() {
			status := &entity.EngineStatus{}
			err = e.GetEngineStatus(status)

			if err != nil {
				log.Error("get engine status error [%+v]", err)
				return
			}
			for status.BackupStatus != 0 {
				log.Debug("status.BackupStatus %d", status.BackupStatus)
				time.Sleep(2 * time.Second)
				status = &entity.EngineStatus{}
				err = e.GetEngineStatus(status)
				if err != nil {
					log.Error("get engine status error [%+v]", err)
					return
				}
			}

			_, err = minioClient.FPutObject(context.Background(), bucketName, objectName, backupFileName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err != nil {
				log.Error("failed to backup space: %+v", err)
				return
			}
			log.Info("backup success, file is [%s]", backupFileName)
		}()
	} else if backup.Command == "restore" {
		go func() {
			err = minioClient.FGetObject(context.Background(), bucketName, objectName, backupFileName, minio.GetObjectOptions{})
			if err != nil {
				log.Error("failed to download file from S3: %+v", err)
				return
			}
			log.Info("downloaded backup file from S3: %s", backupFileName)

			err = e.BackupSpace(backup.Command)
			if err != nil {
				log.Error("failed to restore space: %+v", err)
				return
			}
			log.Info("space restored successfully")
		}()
	}
	return nil
}

type ResourceLimitHandler struct {
	server *Server
}

func (rlh *ResourceLimitHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	defer errutil.CatchError(&err)
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}

	partitonStore := rlh.server.GetPartition(req.PartitionID)
	if partitonStore == nil {
		log.Debug("partitonStore is nil, pid %d not found", req.PartitionID)
		return nil
	}

	resourceLimit := new(entity.ResourceLimit)
	if err := vjson.Unmarshal(req.Data, resourceLimit); err != nil {
		return err
	}
	// check resource or set
	if resourceLimit.ResourceExhausted != nil {
		partitonStore.GetPartition().ResourceExhausted = *resourceLimit.ResourceExhausted
	} else {
		if resource_exhausted, err := os.CheckResource(partitonStore.GetPartition().Path); err != nil {
			return err
		} else {
			partitonStore.GetPartition().ResourceExhausted = resource_exhausted
		}
	}
	log.Debug("partition %d set ResourceExhausted as %v", req.PartitionID, partitonStore.GetPartition().ResourceExhausted)
	return nil
}
