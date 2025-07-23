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

package services

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type MemberService struct {
	client *client.Client
}

func NewMemberService(client *client.Client) *MemberService {
	return &MemberService{client: client}
}

func (s *MemberService) AddMember(ctx context.Context, peerAddrs []string) (resp *clientv3.MemberAddResponse, err error) {
	mc := s.client.Master()
	resp, err = mc.MemberAdd(ctx, peerAddrs)
	if err != nil {
		log.Error("add masters member err:%s", err.Error())
		return nil, err
	}
	return resp, nil
}

func (s *MemberService) RemoveMember(ctx context.Context, master *entity.MemberInfoRequest) (resp *clientv3.MemberRemoveResponse, err error) {
	mc := s.client.Master()
	membersResp, err := mc.MemberList(ctx)
	if err != nil {
		log.Error("master member list err:%s", err.Error())
		return nil, err
	}
	if len(membersResp.Members) <= 1 {
		msg := fmt.Sprintf("master member only have %d, cann't remove member now", len(membersResp.Members))
		log.Error(msg)
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(msg))
	}

	found := false
	for _, member := range membersResp.Members {
		if member.ID == master.ID && member.Name == master.Name {
			found = true
			break
		}
	}
	if !found {
		msg := fmt.Sprintf("master member name:%s id:%d not found", master.Name, master.ID)
		log.Error(msg)
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(msg))
	}
	resp, err = mc.MemberRemove(ctx, master.ID)
	if err != nil {
		msg := fmt.Sprintf("remove master member err:%s", err.Error())
		log.Error(msg)
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(msg))
	}

	err = mc.STM(context.Background(), func(stm concurrency.STM) error {
		stm.Del(entity.MasterMemberKey(master.ID))
		return nil
	})
	if err != nil {
		log.Error("del masters err:%s", err.Error())
		return nil, err
	}
	// TODO other master client should also do member sync
	err = mc.MemberSync(ctx)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *MemberService) ChangeMember(ctx context.Context, spaceService *SpaceService, cm *entity.ChangeMember) error {
	mc := s.client.Master()
	partition, err := mc.QueryPartition(ctx, cm.PartitionID)
	if err != nil {
		log.Error(err)
		return err
	}

	space, err := mc.QuerySpaceByID(ctx, partition.DBId, partition.SpaceId)
	if err != nil {
		return err
	}

	dbName, err := mc.QueryDBId2Name(ctx, space.DBId)
	if err != nil {
		return err
	}

	spacePartition := space.GetPartition(cm.PartitionID)

	if cm.Method != proto.ConfRemoveNode {
		if slices.Contains(spacePartition.Replicas, cm.NodeID) {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition:[%d] already on server:[%d] in replicas:[%v]", cm.PartitionID, cm.NodeID, spacePartition.Replicas))
		}
		spacePartition.Replicas = append(spacePartition.Replicas, cm.NodeID)
	} else {
		tempIDs := make([]entity.NodeID, 0, len(spacePartition.Replicas)-1)

		for _, id := range spacePartition.Replicas {
			if id != cm.NodeID {
				tempIDs = append(tempIDs, id)
			}
		}
		spacePartition.Replicas = tempIDs
	}

	retryTimes := 10
	psNode, err := mc.QueryServer(ctx, partition.LeaderID)
	for err != nil {
		partition, err = mc.QueryPartition(ctx, cm.PartitionID)
		if err != nil {
			log.Error(err)
			return err
		}
		psNode, err = mc.QueryServer(ctx, partition.LeaderID)
		if err != nil {
			retryTimes--
			if retryTimes == 0 {
				return err
			}
			time.Sleep(10 * time.Second)
		}
	}
	log.Info("psNode is [%+v], cm is [%+v] ", psNode, cm)

	var targetNode *entity.Server
	targetNode, err = mc.QueryServer(ctx, cm.NodeID)
	if err != nil {
		if cm.Method == proto.ConfRemoveNode {
			// maybe node is crashed
			targetNode = nil
		} else {
			return err
		}
	}
	log.Info("targetNode is [%+v], cm is [%+v] ", targetNode, cm)

	if !client.IsLive(psNode.RpcAddr()) {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_SERVER_ERROR, fmt.Errorf("server:[%d] addr:[%s] can not connect ", cm.NodeID, psNode.RpcAddr()))
	}

	if cm.Method == proto.ConfAddNode && targetNode != nil {
		if err := client.CreatePartition(targetNode.RpcAddr(), space, cm.PartitionID); err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partiiton has err:[%s] addr:[%s]", err.Error(), targetNode.RpcAddr()))
		}
	} else if cm.Method == proto.ConfRemoveNode {

	} else {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("change member only support add:[%d] remove:[%d] not support:[%d]", proto.ConfAddNode, proto.ConfRemoveNode, cm.Method))
	}

	if err := client.ChangeMember(psNode.RpcAddr(), cm); err != nil {
		return err
	}
	if cm.Method == proto.ConfRemoveNode && targetNode != nil && client.IsLive(targetNode.RpcAddr()) {
		if err := client.DeleteReplica(targetNode.RpcAddr(), cm.PartitionID); err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("delete partiiton has err:[%s] addr:[%s]", err.Error(), targetNode.RpcAddr()))
		}
	}

	if _, err := spaceService.UpdateSpace(ctx, nil, dbName, space.Name, space, "config"); err != nil {
		return err
	}
	log.Info("update space: %v", space)
	return nil
}

func (s *MemberService) ChangeMembers(ctx context.Context, spaceService *SpaceService, cms *entity.ChangeMembers) error {
	for _, partitionId := range cms.PartitionIDs {
		cm := &entity.ChangeMember{
			PartitionID: uint32(partitionId),
			NodeID:      cms.NodeID,
			Method:      cms.Method,
		}
		if err := s.ChangeMember(ctx, spaceService, cm); err != nil {
			return err
		}
	}
	return nil
}

// Check if partition.Id is in s.PartitionIds
func isPartitionIdInSlice[T comparable](partitionId T, partitionIds []T) (bool, int) {
	for i, v := range partitionIds {
		if v == partitionId {
			return true, i
		}
	}
	return false, -1
}

// change replicas, add or delete
func (s *MemberService) ChangeReplica(ctx context.Context, dbService *DBService, spaceService *SpaceService, dbModify *entity.DBModify) (e error) {
	// panic process
	defer errutil.CatchError(&e)
	mc := s.client.Master()
	// query server
	servers, err := dbService.DBServers(ctx, dbModify.DbName)
	errutil.ThrowError(err)
	// generate change servers
	dbID, err := mc.QueryDBName2ID(ctx, dbModify.DbName)
	errutil.ThrowError(err)
	space, err := mc.QuerySpaceByName(ctx, dbID, dbModify.SpaceName)
	errutil.ThrowError(err)
	if dbModify.Method == proto.ConfAddNode && (int(space.ReplicaNum)+1) > len(servers) {
		err := vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("ReplicaNum [%d] exceeds server size [%d]",
			int(space.ReplicaNum)+1, len(servers)))
		return err
	}
	// change space replicas of partition, add or delete one
	changeServer := make([]*entity.ChangeMember, 0)
	for _, partition := range space.Partitions {
		// sort servers, low to high
		sort.Slice(servers, func(i, j int) bool {
			return len(servers[i].PartitionIds) < len(servers[j].PartitionIds)
		})
		// change server
		for _, s := range servers {
			if dbModify.Method == proto.ConfAddNode {
				exist, _ := isPartitionIdInSlice(s.ID, partition.Replicas)
				if !exist {
					// server doesn't contain this partition, then create it
					cm := &entity.ChangeMember{PartitionID: partition.Id, NodeID: s.ID, Method: dbModify.Method}
					changeServer = append(changeServer, cm)
					s.PartitionIds = append(s.PartitionIds, partition.Id)
					break
				}
			} else if dbModify.Method == proto.ConfRemoveNode {
				exist, index := isPartitionIdInSlice(partition.Id, s.PartitionIds)
				if exist {
					// server contains this partition, then remove it
					cm := &entity.ChangeMember{PartitionID: partition.Id, NodeID: s.ID, Method: dbModify.Method}
					changeServer = append(changeServer, cm)
					s.PartitionIds = append(s.PartitionIds[:index], s.PartitionIds[index+1:]...)
					break
				}
			}
		}
	}

	changeServerStr, _ := json.Marshal(changeServer)
	log.Info("need to change partition is [%+v] ", changeServerStr)
	// sleep time
	sleepTime := config.Conf().PS.RaftHeartbeatInterval
	// change partition
	for _, cm := range changeServer {
		if e = s.ChangeMember(ctx, spaceService, cm); e != nil {
			info := fmt.Sprintf("changing partition member [%+v] failed, error is %s ", cm, e)
			log.Error(info)
			panic(fmt.Errorf(info))
		}
		log.Info("changing partition member [%+v] succeeded ", cm)
		if dbModify.Method == proto.ConfRemoveNode {
			time.Sleep(time.Duration(sleepTime*10) * time.Millisecond)
			log.Info("remove partition sleep [%+d] milliseconds", sleepTime)
		}
	}
	log.Info("all partition member changes succeeded")

	// update space ReplicaNum, it will lock cluster, to create space
	mutex := mc.NewLock(ctx, entity.LockSpaceKey(dbModify.DbName, dbModify.SpaceName), time.Second*300)
	if _, err := mutex.TryLock(); err != nil {
		errutil.ThrowError(err)
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("failed to unlock space, the error is: %v ", err)
		}
	}()
	space, err = mc.QuerySpaceByName(ctx, dbID, dbModify.SpaceName)
	errutil.ThrowError(err)
	switch dbModify.Method {
	case proto.ConfAddNode:
		space.ReplicaNum = space.ReplicaNum + 1
	case proto.ConfRemoveNode:
		space.ReplicaNum = space.ReplicaNum - 1
	}
	err = spaceService.UpdateSpaceData(ctx, space)
	log.Info("updateSpace space [%+v] succeeded", space)
	errutil.ThrowError(err)
	return e
}
