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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine/mapping"
)

const (
	DB                  = "db"
	dbName              = "db_name"
	spaceName           = "space_name"
	aliasName           = "alias_name"
	userName            = "user_name"
	roleName            = "role_name"
	memberId            = "member_id"
	peerAddrs           = "peer_addrs"
	headerAuthKey       = "Authorization"
	NodeID              = "node_id"
	DefaultResourceName = "default"
)

type SpaceService struct {
	client *client.Client
}

func NewSpaceService(client *client.Client) *SpaceService {
	return &SpaceService{client: client}
}

func (s *SpaceService) CreateSpace(ctx context.Context, dbs *DBService, dbName string, space *entity.Space) (err error) {
	mc := s.client.Master()
	if space.DBId, err = mc.QueryDBName2ID(ctx, dbName); err != nil {
		log.Error("find DbId according to DbName:%v failed, error: %v", dbName, err)
		return err
	}

	// to validate schema
	_, err = mapping.SchemaMap(space.Fields)
	if err != nil {
		log.Error("master service createSpaceService error: %v", err)
		return err
	}

	// it will lock cluster to create space
	mutex := mc.NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*300)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock space err:[%s]", err.Error())
		}
	}()

	// spaces is existed
	if _, err := mc.QuerySpaceByName(ctx, space.DBId, space.Name); err != nil {
		vErr := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err)
		if vErr.GetError().Code != vearchpb.ErrorEnum_SPACE_NOT_EXIST {
			return vErr
		}
	} else {
		return vearchpb.NewError(vearchpb.ErrorEnum_SPACE_EXIST, nil)
	}

	spaceStr, _ := json.Marshal(space)

	log.Info("create space, db: %s, spaceName: %s, space :[%s]", dbName, space.Name, spaceStr)

	// find all servers for create space
	servers, err := mc.QueryServers(ctx)
	if err != nil {
		return err
	}

	// generate space id
	spaceID, err := mc.NewIDGenerate(ctx, entity.SpaceIdSequence, 1, 5*time.Second)
	if err != nil {
		return err
	}
	space.Id = spaceID

	spaceProperties, err := entity.UnmarshalPropertyJSON(space.Fields)
	if err != nil {
		return err
	}

	space.SpaceProperties = spaceProperties
	for _, f := range spaceProperties {
		if f.FieldType == vearchpb.FieldType_VECTOR && f.Index != nil {
			space.Index = f.Index
		}
	}

	if space.PartitionRule != nil {
		err := space.PartitionRule.Validate(space, true)
		if err != nil {
			return err
		}
		width := math.MaxUint32 / (space.PartitionNum * space.PartitionRule.Partitions)
		for i := range space.PartitionNum * space.PartitionRule.Partitions {
			partitionID, err := mc.NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

			if err != nil {
				return err
			}

			space.Partitions = append(space.Partitions, &entity.Partition{
				Id:      entity.PartitionID(partitionID),
				Name:    space.PartitionRule.Ranges[i/space.PartitionNum].Name,
				SpaceId: space.Id,
				DBId:    space.DBId,
				Slot:    entity.SlotID(i * width),
			})
		}
	} else {
		width := math.MaxUint32 / space.PartitionNum
		for i := range space.PartitionNum {
			partitionID, err := mc.NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

			if err != nil {
				return err
			}

			space.Partitions = append(space.Partitions, &entity.Partition{
				Id:      entity.PartitionID(partitionID),
				SpaceId: space.Id,
				DBId:    space.DBId,
				Slot:    entity.SlotID(i * width),
			})
		}
	}

	serverPartitions, err := s.filterAndSortServer(ctx, dbs, space, servers)
	if err != nil {
		return err
	}

	if int(space.ReplicaNum) > len(serverPartitions) {
		return fmt.Errorf("not enough partition servers, need %d replicas but only have %d",
			int(space.ReplicaNum), len(serverPartitions))
	}

	bFlase := false
	space.Enabled = &bFlase
	defer func() {
		if !(*space.Enabled) { // remove the space if it is still not enabled
			if e := mc.Delete(context.Background(), entity.SpaceKey(space.DBId, space.Id)); e != nil {
				log.Error("to delete space err: %s", e.Error())
			}
		}
	}()

	marshal, err := json.Marshal(space)
	if err != nil {
		return err
	}
	if space.Index == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space vector field index should not be empty"))
	}
	err = mc.Create(ctx, entity.SpaceKey(space.DBId, space.Id), marshal)
	if err != nil {
		return err
	}

	// pick servers for space
	var pAddrs [][]string
	for i := range space.Partitions {
		if addrs, err := s.selectServersForPartition(servers, serverPartitions, space.ReplicaNum, space.Partitions[i]); err != nil {
			return err
		} else {
			pAddrs = append(pAddrs, addrs)
		}
	}

	var errChain = make(chan error, 1)
	// send create space request to partition server
	for i := 0; i < len(space.Partitions); i++ {
		go func(addrs []string, partition *entity.Partition) {
			defer func() {
				if r := recover(); r != nil {
					err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %v", r))
					errChain <- err
					log.Error(err.Error())
				}
			}()
			for _, addr := range addrs {
				if err := client.CreatePartition(addr, space, partition.Id); err != nil {
					err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %s", err.Error()))
					errChain <- err
					log.Error(err.Error())
				}
			}
		}(pAddrs[i], space.Partitions[i])
	}

	// check all partition is ok
	for i := range space.Partitions {
		v := 0
		for {
			v++
			select {
			case err := <-errChain:
				return err
			case <-ctx.Done():
				return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create space has error"))
			default:
			}

			partition, err := mc.QueryPartition(ctx, space.Partitions[i].Id)
			if v%5 == 0 {
				log.Debug("check the partition:%d status", space.Partitions[i].Id)
			}
			if err != nil && vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code != vearchpb.ErrorEnum_PARTITION_NOT_EXIST {
				return err
			}
			if partition != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	bTrue := true
	space.Enabled = &bTrue

	// update version
	err = s.UpdateSpaceData(ctx, space)
	if err != nil {
		bFalse := false
		space.Enabled = &bFalse
		return err
	}

	return nil
}

func (s *SpaceService) DeleteSpace(ctx context.Context, as *AliasService, dbName, spaceName string) error {
	mc := s.client.Master()
	dbId, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return err
	}

	space, err := mc.QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		return err
	}
	if space == nil { // nil if it not exists
		return nil
	}

	mutex := mc.NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*60)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock space err:[%s]", err.Error())
		}
	}()
	// delete key
	err = mc.Delete(ctx, entity.SpaceKey(dbId, space.Id))
	if err != nil {
		return err
	}

	// delete parition and partitionKey
	for _, p := range space.Partitions {
		for _, replica := range p.Replicas {
			if server, err := mc.QueryServer(ctx, replica); err != nil {
				log.Error("query partition:[%d] for replica:[%s] has err:[%s]", p.Id, replica, err.Error())
			} else {
				if err := client.DeletePartition(server.RpcAddr(), p.Id); err != nil {
					log.Error("delete partition:[%d] for server:[%s] has err:[%s]", p.Id, server.RpcAddr(), err.Error())
				}
			}
		}
		err = mc.Delete(ctx, entity.PartitionKey(p.Id))
		if err != nil {
			return err
		}
	}

	// delete alias
	if aliases, err := as.QueryAllAlias(ctx); err != nil {
		return err
	} else {
		for _, alias := range aliases {
			if alias.DbName == dbName && alias.SpaceName == spaceName {
				if err := as.DeleteAlias(ctx, alias.Name); err != nil {
					return err
				}
			}
		}
	}
	err = mc.Delete(ctx, entity.SpaceConfigKey(dbId, space.Id))
	if err != nil {
		return err
	}

	return nil
}

func (s *SpaceService) DescribeSpace(ctx context.Context, space *entity.Space, spaceInfo *entity.SpaceInfo, detail_info bool) (int, error) {
	spaceStatus := 0
	color := []string{"green", "yellow", "red"}
	spaceInfo.Errors = make([]string, 0)
	mc := s.client.Master()

	// check partition num in meta data
	if space.PartitionRule != nil {
		if len(space.Partitions) != int(space.PartitionNum*space.PartitionRule.Partitions) {
			msg := fmt.Sprintf("space: [%s] partitions length:[%d] not equal to partition num:[%d] * PartitionRule.Partitions: [%d]", space.Name, len(space.Partitions), space.PartitionNum, space.PartitionRule.Partitions)
			spaceInfo.Errors = append(spaceInfo.Errors, msg)
			log.Error(msg)
			spaceStatus = 2
		}
	} else {
		if len(space.Partitions) != int(space.PartitionNum) {
			msg := fmt.Sprintf("space: [%s] partitions length:[%d] not equal to partition num:[%d]", space.Name, len(space.Partitions), space.PartitionNum)
			spaceInfo.Errors = append(spaceInfo.Errors, msg)
			log.Error(msg)
			spaceStatus = 2
		}
	}

	for _, spacePartition := range space.Partitions {
		p, err := mc.QueryPartition(ctx, spacePartition.Id)
		pStatus := 0

		if err != nil {
			msg := fmt.Sprintf("partition:[%d] in space: [%s] not found in meta data", spacePartition.Id, space.Name)
			spaceInfo.Errors = append(spaceInfo.Errors, msg)
			log.Error(msg)
			pStatus = 2
			if pStatus > spaceStatus {
				spaceStatus = pStatus
			}
			continue
		}

		nodeID := p.LeaderID
		if nodeID == 0 {
			log.Error("partition:[%d] in space: [%s] leaderID is 0", spacePartition.Id, space.Name)
			if len(p.Replicas) > 0 {
				nodeID = p.Replicas[0]
			}
		}

		server, err := mc.QueryServer(ctx, nodeID)
		if err != nil {
			msg := fmt.Sprintf("space: [%s] partition:[%d], server:[%d] not found", space.Name, spacePartition.Id, nodeID)
			spaceInfo.Errors = append(spaceInfo.Errors, msg)
			log.Error(msg)
			pStatus = 2
			if pStatus > spaceStatus {
				spaceStatus = pStatus
			}
			continue
		}

		partitionInfo, err := client.PartitionInfo(server.RpcAddr(), p.Id, detail_info)
		if err != nil {
			msg := fmt.Sprintf("query space:[%s] server:[%d] partition:[%d] info err :[%s]", space.Name, nodeID, spacePartition.Id, err.Error())
			spaceInfo.Errors = append(spaceInfo.Errors, msg)
			log.Error(msg)
			partitionInfo = &entity.PartitionInfo{}
			pStatus = 2
		} else {
			if len(partitionInfo.Unreachable) > 0 {
				pStatus = 1
			}
		}

		replicasStatus := make(map[entity.NodeID]string)
		for nodeID, status := range p.ReStatusMap {
			if status == entity.ReplicasOK {
				replicasStatus[nodeID] = "ReplicasOK"
			} else {
				replicasStatus[nodeID] = "ReplicasNotReady"
			}
		}

		if partitionInfo.RaftStatus != nil {
			if partitionInfo.RaftStatus.Leader == 0 {
				msg := fmt.Sprintf("partition:[%d] in space:[%s] has no leader", spacePartition.Id, space.Name)
				spaceInfo.Errors = append(spaceInfo.Errors, msg)
				log.Error(msg)
				pStatus = 2
			} else {
				if len(partitionInfo.RaftStatus.Replicas) != int(space.ReplicaNum) {
					msg := fmt.Sprintf("partition:[%d] in space:[%s] replicas: [%d] is not equal to replicaNum: [%d]", spacePartition.Id, space.Name, len(partitionInfo.RaftStatus.Replicas), space.ReplicaNum)
					spaceInfo.Errors = append(spaceInfo.Errors, msg)
					log.Error(msg)
					pStatus = 2
				} else {
					replicaStateProbeNum := 0
					leaderId := 0
					for nodeID, replica := range partitionInfo.RaftStatus.Replicas {
						// TODO FIXME: when leader changed, the unreachableNodeIDnre state may still be ReplicaStateProbe
						isNodeUnreachable := false
						for _, unreachableNodeID := range partitionInfo.Unreachable {
							if nodeID == unreachableNodeID {
								isNodeUnreachable = true
								break
							}
						}
						if isNodeUnreachable {
							continue
						}
						if replica.State == entity.ReplicaStateProbe {
							replicaStateProbeNum += 1
							leaderId = int(nodeID)
						}
					}
					if replicaStateProbeNum != 1 {
						msg := fmt.Sprintf("partition:[%d] in space:[%s] have [%d] leader", spacePartition.Id, space.Name, replicaStateProbeNum)
						spaceInfo.Errors = append(spaceInfo.Errors, msg)
						log.Error(msg)
						pStatus = 2
					}
					if leaderId != int(partitionInfo.RaftStatus.Leader) {
						msg := fmt.Sprintf("partition:[%d] in space:[%s] leader: [%d] is not equal to raft leader: [%d]", spacePartition.Id, space.Name, leaderId, partitionInfo.RaftStatus.Leader)
						spaceInfo.Errors = append(spaceInfo.Errors, msg)
						log.Error(msg)
						pStatus = 2
					}
				}
			}
		}

		//this must from space.Partitions
		partitionInfo.PartitionID = spacePartition.Id
		partitionInfo.Name = spacePartition.Name
		partitionInfo.Color = color[pStatus]
		partitionInfo.ReplicaNum = len(p.Replicas)
		partitionInfo.Ip = server.Ip
		partitionInfo.NodeID = server.ID
		partitionInfo.RepStatus = replicasStatus

		spaceInfo.Partitions = append(spaceInfo.Partitions, partitionInfo)

		if pStatus > spaceStatus {
			spaceStatus = pStatus
		}
	}

	docNum := uint64(0)
	for _, p := range spaceInfo.Partitions {
		docNum += cast.ToUint64(p.DocNum)
	}
	spaceInfo.Status = color[spaceStatus]
	spaceInfo.DocNum = docNum
	return spaceStatus, nil
}

func (s *SpaceService) filterAndSortServer(ctx context.Context, dbs *DBService, space *entity.Space, servers []*entity.Server) (map[int]int, error) {
	db, err := dbs.QueryDB(ctx, cast.ToString(space.DBId))
	if err != nil {
		return nil, err
	}

	var psMap map[string]bool
	if len(db.Ps) > 0 {
		psMap = make(map[string]bool)
		for _, ps := range db.Ps {
			psMap[ps] = true
		}
	}

	serverPartitions := make(map[int]int)

	mc := s.client.Master()
	spaces, err := mc.QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		return nil, err
	}

	serverIndex := make(map[entity.NodeID]int)

	if psMap == nil { // If psMap is nil, only use public servers
		for i, s := range servers {
			// Only use servers with the same resource name
			if s.ResourceName != space.ResourceName {
				continue
			}
			if !s.Private {
				serverPartitions[i] = 0
				serverIndex[s.ID] = i
			}
		}
	} else { // If psMap is not nil, only use defined servers
		for i, s := range servers {
			// Only use servers with the same resource name
			if s.ResourceName != space.ResourceName {
				psMap[s.Ip] = false
				continue
			}
			if psMap[s.Ip] {
				serverPartitions[i] = 0
				serverIndex[s.ID] = i
			}
		}
	}

	for _, space := range spaces {
		for _, partition := range space.Partitions {
			for _, nodeID := range partition.Replicas {
				if index, ok := serverIndex[nodeID]; ok {
					serverPartitions[index] = serverPartitions[index] + 1
				}
			}
		}
	}

	return serverPartitions, nil
}

func (s *SpaceService) UpdateSpaceResource(ctx context.Context, dbs *DBService, spaceResource *entity.SpacePartitionResource) (*entity.Space, error) {
	// it will lock cluster, to update space
	mc := s.client.Master()
	mutex := mc.NewLock(ctx, entity.LockSpaceKey(spaceResource.DbName, spaceResource.SpaceName), time.Second*300)
	if err := mutex.Lock(); err != nil {
		return nil, err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("failed to unlock space, the Error is:%v ", err)
		}
	}()

	dbId, err := mc.QueryDBName2ID(ctx, spaceResource.DbName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("failed to find database id according database name:%v,the Error is:%v ", spaceResource.DbName, err))
	}

	space, err := mc.QuerySpaceByName(ctx, dbId, spaceResource.SpaceName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("failed to find space according space name:%v,the Error is:%v ", spaceResource.SpaceName, err))
	}

	if space == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("can not found space by name : %s", spaceResource.SpaceName))
	}

	if spaceResource.PartitionOperatorType != "" {
		if spaceResource.PartitionOperatorType != entity.Add && spaceResource.PartitionOperatorType != entity.Drop {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition operator type should be %s or %s, but is %s", entity.Add, entity.Drop, spaceResource.PartitionOperatorType))
		}
		return s.updateSpacePartitonRule(ctx, dbs, spaceResource, space)
	}

	// now only support update partition num
	if space.PartitionNum >= spaceResource.PartitionNum {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("paritition_num: %d now should greater than origin space partition_num: %d", spaceResource.PartitionNum, space.PartitionNum))
	}

	partitions := make([]*entity.Partition, 0)
	for i := space.PartitionNum; i < spaceResource.PartitionNum; i++ {
		partitionID, err := mc.NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

		if err != nil {
			return nil, err
		}

		partitions = append(partitions, &entity.Partition{
			Id:      entity.PartitionID(partitionID),
			SpaceId: space.Id,
			DBId:    space.DBId,
		})
		log.Debug("updateSpacePartitionNum Generate partition id %d", partitionID)
	}

	// find all servers for update space partition
	servers, err := mc.QueryServers(ctx)
	if err != nil {
		return nil, err
	}

	// will get all exist partition
	serverPartitions, err := s.filterAndSortServer(ctx, dbs, space, servers)
	if err != nil {
		return nil, err
	}

	if int(space.ReplicaNum) > len(serverPartitions) {
		return nil, fmt.Errorf("not enough PS , need replica %d but only has %d",
			int(space.ReplicaNum), len(serverPartitions))
	}

	// pick servers for space
	var paddrs [][]string
	for i := 0; i < len(partitions); i++ {
		if addrs, err := s.selectServersForPartition(servers, serverPartitions, space.ReplicaNum, partitions[i]); err != nil {
			return nil, err
		} else {
			paddrs = append(paddrs, addrs)
		}
	}

	log.Debug("updateSpacePartitionNum origin paritionNum %d, serverPartitions %v, paddrs %v", space.PartitionNum, serverPartitions, paddrs)

	// when create partition, new partition id will be stored in server partition cache
	space.PartitionNum = spaceResource.PartitionNum
	space.Partitions = append(space.Partitions, partitions...)

	var errChain = make(chan error, 1)
	// send create partition for new
	for i := 0; i < len(partitions); i++ {
		go func(addrs []string, partition *entity.Partition) {
			//send request for all server
			defer func() {
				if r := recover(); r != nil {
					err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %v ", r))
					errChain <- err
					log.Error(err.Error())
				}
			}()
			for _, addr := range addrs {
				if err := client.CreatePartition(addr, space, partition.Id); err != nil {
					err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %s ", err.Error()))
					errChain <- err
					log.Error(err.Error())
				}
			}
		}(paddrs[i], partitions[i])
	}

	// check all partition is ok
	for i := 0; i < len(partitions); i++ {
		times := 0
		for {
			times++
			select {
			case err := <-errChain:
				return nil, err
			case <-ctx.Done():
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("update space has error"))
			default:
			}

			partition, err := mc.QueryPartition(ctx, partitions[i].Id)
			if times%5 == 0 {
				log.Debug("updateSpacePartitionNum check the partition:%d status", partitions[i].Id)
			}
			if err != nil && vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code != vearchpb.ErrorEnum_PARTITION_NOT_EXIST {
				return nil, err
			}
			if partition != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	//update space
	width := math.MaxUint32 / space.PartitionNum
	for i := 0; i < space.PartitionNum; i++ {
		space.Partitions[i].Slot = entity.SlotID(i * width)
	}
	log.Debug("updateSpacePartitionNum space version %d, partition_num %d", space.Version, space.PartitionNum)

	if err := s.UpdateSpaceData(ctx, space); err != nil {
		return nil, err
	} else {
		return space, nil
	}
}

func (s *SpaceService) updateSpacePartitonRule(ctx context.Context, dbs *DBService, spaceResource *entity.SpacePartitionResource, space *entity.Space) (*entity.Space, error) {
	mc := s.client.Master()
	if spaceResource.PartitionOperatorType == entity.Drop {
		if spaceResource.PartitionName == "" {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition name is empty"))
		}
		found := false
		for _, range_rule := range space.PartitionRule.Ranges {
			if range_rule.Name == spaceResource.PartitionName {
				found = true
				break
			}
		}
		if !found {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition name %s not exist", spaceResource.PartitionName))
		}
		new_partitions := make([]*entity.Partition, 0)
		for _, partition := range space.Partitions {
			if partition.Name != spaceResource.PartitionName {
				new_partitions = append(new_partitions, partition)
			} else {
				// delete parition and partitionKey
				for _, replica := range partition.Replicas {
					if server, err := mc.QueryServer(ctx, replica); err != nil {
						log.Error("query partition:[%d] for replica:[%s] has err:[%s]", partition.Id, replica, err.Error())
					} else {
						if err := client.DeletePartition(server.RpcAddr(), partition.Id); err != nil {
							log.Error("delete partition:[%d] for server:[%s] has err:[%s]", partition.Id, server.RpcAddr(), err.Error())
						}
					}
				}
				err := mc.Delete(ctx, entity.PartitionKey(partition.Id))
				if err != nil {
					return nil, err
				}
			}
		}
		space.Partitions = new_partitions
		new_range_rules := make([]entity.Range, 0)
		for _, range_rule := range space.PartitionRule.Ranges {
			if range_rule.Name != spaceResource.PartitionName {
				new_range_rules = append(new_range_rules, range_rule)
			}
		}
		space.PartitionRule.Ranges = new_range_rules
	}

	if spaceResource.PartitionOperatorType == entity.Add {
		if spaceResource.PartitionRule == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition rule is empty"))
		}
		_, err := space.PartitionRule.RangeIsSame(spaceResource.PartitionRule.Ranges)
		if err != nil {
			return nil, err
		}

		// find all servers for update space partition
		servers, err := mc.QueryServers(ctx)
		if err != nil {
			return nil, err
		}

		// will get all exist partition
		serverPartitions, err := s.filterAndSortServer(ctx, dbs, space, servers)
		if err != nil {
			return nil, err
		}

		if int(space.ReplicaNum) > len(serverPartitions) {
			return nil, fmt.Errorf("not enough PS , need replica %d but only has %d",
				int(space.ReplicaNum), len(serverPartitions))
		}

		partitions := make([]*entity.Partition, 0)
		for _, r := range spaceResource.PartitionRule.Ranges {
			for j := 0; j < space.PartitionNum; j++ {
				partitionID, err := mc.NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

				if err != nil {
					return nil, err
				}

				partitions = append(partitions, &entity.Partition{
					Id:      entity.PartitionID(partitionID),
					Name:    r.Name,
					SpaceId: space.Id,
					DBId:    space.DBId,
				})
				log.Debug("updateSpacePartitionrule Generate partition id %d", partitionID)
			}
		}
		space.PartitionRule.Ranges, err = space.PartitionRule.AddRanges(spaceResource.PartitionRule.Ranges)
		if err != nil {
			return nil, err
		}
		log.Debug("updateSpacePartitionrule partition rule %v, add rule %v", space.PartitionRule, spaceResource.PartitionRule)

		// pick servers for space
		var paddrs [][]string
		for i := 0; i < len(partitions); i++ {
			if addrs, err := s.selectServersForPartition(servers, serverPartitions, space.ReplicaNum, partitions[i]); err != nil {
				return nil, err
			} else {
				paddrs = append(paddrs, addrs)
			}
		}

		log.Debug("updateSpacePartitionrule paritionNum %d, serverPartitions %v, paddrs %v", space.PartitionNum, serverPartitions, paddrs)

		// when create partition, new partition id will be stored in server partition cache
		space.Partitions = append(space.Partitions, partitions...)

		var errChain = make(chan error, 1)
		// send create partition for new
		for i := 0; i < len(partitions); i++ {
			go func(addrs []string, partition *entity.Partition) {
				//send request for all server
				defer func() {
					if r := recover(); r != nil {
						err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %v ", r))
						errChain <- err
						log.Error(err.Error())
					}
				}()
				for _, addr := range addrs {
					if err := client.CreatePartition(addr, space, partition.Id); err != nil {
						err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %s ", err.Error()))
						errChain <- err
						log.Error(err.Error())
					}
				}
			}(paddrs[i], partitions[i])
		}
		// check all partition is ok
		for i := 0; i < len(partitions); i++ {
			times := 0
			for {
				times++
				select {
				case err := <-errChain:
					return nil, err
				case <-ctx.Done():
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("update space has error"))
				default:
				}

				partition, err := mc.QueryPartition(ctx, partitions[i].Id)
				if times%5 == 0 {
					log.Debug("updateSpacePartitionNum check the partition:%d status", partitions[i].Id)
				}
				if err != nil && vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code != vearchpb.ErrorEnum_PARTITION_NOT_EXIST {
					return nil, err
				}
				if partition != nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	space.PartitionRule.Partitions = len(space.PartitionRule.Ranges)
	//update space
	width := math.MaxUint32 / (space.PartitionNum * space.PartitionRule.Partitions)
	for i := 0; i < space.PartitionNum*space.PartitionRule.Partitions; i++ {
		space.Partitions[i].Slot = entity.SlotID(i * width)
	}

	if err := s.UpdateSpaceData(ctx, space); err != nil {
		return nil, err
	} else {
		return space, nil
	}
}

// selectServersForPartition selects servers for a partition based on the given criteria.
// It ensures that the servers with the fewest replicas are chosen and applies anti-affinity by zone if configured.
//
// Parameters:
// - servers: A slice of pointers to Server entities representing available servers.
// - serverPartitions: A map where the key is the server index and the value is the number of partitions on that server.
// - replicaNum: The number of replicas needed for the partition.
// - partition: A pointer to the Partition entity that needs to be assigned servers.
//
// Returns:
// - A slice of strings containing the addresses of the selected servers.
// - An error if the required number of servers could not be selected.
//
// The function considers the anti-affinity strategy configured in the master service to avoid placing replicas in the same zone.
func (s *SpaceService) selectServersForPartition(servers []*entity.Server, serverPartitions map[int]int, replicaNum uint8, partition *entity.Partition) ([]string, error) {
	address := make([]string, 0, replicaNum)
	originReplicaNum := replicaNum
	partition.Replicas = make([]entity.NodeID, 0, replicaNum)

	kvList := make([]struct {
		index  int
		length int
	}, len(serverPartitions))

	i := 0
	for k, v := range serverPartitions {
		kvList[i] = struct {
			index  int
			length int
		}{index: k, length: v}
		i++
	}

	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].length < kvList[j].length
	})

	zoneCount := make(map[string]int)

	mc := s.client.Master()
	antiAffinity := mc.Client().Master().Config().PS.ReplicaAntiAffinityStrategy
	// find the servers with the fewest replicas and apply anti-affinity by zone
	for _, kv := range kvList {
		addr := servers[kv.index].RpcAddr()
		ID := servers[kv.index].ID
		var zone string

		switch antiAffinity {
		case 1:
			zone = servers[kv.index].HostIp
		case 2:
			zone = servers[kv.index].HostRack
		case 3:
			zone = servers[kv.index].HostZone
		default:
			zone = ""
		}

		if !client.IsLive(addr) {
			serverPartitions[kv.index] = kv.length
			continue
		}

		if zone != "" && zoneCount[zone] > 0 {
			continue
		}

		serverPartitions[kv.index]++
		if zone != "" {
			zoneCount[zone]++
		}
		address = append(address, addr)
		partition.Replicas = append(partition.Replicas, ID)

		replicaNum--
		if replicaNum <= 0 {
			break
		}
	}

	if replicaNum > 0 {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_MASTER_PS_NOT_ENOUGH_SELECT, fmt.Errorf("need %d partition servers but only got %d", originReplicaNum, len(address)))
	}

	return address, nil
}

func (s *SpaceService) UpdateSpace(ctx context.Context, dbName, spaceName string, temp *entity.Space) (*entity.Space, error) {
	// it will lock cluster to create space
	mc := s.client.Master()
	mutex := mc.NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*300)
	if err := mutex.Lock(); err != nil {
		return nil, err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("failed to unlock space,the Error is:%v ", err)
		}
	}()

	dbId, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("failed to find database id according database name:%v,the Error is:%v ", dbName, err))
	}

	space, err := mc.QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("failed to find space according space name:%v,the Error is:%v ", spaceName, err))
	}

	if space == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("can not found space by name : %s", spaceName))
	}

	buff := bytes.Buffer{}
	if temp.DBId != 0 && temp.DBId != space.DBId {
		buff.WriteString("db_id not same ")
	}

	if temp.PartitionNum != 0 && temp.PartitionNum != space.PartitionNum {
		buff.WriteString("partition_num can not change ")
	}
	if temp.ReplicaNum != 0 && temp.ReplicaNum != space.ReplicaNum {
		buff.WriteString("replica_num can not change ")
	}
	if buff.String() != "" {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(buff.String()))
	}

	if temp.Name != "" {
		space.Name = temp.Name
	}

	if temp.Enabled != nil {
		space.Enabled = temp.Enabled
	}

	if err := space.Validate(); err != nil {
		return nil, err
	}

	space.Version++
	space.Partitions = temp.Partitions

	if temp.Fields != nil && len(temp.Fields) > 0 {
		// parse old space
		oldFieldMap, err := mapping.SchemaMap(space.Fields)
		if err != nil {
			return nil, err
		}

		// parse new space
		newFieldMap, err := mapping.SchemaMap(temp.Fields)
		if err != nil {
			return nil, err
		}

		for k, v := range oldFieldMap {
			if fm, ok := newFieldMap[k]; ok {
				if !mapping.Equals(v, fm) {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("not equals by field:[%s] old[%v] new[%v]", k, v, fm))
				}
				delete(newFieldMap, k)
			}
		}

		if len(newFieldMap) > 0 {
			log.Info("change schema for space: %s, change fields: %d, value is: [%s]", space.Name, len(newFieldMap), string(temp.Fields))

			schema, err := mapping.MergeSchema(space.Fields, temp.Fields)
			if err != nil {
				return nil, err
			}

			space.Fields = schema
		}
	}

	// notify all partitions
	for _, p := range space.Partitions {
		partition, err := mc.QueryPartition(ctx, p.Id)
		if err != nil {
			return nil, err
		}

		server, err := mc.QueryServer(ctx, partition.LeaderID)
		if err != nil {
			return nil, err
		}

		if !client.IsLive(server.RpcAddr()) {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, fmt.Errorf("partition %s is shutdown", server.RpcAddr()))
		}
	}

	for _, p := range space.Partitions {
		partition, err := mc.QueryPartition(ctx, p.Id)
		if err != nil {
			return nil, err
		}

		server, err := mc.QueryServer(ctx, partition.LeaderID)
		if err != nil {
			return nil, err
		}

		log.Debug("update partition server is [%+v], space is [%+v], pid is [%+v]",
			server, space, p.Id)

		if err := client.UpdatePartition(server.RpcAddr(), space, p.Id); err != nil {
			log.Error("UpdatePartition err is [%v]", err)
			return nil, err
		}
	}

	log.Debug("update space is [%+v]", space)
	space.Version--
	if err := s.UpdateSpaceData(ctx, space); err != nil {
		return nil, err
	}

	return space, nil
}

func (s *SpaceService) UpdateSpaceData(ctx context.Context, space *entity.Space) error {
	space.Version++
	if space.PartitionRule == nil {
		space.PartitionNum = len(space.Partitions)
	}
	marshal, err := json.Marshal(space)
	if err != nil {
		return err
	}
	mc := s.client.Master()
	if err = mc.Update(ctx, entity.SpaceKey(space.DBId, space.Id), marshal); err != nil {
		return err
	}

	return nil
}
