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
	"sync"
	"time"

	"slices"

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
	masterClient := s.client.Master()
	if space.DBId, err = masterClient.QueryDBName2ID(ctx, dbName); err != nil {
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
	spaceLock := masterClient.NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*300)
	if err = spaceLock.Lock(); err != nil {
		return err
	}
	defer func() {
		if unlockErr := spaceLock.Unlock(); unlockErr != nil {
			log.Error("unlock space err:[%s]", unlockErr.Error())
		}
	}()

	// spaces is existed
	if _, err := masterClient.QuerySpaceByName(ctx, space.DBId, space.Name); err != nil {
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
	servers, err := masterClient.QueryServers(ctx)
	if err != nil {
		return err
	}

	// generate space id
	spaceID, err := masterClient.NewIDGenerate(ctx, entity.SpaceIdSequence, 1, 5*time.Second)
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
		slotWidth := math.MaxUint32 / (space.PartitionNum * space.PartitionRule.Partitions)
		for i := range space.PartitionNum * space.PartitionRule.Partitions {
			partitionID, err := masterClient.NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

			if err != nil {
				return err
			}

			space.Partitions = append(space.Partitions, &entity.Partition{
				Id:      entity.PartitionID(partitionID),
				Name:    space.PartitionRule.Ranges[i/space.PartitionNum].Name,
				SpaceId: space.Id,
				DBId:    space.DBId,
				Slot:    entity.SlotID(i * slotWidth),
			})
		}
	} else {
		slotWidth := math.MaxUint32 / space.PartitionNum
		for i := range space.PartitionNum {
			partitionID, err := masterClient.NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

			if err != nil {
				return err
			}

			space.Partitions = append(space.Partitions, &entity.Partition{
				Id:      entity.PartitionID(partitionID),
				SpaceId: space.Id,
				DBId:    space.DBId,
				Slot:    entity.SlotID(i * slotWidth),
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

	isSpaceDisabled := false
	space.Enabled = &isSpaceDisabled
	defer func() {
		if !(*space.Enabled) { // remove the space if it is still not enabled
			if deleteErr := masterClient.Delete(context.Background(), entity.SpaceKey(space.DBId, space.Id)); deleteErr != nil {
				log.Error("to delete space err: %s", deleteErr.Error())
			}
		}
	}()

	marshaledSpace, err := json.Marshal(space)
	if err != nil {
		return err
	}
	if space.Index == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space vector field index should not be empty"))
	}
	err = masterClient.Create(ctx, entity.SpaceKey(space.DBId, space.Id), marshaledSpace)
	if err != nil {
		return err
	}

	// pick servers for space
	partitionServerAddresses := make([][]string, len(space.Partitions))
	for partitionIndex := range space.Partitions {
		if serverAddresses, err := s.selectServersForPartition(servers, serverPartitions, space.ReplicaNum, space.Partitions[partitionIndex]); err != nil {
			return err
		} else {
			partitionServerAddresses[partitionIndex] = serverAddresses
		}
	}

	errorChannel := make(chan error, len(space.Partitions))
	// send create space request to partition server
	for partitionIndex := range space.Partitions {
		go s.createPartitionOnServers(partitionServerAddresses[partitionIndex], space.Partitions[partitionIndex], space, errorChannel)
	}

	// check all partition is ok
	if err := s.waitForPartitionsReady(ctx, masterClient, space.Partitions, errorChannel); err != nil {
		return err
	}

	isSpaceEnabled := true
	space.Enabled = &isSpaceEnabled

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
	masterClient := s.client.Master()
	databaseID, err := masterClient.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return err
	}

	space, err := masterClient.QuerySpaceByName(ctx, databaseID, spaceName)
	if err != nil {
		return err
	}
	if space == nil { // nil if it not exists
		return nil
	}

	spaceLock := masterClient.NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*60)
	if err = spaceLock.Lock(); err != nil {
		return err
	}
	defer func() {
		if unlockErr := spaceLock.Unlock(); unlockErr != nil {
			log.Error("unlock space err:[%s]", unlockErr.Error())
		}
	}()
	// delete key
	err = masterClient.Delete(ctx, entity.SpaceKey(databaseID, space.Id))
	if err != nil {
		return err
	}

	// delete parition and partitionKey
	for _, partition := range space.Partitions {
		for _, replicaID := range partition.Replicas {
			if server, err := masterClient.QueryServer(ctx, replicaID); err != nil {
				log.Error("query partition:[%d] for replica:[%s] has err:[%s]", partition.Id, replicaID, err.Error())
			} else {
				if err := client.DeletePartition(server.RpcAddr(), partition.Id); err != nil {
					log.Error("delete partition:[%d] for server:[%s] has err:[%s]", partition.Id, server.RpcAddr(), err.Error())
				}
			}
		}
		err = masterClient.Delete(ctx, entity.PartitionKey(partition.Id))
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
	err = masterClient.Delete(ctx, entity.SpaceConfigKey(databaseID, space.Id))
	if err != nil {
		return err
	}

	return nil
}

func (s *SpaceService) DescribeSpace(ctx context.Context, space *entity.Space, spaceInfo *entity.SpaceInfo, detail_info bool) (int, error) {
	spaceStatus := 0
	statusColors := []string{"green", "yellow", "red"}
	spaceInfo.Errors = make([]string, 0)
	masterClient := s.client.Master()

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
		partition, err := masterClient.QueryPartition(ctx, spacePartition.Id)
		partitionStatus := 0

		if err != nil {
			msg := fmt.Sprintf("partition:[%d] in space: [%s] not found in meta data", spacePartition.Id, space.Name)
			spaceInfo.Errors = append(spaceInfo.Errors, msg)
			log.Error(msg)
			partitionStatus = 2
			if partitionStatus > spaceStatus {
				spaceStatus = partitionStatus
			}
			continue
		}

		nodeID := partition.LeaderID
		if nodeID == 0 {
			log.Error("partition:[%d] in space: [%s] leaderID is 0", spacePartition.Id, space.Name)
			if len(partition.Replicas) > 0 {
				nodeID = partition.Replicas[0]
			}
		}

		server, err := masterClient.QueryServer(ctx, nodeID)
		if err != nil {
			msg := fmt.Sprintf("space: [%s] partition:[%d], server:[%d] not found", space.Name, spacePartition.Id, nodeID)
			spaceInfo.Errors = append(spaceInfo.Errors, msg)
			log.Error(msg)
			partitionStatus = 2
			if partitionStatus > spaceStatus {
				spaceStatus = partitionStatus
			}
			continue
		}

		partitionInfo, err := client.PartitionInfo(server.RpcAddr(), partition.Id, detail_info)
		if err != nil {
			msg := fmt.Sprintf("query space:[%s] server:[%d] partition:[%d] info err :[%s]", space.Name, nodeID, spacePartition.Id, err.Error())
			spaceInfo.Errors = append(spaceInfo.Errors, msg)
			log.Error(msg)
			partitionInfo = &entity.PartitionInfo{}
			partitionStatus = 2
		} else {
			if len(partitionInfo.Unreachable) > 0 {
				partitionStatus = 1
			}
		}

		replicasStatus := make(map[entity.NodeID]string)
		for nodeID, status := range partition.ReStatusMap {
			if status == entity.ReplicasOK {
				replicasStatus[nodeID] = "ReplicasOK"
			} else {
				replicasStatus[nodeID] = "ReplicasNotReady"
			}
		}

		if partitionInfo.RaftStatus != nil {
			if partitionInfo.RaftStatus.Leader == 0 {
				partitionStatus = s.addPartitionError(spaceInfo, 2, "partition:[%d] in space:[%s] has no leader", spacePartition.Id, space.Name)
			} else {
				if len(partitionInfo.RaftStatus.Replicas) != int(space.ReplicaNum) {
					partitionStatus = s.addPartitionError(spaceInfo, 2, "partition:[%d] in space:[%s] replicas: [%d] is not equal to replicaNum: [%d]", spacePartition.Id, space.Name, len(partitionInfo.RaftStatus.Replicas), space.ReplicaNum)
				} else {
					replicaStateProbeNum := 0
					leaderID := 0
					for nodeID, replica := range partitionInfo.RaftStatus.Replicas {
						// TODO FIXME: when leader changed, the unreachableNodeIDnre state may still be ReplicaStateProbe
						if slices.Contains(partitionInfo.Unreachable, nodeID) {
							continue
						}
						if replica.State == entity.ReplicaStateProbe {
							replicaStateProbeNum += 1
							leaderID = int(nodeID)
						}
					}
					if replicaStateProbeNum != 1 {
						partitionStatus = s.addPartitionError(spaceInfo, 2, "partition:[%d] in space:[%s] have [%d] leader", spacePartition.Id, space.Name, replicaStateProbeNum)
					}
					if leaderID != int(partitionInfo.RaftStatus.Leader) {
						partitionStatus = s.addPartitionError(spaceInfo, 2, "partition:[%d] in space:[%s] leader: [%d] is not equal to raft leader: [%d]", spacePartition.Id, space.Name, leaderID, partitionInfo.RaftStatus.Leader)
					}
				}
			}
		}

		// this must from space.Partitions
		partitionInfo.PartitionID = spacePartition.Id
		partitionInfo.Name = spacePartition.Name
		partitionInfo.Color = statusColors[partitionStatus]
		partitionInfo.ReplicaNum = len(partition.Replicas)
		partitionInfo.Ip = server.Ip
		partitionInfo.NodeID = server.ID
		partitionInfo.RepStatus = replicasStatus

		spaceInfo.Partitions = append(spaceInfo.Partitions, partitionInfo)

		if partitionStatus > spaceStatus {
			spaceStatus = partitionStatus
		}
	}

	totalDocuments := uint64(0)
	for _, partitionInfo := range spaceInfo.Partitions {
		totalDocuments += cast.ToUint64(partitionInfo.DocNum)
	}
	spaceInfo.Status = statusColors[spaceStatus]
	spaceInfo.DocNum = totalDocuments
	return spaceStatus, nil
}

func (s *SpaceService) filterAndSortServer(ctx context.Context, dbs *DBService, space *entity.Space, servers []*entity.Server) (map[int]int, error) {
	database, err := dbs.QueryDB(ctx, cast.ToString(space.DBId))
	if err != nil {
		return nil, err
	}

	var allowedServersMap map[string]bool
	if len(database.Ps) > 0 {
		allowedServersMap = make(map[string]bool)
		for _, serverIP := range database.Ps {
			allowedServersMap[serverIP] = true
		}
	}

	serverPartitionCounts := make(map[int]int)

	masterClient := s.client.Master()
	allSpaces, err := masterClient.QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		return nil, err
	}

	serverIndexMap := make(map[entity.NodeID]int)

	if allowedServersMap == nil { // If allowedServersMap is nil, only use public servers
		for serverIndex, server := range servers {
			// Only use servers with the same resource name
			if server.ResourceName != space.ResourceName {
				continue
			}
			if !server.Private {
				serverPartitionCounts[serverIndex] = 0
				serverIndexMap[server.ID] = serverIndex
			}
		}
	} else { // If allowedServersMap is not nil, only use defined servers
		for serverIndex, server := range servers {
			// Only use servers with the same resource name
			if server.ResourceName != space.ResourceName {
				allowedServersMap[server.Ip] = false
				continue
			}
			if allowedServersMap[server.Ip] {
				serverPartitionCounts[serverIndex] = 0
				serverIndexMap[server.ID] = serverIndex
			}
		}
	}

	for _, spaceIterator := range allSpaces {
		for _, partition := range spaceIterator.Partitions {
			for _, nodeID := range partition.Replicas {
				if serverIndex, exists := serverIndexMap[nodeID]; exists {
					serverPartitionCounts[serverIndex] = serverPartitionCounts[serverIndex] + 1
				}
			}
		}
	}

	return serverPartitionCounts, nil
}

// UpdateSpace is a unified function that handles both space configuration updates and resource updates
// It can handle:
// 1. Configuration updates (name, enabled, fields) - when updateRequest contains space properties
// 2. Partition number expansion - when updateRequest contains partition_num > current
// 3. Partition rule operations - when updateRequest contains partition_operator_type
func (s *SpaceService) UpdateSpace(ctx context.Context, dbs *DBService, dbName, spaceName string, updateRequest *entity.Space, op string) (*entity.Space, error) {
	// Acquire distributed lock
	masterClient := s.client.Master()
	spaceLock := masterClient.NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*300)
	if err := spaceLock.Lock(); err != nil {
		return nil, err
	}
	defer func() {
		if unlockErr := spaceLock.Unlock(); unlockErr != nil {
			log.Error("failed to unlock space: %v", unlockErr)
		}
	}()

	// Get current space
	databaseID, err := masterClient.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
			fmt.Errorf("failed to find database id for %s: %v", dbName, err))
	}

	space, err := masterClient.QuerySpaceByName(ctx, databaseID, spaceName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
			fmt.Errorf("failed to find space %s: %v", spaceName, err))
	}

	if space == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
			fmt.Errorf("space not found: %s", spaceName))
	}

	if op == "config" {
		return s.handleConfigurationUpdate(ctx, space, updateRequest)
	}
	if updateRequest.PartitionName != nil || updateRequest.PartitionOperatorType != nil || updateRequest.PartitionRule != nil || updateRequest.PartitionNum > 0 {
		return s.handleResourceUpdate(ctx, dbs, space, updateRequest)
	}
	return s.handleConfigurationUpdate(ctx, space, updateRequest)
}

// handleResourceUpdate handles partition-related updates (expansion, rule operations)
func (s *SpaceService) handleResourceUpdate(ctx context.Context, dbs *DBService, space *entity.Space, updateRequest *entity.Space) (*entity.Space, error) {
	// Handle partition rule operations (Add/Drop)
	if updateRequest.PartitionOperatorType != nil {
		if *updateRequest.PartitionOperatorType != entity.Add && *updateRequest.PartitionOperatorType != entity.Drop {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("partition operator type should be %s or %s, but is %s",
					entity.Add, entity.Drop, *updateRequest.PartitionOperatorType))
		}
		if space.PartitionRule == nil || space.PartitionRule.Ranges == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("space %s partition rule is empty", space.Name))
		}
		return s.updateSpacePartitonRule(ctx, dbs, *updateRequest.PartitionName, *updateRequest.PartitionOperatorType, updateRequest.PartitionRule, space)
	}

	// Handle partition number expansion
	if updateRequest.PartitionNum > 0 {
		if space.PartitionNum >= updateRequest.PartitionNum {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("partition_num %d should be greater than current %d",
					updateRequest.PartitionNum, space.PartitionNum))
		}

		return s.expandPartitions(ctx, dbs, space, uint32(updateRequest.PartitionNum))
	}

	return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
		fmt.Errorf("no valid resource update operation specified"))
}

// handleConfigurationUpdate handles space configuration updates (name, enabled, fields)
func (s *SpaceService) handleConfigurationUpdate(ctx context.Context, space *entity.Space, temp *entity.Space) (*entity.Space, error) {
	// Validate immutable properties
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

	// Update mutable properties
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
	if temp.Partitions != nil {
		space.Partitions = temp.Partitions
	}

	// Handle schema updates (field additions)
	if len(temp.Fields) > 0 {
		if err := s.updateSpaceFields(space, temp.Fields); err != nil {
			return nil, err
		}
	}

	// Notify all partitions of configuration changes
	if err := s.notifyPartitionsConfigUpdate(ctx, space); err != nil {
		return nil, err
	}

	space.Version--
	if err := s.UpdateSpaceData(ctx, space); err != nil {
		return nil, err
	}

	return space, nil
}

// expandPartitions handles partition number expansion
func (s *SpaceService) expandPartitions(ctx context.Context, dbs *DBService, space *entity.Space, newPartitionCount uint32) (*entity.Space, error) {
	// Create new partitions
	masterClient := s.client.Master()
	newPartitions := make([]*entity.Partition, 0, int(newPartitionCount)-space.PartitionNum)
	for partitionIndex := space.PartitionNum; partitionIndex < int(newPartitionCount); partitionIndex++ {
		partitionID, err := masterClient.NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)
		if err != nil {
			return nil, err
		}

		newPartitions = append(newPartitions, &entity.Partition{
			Id:      entity.PartitionID(partitionID),
			SpaceId: space.Id,
			DBId:    space.DBId,
		})
		log.Debug("expandPartitions Generate partition id %d", partitionID)
	}

	// Get servers and validate
	servers, err := masterClient.QueryServers(ctx)
	if err != nil {
		return nil, err
	}

	serverPartitionCounts, err := s.filterAndSortServer(ctx, dbs, space, servers)
	if err != nil {
		return nil, err
	}

	if int(space.ReplicaNum) > len(serverPartitionCounts) {
		return nil, fmt.Errorf("not enough PS, need replica %d but only has %d",
			int(space.ReplicaNum), len(serverPartitionCounts))
	}

	// Pick servers for partitions
	partitionServerAddresses := make([][]string, len(newPartitions))
	for i := range newPartitions {
		if addresses, err := s.selectServersForPartition(servers, serverPartitionCounts, space.ReplicaNum, newPartitions[i]); err != nil {
			return nil, err
		} else {
			partitionServerAddresses[i] = addresses
		}
	}

	log.Debug("expandPartitions origin partitionNum %d, serverPartitions %v, partitionServerAddresses %v",
		space.PartitionNum, serverPartitionCounts, partitionServerAddresses)

	// Update space with new partitions
	space.PartitionNum = int(newPartitionCount)
	space.Partitions = append(space.Partitions, newPartitions...)

	// Create partitions on servers asynchronously
	errorChannel := make(chan error, len(newPartitions))
	for i := range newPartitions {
		go s.createPartitionOnServers(partitionServerAddresses[i], newPartitions[i], space, errorChannel)
	}

	// Wait for all partitions to be created
	if err := s.waitForPartitionsReady(ctx, masterClient, newPartitions, errorChannel); err != nil {
		return nil, err
	}

	// Update slot assignments
	slotWidth := math.MaxUint32 / uint32(space.PartitionNum)
	for partitionIndex := range space.PartitionNum {
		space.Partitions[partitionIndex].Slot = entity.SlotID(uint32(partitionIndex) * slotWidth)
	}

	log.Debug("expandPartitions space version %d, partition_num %d", space.Version, space.PartitionNum)

	if err := s.UpdateSpaceData(ctx, space); err != nil {
		return nil, err
	}

	return space, nil
}

// updateSpaceFields handles field schema updates
func (s *SpaceService) updateSpaceFields(space *entity.Space, newFields []byte) error {
	// parse old space
	oldFieldMap, err := mapping.SchemaMap(space.Fields)
	if err != nil {
		return err
	}

	// parse new space
	newFieldMap, err := mapping.SchemaMap(newFields)
	if err != nil {
		return err
	}

	for fieldName, fieldValue := range oldFieldMap {
		if newFieldValue, exists := newFieldMap[fieldName]; exists {
			if !mapping.Equals(fieldValue, newFieldValue) {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
					fmt.Errorf("not equals by field:[%s] old[%v] new[%v]", fieldName, fieldValue, newFieldValue))
			}
			delete(newFieldMap, fieldName)
		}
	}

	if len(newFieldMap) > 0 {
		log.Info("change schema for space: %s, change fields: %d, value is: [%s]",
			space.Name, len(newFieldMap), string(newFields))

		schema, err := mapping.MergeSchema(space.Fields, newFields)
		if err != nil {
			return err
		}

		space.Fields = schema
	}

	return nil
}

// notifyPartitionsConfigUpdate notifies all partitions of configuration changes
func (s *SpaceService) notifyPartitionsConfigUpdate(ctx context.Context, space *entity.Space) error {
	errorChannel := make(chan error, len(space.Partitions))
	var waitGroup sync.WaitGroup

	for _, partition := range space.Partitions {
		waitGroup.Add(1)
		go func(currentPartition *entity.Partition) {
			defer waitGroup.Done()
			if err := s.updateSinglePartition(ctx, space, currentPartition); err != nil {
				errorChannel <- err
			}
		}(partition)
	}

	// Wait for all goroutines to complete
	go func() {
		waitGroup.Wait()
		close(errorChannel)
	}()

	// Check for any errors
	for err := range errorChannel {
		if err != nil {
			log.Error("UpdatePartition err: %v", err)
			return err
		}
	}

	return nil
}

// createPartitionOnServers creates a partition on all specified servers
func (s *SpaceService) createPartitionOnServers(serverAddresses []string, partition *entity.Partition, space *entity.Space, errorChannel chan<- error) {
	defer func() {
		if recoveredError := recover(); recoveredError != nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
				fmt.Errorf("create partition err: %v", recoveredError))
			errorChannel <- err
			log.Error(err.Error())
		}
	}()

	for _, address := range serverAddresses {
		if err := client.CreatePartition(address, space, partition.Id); err != nil {
			err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
				fmt.Errorf("create partition err: %s", err.Error()))
			errorChannel <- err
			log.Error(err.Error())
			return
		}
	}
}

// waitForPartitionsReady waits for all partitions to be created and ready
func (s *SpaceService) waitForPartitionsReady(ctx context.Context, masterClient any, partitions []*entity.Partition, errorChannel <-chan error) error {
	for partitionIndex := range partitions {
		attemptCount := 0
		for {
			attemptCount++
			select {
			case err := <-errorChannel:
				return err
			case <-ctx.Done():
				return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
					fmt.Errorf("update space has error"))
			default:
			}

			partition, err := masterClient.(interface {
				QueryPartition(context.Context, entity.PartitionID) (*entity.Partition, error)
			}).QueryPartition(ctx, partitions[partitionIndex].Id)
			if attemptCount%5 == 0 {
				log.Debug("waitForPartitionsReady check partition %d status", partitions[partitionIndex].Id)
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
	return nil
}

func (s *SpaceService) updateSpacePartitonRule(ctx context.Context, dbs *DBService, partitionName string, partitionOperatorType string, partitionRule *entity.PartitionRule, space *entity.Space) (*entity.Space, error) {
	masterClient := s.client.Master()
	if partitionOperatorType == entity.Drop {
		if partitionName == "" {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition name is empty"))
		}
		found := false
		for _, rangeRule := range space.PartitionRule.Ranges {
			if rangeRule.Name == partitionName {
				found = true
				break
			}
		}
		if !found {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition name %s not exist", partitionName))
		}
		remainingPartitions := make([]*entity.Partition, 0)
		for _, partition := range space.Partitions {
			if partition.Name != partitionName {
				remainingPartitions = append(remainingPartitions, partition)
			} else {
				// delete parition and partitionKey
				for _, replica := range partition.Replicas {
					if server, err := masterClient.QueryServer(ctx, replica); err != nil {
						log.Error("query partition:[%d] for replica:[%s] has err:[%s]", partition.Id, replica, err.Error())
					} else {
						if err := client.DeletePartition(server.RpcAddr(), partition.Id); err != nil {
							log.Error("delete partition:[%d] for server:[%s] has err:[%s]", partition.Id, server.RpcAddr(), err.Error())
						}
					}
				}
				err := masterClient.Delete(ctx, entity.PartitionKey(partition.Id))
				if err != nil {
					return nil, err
				}
			}
		}
		space.Partitions = remainingPartitions
		remainingRangeRules := make([]entity.Range, 0)
		for _, rangeRule := range space.PartitionRule.Ranges {
			if rangeRule.Name != partitionName {
				remainingRangeRules = append(remainingRangeRules, rangeRule)
			}
		}
		space.PartitionRule.Ranges = remainingRangeRules
	}

	if partitionOperatorType == entity.Add {
		if partitionRule == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition rule is empty"))
		}
		_, err := space.PartitionRule.RangeIsSame(partitionRule.Ranges)
		if err != nil {
			return nil, err
		}

		// find all servers for update space partition
		servers, err := masterClient.QueryServers(ctx)
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

		newPartitions := make([]*entity.Partition, 0)
		for _, rangeRule := range partitionRule.Ranges {
			if rangeRule.Name == "" {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition name is empty"))
			}
			for j := 0; j < space.PartitionNum; j++ {
				partitionID, err := masterClient.NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

				if err != nil {
					return nil, err
				}

				newPartitions = append(newPartitions, &entity.Partition{
					Id:      entity.PartitionID(partitionID),
					Name:    rangeRule.Name,
					SpaceId: space.Id,
					DBId:    space.DBId,
				})
				log.Debug("updateSpacePartitionrule Generate partition id %d", partitionID)
			}
		}
		space.PartitionRule.Ranges, err = space.PartitionRule.AddRanges(partitionRule.Ranges)
		if err != nil {
			return nil, err
		}
		log.Debug("updateSpacePartitionrule partition rule %v, add rule %v", space.PartitionRule, partitionRule)

		// pick servers for space
		var partitionServerAddresses [][]string
		for i := 0; i < len(newPartitions); i++ {
			if addresses, err := s.selectServersForPartition(servers, serverPartitions, space.ReplicaNum, newPartitions[i]); err != nil {
				return nil, err
			} else {
				partitionServerAddresses = append(partitionServerAddresses, addresses)
			}
		}

		log.Debug("updateSpacePartitionrule paritionNum %d, serverPartitions %v, partitionServerAddresses %v", space.PartitionNum, serverPartitions, partitionServerAddresses)

		// when create partition, new partition id will be stored in server partition cache
		space.Partitions = append(space.Partitions, newPartitions...)

		var errorChannel = make(chan error, 1)
		// send create partition for new
		for i := 0; i < len(newPartitions); i++ {
			go func(addresses []string, partition *entity.Partition) {
				//send request for all server
				defer func() {
					if recoveredError := recover(); recoveredError != nil {
						err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %v ", recoveredError))
						errorChannel <- err
						log.Error(err.Error())
					}
				}()
				for _, address := range addresses {
					if err := client.CreatePartition(address, space, partition.Id); err != nil {
						err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %s ", err.Error()))
						errorChannel <- err
						log.Error(err.Error())
					}
				}
			}(partitionServerAddresses[i], newPartitions[i])
		}
		// check all partition is ok
		for i := 0; i < len(newPartitions); i++ {
			attemptCount := 0
			for {
				attemptCount++
				select {
				case err := <-errorChannel:
					return nil, err
				case <-ctx.Done():
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("update space has error"))
				default:
				}

				partition, err := masterClient.QueryPartition(ctx, newPartitions[i].Id)
				if attemptCount%5 == 0 {
					log.Debug("updateSpacePartitionNum check the partition:%d status", newPartitions[i].Id)
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
	slotWidth := math.MaxUint32 / (space.PartitionNum * space.PartitionRule.Partitions)
	for i := 0; i < space.PartitionNum*space.PartitionRule.Partitions; i++ {
		space.Partitions[i].Slot = entity.SlotID(i * slotWidth)
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
// - serverPartitionCounts: A map where the key is the server index and the value is the number of partitions on that server.
// - replicaCount: The number of replicas needed for the partition.
// - partition: A pointer to the Partition entity that needs to be assigned servers.
//
// Returns:
// - A slice of strings containing the addresses of the selected servers.
// - An error if the required number of servers could not be selected.
//
// The function considers the anti-affinity strategy configured in the master service to avoid placing replicas in the same zone.
func (s *SpaceService) selectServersForPartition(servers []*entity.Server, serverPartitionCounts map[int]int, replicaCount uint8, partition *entity.Partition) ([]string, error) {
	selectedAddresses := make([]string, 0, replicaCount)
	originalReplicaCount := replicaCount
	partition.Replicas = make([]entity.NodeID, 0, replicaCount)

	serverCountPairs := make([]struct {
		serverIndex    int
		partitionCount int
	}, len(serverPartitionCounts))

	pairIndex := 0
	for serverIndex, partitionCount := range serverPartitionCounts {
		serverCountPairs[pairIndex] = struct {
			serverIndex    int
			partitionCount int
		}{serverIndex: serverIndex, partitionCount: partitionCount}
		pairIndex++
	}

	sort.Slice(serverCountPairs, func(i, j int) bool {
		return serverCountPairs[i].partitionCount < serverCountPairs[j].partitionCount
	})

	zoneUsageCount := make(map[string]int)

	masterClient := s.client.Master()
	antiAffinityStrategy := masterClient.Client().Master().Config().PS.ReplicaAntiAffinityStrategy
	// find the servers with the fewest replicas and apply anti-affinity by zone
	for _, pair := range serverCountPairs {
		serverAddress := servers[pair.serverIndex].RpcAddr()
		serverID := servers[pair.serverIndex].ID
		var zoneIdentifier string

		switch antiAffinityStrategy {
		case 1:
			zoneIdentifier = servers[pair.serverIndex].HostIp
		case 2:
			zoneIdentifier = servers[pair.serverIndex].HostRack
		case 3:
			zoneIdentifier = servers[pair.serverIndex].HostZone
		default:
			zoneIdentifier = ""
		}

		if !client.IsLive(serverAddress) {
			serverPartitionCounts[pair.serverIndex] = pair.partitionCount
			continue
		}

		if zoneIdentifier != "" && zoneUsageCount[zoneIdentifier] > 0 {
			continue
		}

		serverPartitionCounts[pair.serverIndex]++
		if zoneIdentifier != "" {
			zoneUsageCount[zoneIdentifier]++
		}
		selectedAddresses = append(selectedAddresses, serverAddress)
		partition.Replicas = append(partition.Replicas, serverID)

		replicaCount--
		if replicaCount <= 0 {
			break
		}
	}

	if replicaCount > 0 {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_MASTER_PS_NOT_ENOUGH_SELECT, fmt.Errorf("need %d partition servers but only got %d", originalReplicaCount, len(selectedAddresses)))
	}

	return selectedAddresses, nil
}

// updateSinglePartition updates a single partition
func (s *SpaceService) updateSinglePartition(ctx context.Context, space *entity.Space, partition *entity.Partition) error {
	masterClient := s.client.Master()
	partitionInfo, err := masterClient.QueryPartition(ctx, partition.Id)
	if err != nil {
		return err
	}

	server, err := masterClient.QueryServer(ctx, partitionInfo.LeaderID)
	if err != nil {
		return err
	}

	if !client.IsLive(server.RpcAddr()) {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED,
			fmt.Errorf("partition %s is shutdown", server.RpcAddr()))
	}

	log.Debug("update partition server: %+v, space: %+v, pid: %+v", server, space, partition.Id)

	if err := client.UpdatePartition(server.RpcAddr(), space, partition.Id); err != nil {
		return err
	}

	return nil
}

// addPartitionError adds an error to spaceInfo and returns the status
func (s *SpaceService) addPartitionError(spaceInfo *entity.SpaceInfo, status int, format string, args ...any) int {
	msg := fmt.Sprintf(format, args...)
	spaceInfo.Errors = append(spaceInfo.Errors, msg)
	log.Error(msg)
	return status
}

func (s *SpaceService) UpdateSpaceData(ctx context.Context, space *entity.Space) error {
	space.Version++
	if space.PartitionRule == nil {
		space.PartitionNum = len(space.Partitions)
	}
	space.PartitionName = nil
	space.PartitionOperatorType = nil
	marshaledSpace, err := json.Marshal(space)
	if err != nil {
		return err
	}
	masterClient := s.client.Master()
	if err = masterClient.Update(ctx, entity.SpaceKey(space.DBId, space.Id), marshaledSpace); err != nil {
		return err
	}

	return nil
}
