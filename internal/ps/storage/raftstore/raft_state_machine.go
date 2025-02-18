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

package raftstore

import (
	"fmt"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/psutil"
)

// replicas status,behind leader or equal leader
func (s *Store) ReplicasStatusChange() bool {
	statusChange := false
	// get leader
	leaderCommit := s.Status().Commit
	var currentStatus uint32
	for nodeID, rs := range s.Status().Replicas {
		if nodeID == s.Partition.LeaderID || leaderCommit-rs.Commit < s.raftDiffCount {
			currentStatus = entity.ReplicasOK
		} else {
			currentStatus = entity.ReplicasNotReady
		}
		pStatus, found := s.RsStatusMap.Load(nodeID)
		if found {
			// status unequal
			if pStatus.(uint32) != currentStatus {
				if !statusChange {
					statusChange = true
				}
				log.Debug("current nodeID is [%d], partitionID is [%d], commit is [%d], leader nodeID is [%d], leader commit is [%d]",
					nodeID, s.Partition.Id, rs.Commit, s.Partition.LeaderID, leaderCommit)
				log.Debug("status change, because nodeID [%d] statusChange.", nodeID)
				s.RsStatusMap.Store(nodeID, currentStatus)
			}
		} else {
			// add replicas
			if !statusChange {
				statusChange = true
			}
			log.Debug("current nodeID is [%d], partitionID is [%d], commit is [%d],leader nodeID is [%d], leader commit is [%d]",
				nodeID, s.Partition.Id, rs.Commit, s.Partition.LeaderID, leaderCommit)
			log.Debug("status change, because nodeID [%d] not found.", nodeID)
			s.RsStatusMap.Store(nodeID, currentStatus)
		}
	}
	// delete unExist replicas
	s.RsStatusMap.Range(func(key, value interface{}) bool {
		nodeID := key.(uint64)
		if _, find := s.Status().Replicas[nodeID]; !find {
			log.Debug("delete unExist replicas nodeID [%d]", key)
			s.RsStatusMap.Delete(key)
			// rm replicas
			if !statusChange {
				statusChange = true
			}
		}
		return true
	})
	return statusChange
}

// Apply implements the raft interface.
func (s *Store) Apply(command []byte, index uint64) (resp interface{}, err error) {
	raftCmd := &vearchpb.RaftCommand{}

	if err = vjson.Unmarshal(command, raftCmd); err != nil {
		log.Error(err)
		return nil, err
	}

	resp = s.innerApply(index, raftCmd)

	// if follow after leader this value,means can't offer server
	// just leader check
	if config.Conf().Global.RaftConsistent {
		if s.IsLeader() {
			if s.ReplicasStatusChange() {
				partitionStatus := &ReplicasStatusEntry{
					NodeID:      s.NodeID,
					PartitionID: s.Partition.Id,
					ReStatusMap: s.RsStatusMap,
				}
				log.Debug("reStatus change, leader nodeId [%d]", s.Partition.LeaderID)
				for key, value := range s.Status().Replicas {
					log.Debug("reStatus change, nodeId [%d], commit [%d]", key, value.Commit)
				}
				// send message,stop search server
				s.RsStatusC <- partitionStatus
			}
		}
	}
	return resp, nil
}

// Apply implements the raft interface.
func (s *Store) innerApply(index uint64, raftCmd *vearchpb.RaftCommand) interface{} {
	resp := new(RaftApplyResponse)
	switch raftCmd.Type {
	case vearchpb.CmdType_WRITE:
		resp.Err = s.Engine.Writer().Write(s.Ctx, raftCmd.WriteCommand)
	case vearchpb.CmdType_UPDATESPACE:
		resp = s.updateSchemaBySpace(raftCmd.UpdateSpace.Space)
	case vearchpb.CmdType_FLUSH:
		flushC, err := s.Engine.Writer().Commit(s.Ctx, int64(index))
		resp.FlushC = flushC
		resp.Err = err
	default:
		log.Error("unsupported command[%s]", raftCmd.Type)
		resp.SetErr(fmt.Errorf("unsupported command[%s]", raftCmd.Type))
	}

	// set current index to store
	s.Sn = int64(index)

	return resp
}

// changeSchema for add schema field
func (s *Store) updateSchemaBySpace(spaceBytes []byte) (rap *RaftApplyResponse) {
	rap = new(RaftApplyResponse)

	space := &entity.Space{}
	err := vjson.Unmarshal(spaceBytes, space)
	if err != nil {
		return rap.SetErr(err)
	}

	err = s.Engine.UpdateMapping(space)
	if err != nil {
		return rap.SetErr(err)
	}

	// save partition meta file
	err = psutil.SavePartitionMeta(s.GetPartition().Path, s.GetPartition().Id, space)
	if err != nil {
		return
	}

	s.SetSpace(space)

	return
}

// ApplyMemberChange implements the raft interface.
func (s *Store) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	s.Lock()
	defer s.Unlock()

	switch confChange.Type {
	case proto.ConfAddNode:
		replica := new(entity.Replica)
		if err := replica.Unmarshal(confChange.Context); err != nil {
			log.Error(err.Error())
		}
		s.EventListener.HandleRaftReplicaEvent(&RaftReplicaEvent{PartitionId: s.Partition.Id, Replica: replica})
		var exist bool
		for _, r := range s.Partition.Replicas {
			if confChange.Peer.ID == uint64(r) { // if node already in, not need add
				exist = true
			}
		}
		if !exist {
			s.Partition.Replicas = append(s.Partition.Replicas, replica.NodeID)
		}

	case proto.ConfRemoveNode:
		replica := new(entity.Replica)
		if err := replica.Unmarshal(confChange.Context); err != nil {
			log.Error(err.Error())
		}
		s.EventListener.HandleRaftReplicaEvent(&RaftReplicaEvent{PartitionId: s.Partition.Id, Delete: true, Replica: replica})

		replicas := make([]entity.NodeID, 0, len(s.Partition.Replicas)-1)
		for _, r := range s.Partition.Replicas {
			if r != replica.NodeID {
				replicas = append(replicas, r)
			}
		}

		s.Partition.Replicas = replicas

	default:
		log.Warn("err type to change node %d", confChange.Type)
	}

	return nil, nil
}

// HandleLeaderChange implements the raft interface.
func (s *Store) HandleLeaderChange(leader uint64) {
	s.Lock()
	defer s.Unlock()

	log.Info("partition[%d] node[%d] change leader to [%d]", s.Partition.Id, s.NodeID, leader)
	if leader == uint64(s.NodeID) {
		s.Partition.SetStatus(entity.PA_READWRITE)
		s.EventListener.HandleRaftLeaderEvent(&RaftLeaderEvent{PartitionId: s.Partition.Id, Leader: leader})
	} else {
		s.Partition.SetStatus(entity.PA_READONLY)
	}
}

// HandleFatalEvent implements the raft interface.
func (s *Store) HandleFatalEvent(err *raft.FatalError) {
	log.Error("partition[%d] fatal error: %s", s.Partition.Id, err.Err)
	if e := s.Close(); e != nil {
		log.Error(e.Error())
	}

	s.EventListener.HandleRaftFatalEvent(&RaftFatalEvent{
		PartitionId: s.Partition.Id,
		Cause:       err.Err,
	})
}
