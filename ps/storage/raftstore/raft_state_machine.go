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
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb/raftpb"
	"github.com/vearch/vearch/ps/psutil"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
)

// Apply implements the raft interface.
func (s *Store) Apply(command []byte, index uint64) (resp interface{}, err error) {
	raftCmd := raftpb.CreateRaftCommand()

	if err = raftCmd.Unmarshal(command); err != nil {
		panic(err)
	}

	resp = s.innerApply(command, index, raftCmd)

	if err := raftCmd.Close(); err != nil {
		log.Error(err.Error())
	}
	return resp, nil
}

// Apply implements the raft interface.
func (s *Store) innerApply(command []byte, index uint64, raftCmd *raftpb.RaftCommand) interface{} {
	resp := new(RaftApplyResponse)
	switch raftCmd.Type {
	case raftpb.CmdType_WRITE:
		resp.Result = s.Engine.Writer().Write(s.Ctx, raftCmd.WriteCommand)
	case raftpb.CmdType_UPDATESPACE:
		resp = s.updateSchemaBySpace(raftCmd.UpdateSpace.Space, raftCmd.UpdateSpace.Version)
	case raftpb.CmdType_FLUSH:
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
func (s *Store) updateSchemaBySpace(spaceBytes []byte, version uint64) (rap *RaftApplyResponse) {
	rap = new(RaftApplyResponse)
	/*if s.Space.Version > version {
		log.Warn("update schema version not right, old:[%d] new:[%d] ", s.Space.Version, version)
		return
	}*/

	space := &entity.Space{}
	err := cbjson.Unmarshal(spaceBytes, space)
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

	log.Info("partition[%d] change leader to %d", s.Partition.Id, leader)
	if leader == uint64(s.NodeID) {
		s.Partition.SetStatus(entity.PA_READWRITE)
		s.EventListener.HandleRaftLeaderEvent(&RaftLeaderEvent{PartitionId: s.Partition.Id, Leader: leader})
	} else {
		s.Partition.SetStatus(entity.PA_READONLY)
	}
}

// HandleFatalEvent implements the raft interface.
func (s *Store) HandleFatalEvent(err *raft.FatalError) {
	log.Error("partition[%d] occur fatal error: %s", s.Partition.Id, err.Err)
	if e := s.Close(); e != nil {
		log.Error(e.Error())
	}

	s.EventListener.HandleRaftFatalEvent(&RaftFatalEvent{
		PartitionId: s.Partition.Id,
		Cause:       err.Err,
	})
}
