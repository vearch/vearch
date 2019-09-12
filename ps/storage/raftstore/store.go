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
	"context"
	"fmt"
	"github.com/vearch/vearch/ps/psutil"
	"os"

	"github.com/tiglabs/log"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/ps/engine/register"
	"github.com/vearch/vearch/ps/storage"
)

// Store is the default implementation of PartitionStore interface which
// contiguous slot-space with writes managed via an instance of the Raft
// consensus algorithm.

type Store struct {
	*storage.StoreBase
	RaftPath      string
	RaftServer    *raft.RaftServer
	EventListener EventListener
	Sn            int64
	LastFlushSn   int64
	Client        *client.Client
}

// CreateStore create an instance of Store.
func CreateStore(ctx context.Context, pID entity.PartitionID, nodeID entity.NodeID, space *entity.Space, raftServer *raft.RaftServer, eventListener EventListener, client *client.Client) (*Store, error) {

	path := config.Conf().GetDataDirBySlot(config.PS, pID)
	dataPath, raftPath, metaPath, err := psutil.CreatePartitionPaths(path, space, pID) //FIXME: it will double writer space when load space

	if err != nil {
		return nil, err
	}

	base, err := storage.NewStoreBase(ctx, pID, nodeID, path, dataPath, metaPath, space)
	if err != nil {
		return nil, err
	}
	s := &Store{
		StoreBase:     base,
		RaftPath:      raftPath,
		RaftServer:    raftServer,
		EventListener: eventListener,
		Client:        client,
	}
	return s, nil
}

// Start start the store.
func (s *Store) Start() (err error) {
	s.Engine, err = register.Build(s.Space.Engine.Name, register.EngineConfig{
		Path:        s.DataPath,
		Space:       s.Space,
		PartitionID: s.Partition.Id,
		DWPTNum:     config.Conf().PS.EngineDWPTNum,
	})
	if err != nil {
		return err
	}

	apply, err := s.Engine.Reader().ReadSN(s.Ctx)
	if err != nil {
		s.Engine.Close()
		return err
	}
	s.LastFlushSn = apply

	s.Partition.SetStatus(entity.PA_READONLY)

	raftStore, err := wal.NewStorage(s.RaftPath, nil)
	if err != nil {
		s.Engine.Close()
		return fmt.Errorf("start partition[%d] open raft store engine error: %s", s.Partition.Id, err.Error())
	}

	partition := s.Space.GetPartition(s.Partition.Id)
	if partition == nil {
		return fmt.Errorf("can not found partition by id:[%d]", s.Partition.Id)
	}

	raftConf := &raft.RaftConfig{
		ID:           uint64(s.Partition.Id),
		Applied:      uint64(apply),
		Peers:        make([]proto.Peer, 0, len(partition.Replicas)),
		Storage:      raftStore,
		StateMachine: s,
	}

	if s.Partition.Replicas[0] == s.NodeID {
		raftConf.Leader = s.NodeID
	}

	for _, repl := range partition.Replicas {
		peer := proto.Peer{Type: proto.PeerNormal, ID: uint64(repl)}
		raftConf.Peers = append(raftConf.Peers, peer)
	}
	if err = s.RaftServer.CreateRaft(raftConf); err != nil {
		s.Engine.Close()
		return fmt.Errorf("start partition[%d] create raft error: %s", s.Partition.Id, err)
	}

	// Start Raft Sn Flush worker
	s.startFlushJob()
	// Start Raft Truncate Worker
	s.startTruncateJob(apply)

	// Start frozen check worker
	if config.Conf().PS.MaxSize > 0 && s.Space.CanFrozen() {
		s.startFrozenJob()
	}

	return nil
}

// Destroy close partition store if it running currently.
func (s *Store) Close() error {
	s.CloseOnce.Do(func() {

		s.Partition.SetStatus(entity.PA_CLOSED)

		if err := s.RaftServer.RemoveRaft(uint64(s.Partition.Id)); err != nil {
			log.Error("close raft server err : %s , Partition.Id: %d", err.Error(), s.Partition.Id)
		}
		if s.Engine != nil {
			s.Engine.Close()
		}

		s.CtxCancel() // to stop
	})
	return nil
}

// Destroy close partition store if it running currently and remove all data file from filesystem.
func (s *Store) Destroy() (err error) {
	if err = s.Close(); err != nil {
		return
	}
	// delete data and raft log
	if err = os.RemoveAll(s.DataPath); err != nil {
		return
	}
	if err = os.RemoveAll(s.RaftPath); err != nil {
		return
	}
	if err = os.RemoveAll(s.MetaPath); err != nil {
		return
	}
	return
}

func (s *Store) IsLeader() bool {
	leaderID, _ := s.GetLeader()
	return s.NodeID == leaderID
}

func (s *Store) GetLeader() (entity.NodeID, uint64) {
	return s.RaftServer.LeaderTerm(uint64(s.Partition.Id))
}

func (s *Store) GetUnreachable(id uint64) []uint64 {
	return s.RaftServer.GetUnreachable(id)

}

func (s *Store) GetPartition() *entity.Partition {
	return s.Partition
}
