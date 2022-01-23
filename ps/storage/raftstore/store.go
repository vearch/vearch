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
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/ps/engine/register"
	"github.com/vearch/vearch/ps/psutil"
	"github.com/vearch/vearch/ps/storage"
	"github.com/vearch/vearch/util/log"
)

// Store is the default implementation of PartitionStore interface which
// contiguous slot-space with writes managed via an instance of the Raft
// consensus algorithm.

//var _  ps.PartitionStore = &Store{}

type ReplicasStatusEntry struct {
	NodeID      entity.NodeID
	PartitionID entity.PartitionID
	ReStatusMap sync.Map
}

type Store struct {
	*storage.StoreBase
	RaftPath      string
	RaftServer    *raft.RaftServer
	EventListener EventListener
	Sn            int64
	LastFlushSn   int64
	LastFlushTime time.Time
	Client        *client.Client
	raftDiffCount uint64
	RsStatusC     chan *ReplicasStatusEntry
	RsStatusMap   sync.Map
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
		RsStatusMap:   sync.Map{},
	}
	if config.Conf().PS.RaftDiffCount > 0 {
		s.raftDiffCount = config.Conf().PS.RaftDiffCount
	} else {
		s.raftDiffCount = 10000
	}
	return s, nil
}

// snapshot after load engine
func (s *Store) ReBuildEngine() (err error) {
	log.Debug("begin re build engine")
	// re create engine
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
	// sn - 1
	s.LastFlushSn = apply - 1
	s.LastFlushTime = time.Now()
	s.Partition.SetStatus(entity.PA_READONLY)

	return err
}

// Start start the store.
func (s *Store) Start() (err error) {
	// todo: gamma engine load need run after snapshot finish
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
	s.LastFlushTime = time.Now()

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

	return nil
}

// Destroy close partition store if it running currently.
func (s *Store) Close() error {

	if err := s.RaftServer.RemoveRaft(uint64(s.Partition.Id)); err != nil {
		log.Error("close raft server err : %s , Partition.Id: %d", err.Error(), s.Partition.Id)
		return err
	}

	if s.Engine != nil {
		s.Engine.Close()
	}
	s.CtxCancel() // to stop
	s.Partition.SetStatus(entity.PA_CLOSED)

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

func (s *Store) Status() *raft.Status {
	return s.RaftServer.Status(uint64(s.Partition.Id))
}

func (s *Store) GetLeader() (entity.NodeID, uint64) {
	return s.RaftServer.LeaderTerm(uint64(s.Partition.Id))
}

func (s *Store) TryToLeader() error {
	future := s.RaftServer.TryToLeader(uint64(s.Partition.Id))
	response, err := future.Response()
	if response != nil && response.(*RaftApplyResponse).Err != nil {
		return response.(*RaftApplyResponse).Err
	}
	return err
}

func (s *Store) GetUnreachable(id uint64) []uint64 {
	return s.RaftServer.GetUnreachable(id)

}

func (s *Store) GetPartition() *entity.Partition {
	return s.Partition
}

func (s *Store) RemoveDataPath() (err error) {
	// delete data and raft log
	return os.RemoveAll(s.DataPath)
}

func (s *Store) ChangeMember(changeType proto.ConfChangeType, server *entity.Server) error {
	id := uint64(s.Partition.Id)

	peer := proto.Peer{
		Type: proto.PeerNormal,
		ID:   server.ID,
	}

	bytes, err := json.Marshal(server.Replica())
	if err != nil {
		return err
	}

	future := s.RaftServer.ChangeMember(id, changeType, peer, bytes)

	response, err := future.Response()
	if err != nil {
		return err
	}

	if response != nil && response.(*RaftApplyResponse).Err != nil {
		return response.(*RaftApplyResponse).Err
	}

	return nil
}
