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
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/vearch/vearch/internal/config"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/pkg/log"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/ps/engine"
	"github.com/vearch/vearch/internal/ps/psutil"
	"github.com/vearch/vearch/internal/ps/storage/raftstore"
)

type Base interface {
	Start() error

	// Destroy close partition store if it running currently.
	Close() error

	// Destroy close partition store if it running currently and remove all data file from filesystem.
	Destroy() error

	// GetMeta returns meta information about this store.
	GetPartition() *entity.Partition

	//GetEngine return engine
	GetEngine() engine.Engine

	//space change API
	GetSpace() entity.Space

	// SetSpace
	SetSpace(space *entity.Space)
}

type Raft interface {
	GetLeader() (entity.NodeID, uint64)

	IsLeader() bool

	TryToLeader() error

	Status() *raft.Status

	GetVersion() uint64

	GetUnreachable(id uint64) []uint64

	ChangeMember(changeType proto.ConfChangeType, server *entity.Server) error
}

type PartitionStore interface {
	Base

	Raft

	UpdateSpace(ctx context.Context, space *entity.Space) error

	GetDocument(ctx context.Context, readLeader bool, doc *vearchpb.Document, getByDocId bool, next bool) (err error)

	Write(ctx context.Context, request *vearchpb.DocCmd) (err error)

	Flush(ctx context.Context) error

	Search(ctx context.Context, query *vearchpb.SearchRequest, response *vearchpb.SearchResponse) error
}

func (s *Server) GetPartition(id entity.PartitionID) (partition PartitionStore) {
	if p, _ := s.partitions.Load(id); p != nil {
		partition = p.(PartitionStore)
	}
	return
}

func (s *Server) RangePartition(fun func(entity.PartitionID, PartitionStore)) {
	s.partitions.Range(func(key, value interface{}) bool {
		fun(key.(entity.PartitionID), value.(PartitionStore))
		return true
	})
}

// load partition for in disk
func (s *Server) LoadPartition(ctx context.Context, pid entity.PartitionID, spaces []*entity.Space) (PartitionStore, error) {
	// space, err := psutil.LoadPartitionMeta(config.Conf().GetDataDirBySlot(config.PS, pid), pid)

	// if err != nil {
	// 	return nil, err
	// }
	i := -1
	for k, s := range spaces {
		for _, p := range s.Partitions {
			if p.Id == pid {
				i = k
				break
			}
		}
		if i > 0 {
			break
		}
	}
	if i < 0 {
		return nil, fmt.Errorf("cannot found pid [%d]", pid)
	}
	space := spaces[i]

	store, err := raftstore.CreateStore(ctx, pid, s.nodeID, space, s.raftServer, s, s.client)
	if err != nil {
		return nil, err
	}
	// partition status chan
	store.RsStatusC = s.replicasStatusC
	replicas := store.GetPartition().Replicas
	for _, replica := range replicas {
		if server, err := s.client.Master().QueryServer(context.Background(), replica); err != nil {
			log.Error("partition recovery get server info err: %s", err.Error())
		} else {
			s.raftResolver.AddNode(replica, server.Replica())
		}
	}

	if err := store.Start(); err != nil {
		return nil, err
	}

	s.partitions.Store(pid, store)

	return store, nil
}

func (s *Server) CreatePartition(ctx context.Context, space *entity.Space, pid entity.PartitionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	store, err := raftstore.CreateStore(ctx, pid, s.nodeID, space, s.raftServer, s, s.client)
	if err != nil {
		return err
	}
	store.RsStatusC = s.replicasStatusC
	if _, ok := s.partitions.LoadOrStore(pid, store); ok {
		log.Warn("partition is already exist partition id:[%d]", pid)
		if err := store.Close(); err != nil {
			log.Error("partitions close err : %s", err.Error())
		}
	} else {
		for _, nodeId := range store.Partition.Replicas {
			if server, err := s.client.Master().QueryServer(ctx, nodeId); err != nil {
				log.Error("get server info err %s", err.Error())
				return err
			} else {
				s.raftResolver.AddNode(nodeId, server.Replica())
			}
		}
		if err = store.Start(); err != nil {
			return err
		}
	}

	s.partitions.Store(pid, store)
	return nil
}

func (s *Server) DeleteReplica(id entity.PartitionID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if p, ok := s.partitions.Load(id); ok {
		s.partitions.Delete(id)
		if partition, is := p.(PartitionStore); is {
			s.raftResolver.DeleteNode(s.nodeID)
			if err := partition.Destroy(); err != nil {
				log.Error("delete partition[%v] fail cause: %v", id, err)
			}
		}
	}

	psutil.ClearPartition(config.Conf().GetDataDirBySlot(config.PS, id), id)
	log.Info("delete partition[%d] success", id)
}

func (s *Server) DeletePartition(id entity.PartitionID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if p, ok := s.partitions.Load(id); ok {
		if partition, is := p.(PartitionStore); is {
			for _, r := range partition.GetPartition().Replicas {
				s.raftResolver.DeleteNode(r)
			}
			if err := partition.Destroy(); err != nil {
				log.Error("delete partition[%v] fail cause: %v", id, err)
				return
			}

			if partition.GetPartition().GetStatus() == entity.PA_INVALID {
				s.partitions.Delete(id)
			}
		}
	}

	psutil.ClearPartition(config.Conf().GetDataDirBySlot(config.PS, id), id)
	log.Info("delete partition:[%d] success", id)

	// delete partition cache
	if _, ok := s.partitions.Load(id); ok {
		s.partitions.Delete(id)
	}
}

func (s *Server) PartitionNum() int {
	var count int
	s.partitions.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func (s *Server) ClosePartitions() {
	log.Info("ClosePartitions() invoked...")
	s.partitions.Range(func(key, value interface{}) bool {
		partition := value.(PartitionStore)
		err := partition.Close()
		if err != nil {
			log.Error("ClosePartitions() partition close has err: [%v]", err)
		}
		return true
	})
}

func (s *Server) recoverPartitions(pids []entity.PartitionID, spaces []*entity.Space) {
	wg := new(sync.WaitGroup)
	wg.Add(len(pids))

	ctx := context.Background()

	// parallel execution recovery
	for i := 0; i < len(pids); i++ {
		idx := i
		go func(pid entity.PartitionID) {
			defer wg.Done()

			log.Debug("starting recover partition[%d]...", pid)
			_, err := s.LoadPartition(ctx, pid, spaces)
			if err != nil {
				log.Error("init partition err :[%s]", err.Error())
			} else {
				log.Debug("partition[%d] recovered complete", pid)
			}
		}(pids[idx])
	}

	wg.Wait()
}
