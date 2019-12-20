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
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/vearch/vearch/proto/request"
	"sync"

	"github.com/vearch/vearch/proto/response"

	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/ps/psutil"
	"github.com/vearch/vearch/ps/storage/raftstore"
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

	GetDocument(ctx context.Context, readLeader bool, docID string) (doc *response.DocResult, err error)

	GetDocuments(ctx context.Context, readLeader bool, docIds []string) (results response.DocResults, err error)

	DeleteByQuery(ctx context.Context, readLeader bool, query *request.SearchRequest) (delCount int, err error)

	Search(ctx context.Context, readLeader bool, query *request.SearchRequest) (result *response.SearchResponse, err error)

	MSearch(ctx context.Context, readLeader bool, query *request.SearchRequest) (result response.SearchResponses, err error)

	//you can use ctx to cancel the stream , when this function returned will close resultChan
	StreamSearch(ctx context.Context, readLeader bool, query *request.SearchRequest, resultChan chan *response.DocResult) error

	Write(ctx context.Context, request *pspb.DocCmd) (result *response.DocResult, err error)

	Flush(ctx context.Context) error
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

//load partition for in disk
func (s *Server) LoadPartition(ctx context.Context, pid entity.PartitionID) (PartitionStore, error) {

	space, err := psutil.LoadPartitionMeta(config.Conf().GetDataDirBySlot(config.PS, pid), pid)

	if err != nil {
		return nil, err
	}

	store, err := raftstore.CreateStore(ctx, pid, s.nodeID, space, s.raftServer, s, s.client)
	if err != nil {
		return nil, err
	}

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
		s.partitions.Delete(id)
		if partition, is := p.(PartitionStore); is {
			if partition.GetPartition().GetStatus() != entity.PA_INVALID {
				for _, r := range partition.GetPartition().Replicas {
					s.raftResolver.DeleteNode(r)
				}
				if err := partition.Destroy(); err != nil {
					log.Error("delete partition[%v] fail cause: %v", id, err)
				}
			}
		}

	}

	psutil.ClearPartition(config.Conf().GetDataDirBySlot(config.PS, id), id)
	log.Info("delete partition[%d] success", id)
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

func (s *Server) recoverPartitions(pids []entity.PartitionID) {
	wg := new(sync.WaitGroup)
	wg.Add(len(pids))

	ctx := context.Background()

	// parallel execution recovery
	for i := 0; i < len(pids); i++ {
		idx := i
		go func(pid entity.PartitionID) {
			defer wg.Done()

			log.Debug("starting recover partition[%d]...", pid)
			_, err := s.LoadPartition(ctx, pid)
			if err != nil {
				log.Error("init partition err :[%s]", err.Error())
			}
			log.Debug("partition[%d] recovered complete", pid)
		}(pids[idx])
	}

	wg.Wait()
}
