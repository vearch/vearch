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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

func StartRaftServer(nodeId entity.NodeID, ip string, resolver raft.SocketResolver) (*raft.RaftServer, error) {
	rc := raft.DefaultConfig()
	rc.NodeID = uint64(nodeId)
	rc.LeaseCheck = true
	rc.HeartbeatAddr = fmt.Sprintf(ip + ":" + cast.ToString(config.Conf().PS.RaftHeartbeatPort))
	rc.ReplicateAddr = fmt.Sprintf(ip + ":" + cast.ToString(config.Conf().PS.RaftReplicatePort))
	rc.Resolver = resolver
	rc.TickInterval = 500 * time.Millisecond
	if config.Conf().PS.RaftReplicaConcurrency > 0 {
		rc.MaxReplConcurrency = config.Conf().PS.RaftReplicaConcurrency
	}
	if config.Conf().PS.RaftSnapConcurrency > 0 {
		rc.MaxSnapConcurrency = config.Conf().PS.RaftSnapConcurrency
	}
	if config.Conf().PS.RaftHeartbeatInterval > 0 {
		rc.TickInterval = time.Millisecond * time.Duration(config.Conf().PS.RaftHeartbeatInterval)
	}
	if config.Conf().PS.RaftRetainLogs > 0 {
		rc.RetainLogs = config.Conf().PS.RaftRetainLogs
	}

	return raft.NewRaftServer(rc)
}

// this interface for event , server implements it
type EventListener interface {
	HandleRaftReplicaEvent(event *RaftReplicaEvent)
	HandleRaftLeaderEvent(event *RaftLeaderEvent)
	HandleRaftFatalEvent(event *RaftFatalEvent)
}

type RaftReplicaEvent struct {
	PartitionId entity.PartitionID
	Delete      bool
	Replica     *entity.Replica
}

type RaftLeaderEvent struct {
	PartitionId entity.PartitionID
	Leader      entity.NodeID
}

type RaftFatalEvent struct {
	PartitionId entity.PartitionID
	Cause       error
}

type nodeRef struct {
	refCount      int32
	heartbeatAddr string
	replicateAddr string
	rpcAddr       string
}

// RaftResolver resolve NodeID to net.Addr addresses
type RaftResolver struct {
	nodes *sync.Map
}

// NewRaftResolver create RaftResolver
func NewRaftResolver() *RaftResolver {
	return &RaftResolver{
		nodes: new(sync.Map),
	}
}

func (r *RaftResolver) AddNode(id entity.NodeID, replica *entity.Replica) {
	ref := new(nodeRef)
	ref.heartbeatAddr = replica.HeartbeatAddr
	ref.replicateAddr = replica.ReplicateAddr
	ref.rpcAddr = replica.RpcAddr
	obj, _ := r.nodes.LoadOrStore(id, ref)
	log.Debug("RaftResolver AddNode(), nodeId: [%d], nodeRef:[%v], obj:[%v]", id, ref, obj)
	r.nodes.Range(func(key, value any) bool {
		log.Debug("r.nodes, key: [%v], value: [%v]", key, value)
		return true
	})
	atomic.AddInt32(&(obj.(*nodeRef).refCount), 1)
}

func (r *RaftResolver) UpdateNode(id entity.NodeID, replica *entity.Replica) {
	ref := new(nodeRef)
	ref.heartbeatAddr = replica.HeartbeatAddr
	ref.replicateAddr = replica.ReplicateAddr
	ref.rpcAddr = replica.RpcAddr
	obj, loaded := r.nodes.LoadOrStore(id, ref)
	if loaded {
		atomic.StoreInt32(&ref.refCount, obj.(*nodeRef).refCount)
	} else {
		atomic.AddInt32(&ref.refCount, 1)
	}
	r.nodes.Store(id, ref)
	// r.nodes.Range(func(key, value interface{}) bool {
	// 	log.Debug("r.nodes, key: [%v], value: [%v]", key, value)
	// 	return true
	// })
}

func (r *RaftResolver) ToReplica(id entity.NodeID) (replica *entity.Replica) {
	replica = &entity.Replica{
		NodeID: id,
	}
	node := r.GetNode(id)
	if node == nil {
		return replica
	}
	replica.RpcAddr = node.rpcAddr
	return replica
}

func (r *RaftResolver) DeleteNode(id entity.NodeID) {
	if obj, _ := r.nodes.Load(id); obj != nil {
		log.Debug("RaftResolver DeleteNode(), nodeId: [%d]", id)
		r.nodes.Range(func(key, value interface{}) bool {
			log.Debug("r.nodes, key: [%v], value: [%v]", key, value)
			return true
		})
		if count := atomic.AddInt32(&(obj.(*nodeRef).refCount), -1); count <= 0 {
			r.nodes.Delete(id)
		}
	}
}

func (r *RaftResolver) GetNode(id entity.NodeID) *nodeRef {
	if obj, _ := r.nodes.Load(id); obj != nil {
		return obj.(*nodeRef)
	}
	return nil
}

func (r *RaftResolver) RangeNodes(f func(key, value interface{}) bool) {
	r.nodes.Range(f)
}

// NodeAddress resolve NodeID to net.Addr addresses.
func (r *RaftResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (string, error) {
	node := r.GetNode(entity.NodeID(nodeID))
	if node == nil {
		return "", vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("cannot get node network information, nodeID=[%d]", nodeID))
	}

	switch stype {
	case raft.HeartBeat:
		return node.heartbeatAddr, nil
	case raft.Replicate:
		return node.replicateAddr, nil
	default:
		return "", vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unknown socket type[%v]", stype))
	}
}
