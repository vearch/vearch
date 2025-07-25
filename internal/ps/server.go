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
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	"github.com/vearch/vearch/v3/internal/pkg/routine"
	rpc "github.com/vearch/vearch/v3/internal/pkg/server/rpc"
	_ "github.com/vearch/vearch/v3/internal/ps/engine/gammacb"
	"github.com/vearch/vearch/v3/internal/ps/psutil"
	"github.com/vearch/vearch/v3/internal/ps/storage/raftstore"
)

const maxTryTime = 5

var (
	defaultConcurrentNum = 32
	defaultRpcTimeOut    = 10 // 10 seconds
)

// Server partition server
type Server struct {
	mu              sync.RWMutex
	nodeID          entity.NodeID // server id
	ip              string
	partitions      sync.Map
	raftResolver    *raftstore.RaftResolver
	raftServer      *raft.RaftServer
	rpcServer       *rpc.RpcServer
	client          *client.Client
	ctx             context.Context
	ctxCancel       context.CancelFunc
	stopping        bool
	wg              sync.WaitGroup
	changeLeaderC   chan *changeLeaderEntry
	replicasStatusC chan *raftstore.ReplicasStatusEntry
	concurrent      chan bool
	concurrentNum   int
	rpcTimeOut      int
	backupStatus    map[uint32]int
}

// NewServer creates a server instance
func NewServer(ctx context.Context) *Server {
	cli, err := client.NewClient(config.Conf())
	if err != nil {
		panic(err)
	}
	changeLeaderC := make(chan *changeLeaderEntry, 1000)
	replicasStatusC := make(chan *raftstore.ReplicasStatusEntry, 1000)
	s := &Server{
		client:          cli,
		raftResolver:    raftstore.NewRaftResolver(),
		changeLeaderC:   changeLeaderC,
		replicasStatusC: replicasStatusC,
	}
	s.concurrentNum = defaultConcurrentNum
	if config.Conf().PS.ConcurrentNum > 0 {
		s.concurrentNum = config.Conf().PS.ConcurrentNum
	}
	s.concurrent = make(chan bool, s.concurrentNum)
	s.backupStatus = make(map[uint32]int)

	s.rpcTimeOut = defaultRpcTimeOut
	if config.Conf().PS.RpcTimeOut > 0 {
		s.rpcTimeOut = config.Conf().PS.RpcTimeOut
	}
	s.ctx, s.ctxCancel = context.WithCancel(ctx)

	s.rpcServer = rpc.NewRpcServer(config.LocalCastAddr, config.Conf().PS.RpcPort) // any port ???

	return s
}

type changeLeaderEntry struct {
	leader entity.NodeID
	pid    entity.PartitionID
}

// Start starts the server
func (s *Server) Start() error {
	s.wg.Add(1)
	defer func() {
		s.wg.Done()
	}()

	var err error

	s.stopping = false // set start flag for all jobs; if false, all jobs will end

	// load meta data
	nodeId := psutil.InitMeta(s.client, config.Conf().Global.Name, config.Conf().GetDataDir())
	s.nodeID = nodeId

	// load local partitions
	server := s.register()
	s.ip = server.Ip
	mserver.SetIp(server.Ip, true)

	// create raft server
	retryTime := 0
	s.raftServer, err = raftstore.StartRaftServer(nodeId, s.ip, s.raftResolver)
	for err != nil {
		log.Error("ps StartRaftServer error: %v", err)
		if retryTime > maxTryTime {
			log.Panic("ps StartRaftServer error: %v", err)
		}
		time.Sleep(5 * time.Second)
		retryTime++
		s.raftServer, err = raftstore.StartRaftServer(nodeId, s.ip, s.raftResolver)
	}

	// create and recover partitions
	s.recoverPartitions(server.PartitionIds, server.Spaces)

	// start change leader job
	s.startChangeLeaderC()

	// start heartbeat job
	s.StartHeartbeatJob()

	// start rpc server
	if err = s.rpcServer.Run(); err != nil {
		log.Panic(fmt.Sprintf("ps rpcServer run error: %v", err))
	}

	ExportToRpcHandler(s)
	ExportToRpcAdminHandler(s)

	log.Info("ps server successfully started...")

	s.wg.Wait()
	return nil
}

// Stop stops the server
func (s *Server) Close() error {
	log.Info("ps shutdown... start")
	s.stopping = true
	s.ctxCancel()

	if err := routine.Stop(); err != nil {
		log.Error(err.Error())
	}

	// close partitions will close partition engine and partition raft
	s.ClosePartitions()

	if s.raftServer != nil {
		s.raftServer.Stop()
	}

	log.Info("ps shutdown... end")

	return nil
}

func (s *Server) startChangeLeaderC() {
	go func() {
		defer func() {
			if rErr := recover(); rErr != nil {
				log.Error("recover() err: [%v]", rErr)
				log.Error("stack: [%s]", debug.Stack())
			}
		}()
		for {
			select {
			case <-s.ctx.Done():
				log.Info("startChangeLeaderC() closed")
				return
			case entry := <-s.changeLeaderC:
				log.Info("startChangeLeaderC() received a change leader event, nodeId: %d, partitionId: %d", entry.leader, entry.pid)
				s.registerMaster(entry.leader, entry.pid)
			case pStatus := <-s.replicasStatusC:
				log.Info("received a change leader status, nodeId: %d, partitionId: %d", pStatus.NodeID, pStatus.PartitionID)
				s.changeReplicas(pStatus)
			}
		}
	}()
}

func (s *Server) register() (server *entity.Server) {
	var err error

	for i := range math.MaxInt32 {
		log.Info("registering master, nodeId: [%d], attempt: %d", s.nodeID, i)
		server, err = s.client.Master().Register(s.ctx, config.Conf().Global.Name, s.nodeID, 30*time.Second)
		if err != nil {
			log.Error("register master error, nodeId: [%d], err: %s", s.nodeID, err.Error()) // some errors need to stop?
		} else if server == nil {
			log.Error("no error but server is nil, nodeId: [%d]", s.nodeID)
		} else {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if server == nil {
		s.Close()
	}
	log.Info("register master successful, nodeId: [%d]", s.nodeID)
	return server
}

// get router IPs from etcd
func (s *Server) getRouterIPS(ctx context.Context) (routerIPS []string) {
	var err error
	num := 0
	for {
		if num >= maxTryTime {
			panic(fmt.Errorf("query router IP exceeded max retry attempts"))
		}
		if routerIPS, err = s.client.Master().QueryRouter(ctx, config.Conf().Global.Name); err != nil {
			log.Error("query router IP error: [%v]", err)
			panic(fmt.Errorf("query router IP error"))
		}
		if len(routerIPS) > 0 {
			for _, IP := range routerIPS {
				config.Conf().Router.RouterIPS = append(config.Conf().Router.RouterIPS, IP)
			}
			log.Info("retrieved router info: [%v]", routerIPS)
			break
		} else {
			log.Info("router IPs are null")
		}
		num++
		time.Sleep(1 * time.Second)
	}
	return routerIPS
}

func (s *Server) HandleRaftReplicaEvent(event *raftstore.RaftReplicaEvent) {
	if event.Delete {
		log.Debug("HandleRaftReplicaEvent() delete, nodeId: [%d]", event.Replica.NodeID)
		s.raftResolver.DeleteNode(event.Replica.NodeID)
	} else {
		log.Debug("HandleRaftReplicaEvent() put, nodeId: [%d]", event.Replica.NodeID)
		if node := s.raftResolver.GetNode(event.Replica.NodeID); node == nil { // if not found, get it from master
			if server, err := s.client.Master().QueryServer(context.Background(), event.Replica.NodeID); err != nil {
				log.Error("get server info error: %s", err.Error())
			} else {
				s.raftResolver.AddNode(event.Replica.NodeID, server.Replica())
			}
		} else {
			s.raftResolver.AddNode(event.Replica.NodeID, event.Replica)
		}
	}
	if s.raftServer.IsLeader(uint64(event.PartitionId)) {
		s.changeLeaderC <- &changeLeaderEntry{
			leader: s.nodeID,
			pid:    event.PartitionId,
		}
	}
}

// on leader change, notify master
func (s *Server) HandleRaftLeaderEvent(event *raftstore.RaftLeaderEvent) {
	s.changeLeaderC <- &changeLeaderEntry{
		leader: event.Leader,
		pid:    event.PartitionId,
	}
}

// register master partition
func (s *Server) registerMaster(leader entity.NodeID, pid entity.PartitionID) {
	if leader != s.nodeID { // only leader registers partition
		return
	}

	store, ok := s.partitions.Load(pid)

	if !ok {
		log.Error("partition not found by id: [%d]", pid)
		return
	}

	partition := store.(PartitionStore).GetPartition()
	partition.LeaderID = s.nodeID

	if err := s.client.Master().RegisterPartition(context.Background(), partition); err != nil {
		log.Error("register partition error: [%s]", err.Error())
	}
}

// change replicas status
func (s *Server) changeReplicas(pStatus *raftstore.ReplicasStatusEntry) {
	var err error
	errutil.CatchError(&err)

	log.Debug("received changeReplicas message, pStatus: [%+v]", pStatus)

	store, ok := s.partitions.Load(pStatus.PartitionID)

	if !ok {
		log.Error("partition not found by id: [%d]", pStatus.PartitionID)
		return
	}

	partition := store.(PartitionStore).GetPartition()

	// initialize ReStatusMap
	if partition.ReStatusMap == nil {
		partition.ReStatusMap = make(map[uint64]uint32)
	}

	pStatus.ReStatusMap.Range(func(key, value any) bool {
		partition.ReStatusMap[key.(uint64)] = value.(uint32)
		return true
	})

	if err := s.client.Master().RegisterPartition(context.Background(), partition); err != nil {
		log.Error("register partition error: [%s]", err.Error())
	}
}

func (s *Server) HandleRaftFatalEvent(event *raftstore.RaftFatalEvent) {
	log.Error("error in raft: [%s]", event.Cause.Error())
}
