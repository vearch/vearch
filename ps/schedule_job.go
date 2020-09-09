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
	"time"

	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/ps/psutil"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/slice"
	"go.etcd.io/etcd/clientv3"
)

// this job for heartbeat master 1m once
func (s *Server) StartHeartbeatJob() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		server := &entity.Server{
			ID:                s.nodeID,
			Ip:                s.ip,
			RpcPort:           config.Conf().PS.RpcPort,
			RaftHeartbeatPort: config.Conf().PS.RaftHeartbeatPort,
			RaftReplicatePort: config.Conf().PS.RaftReplicatePort,
			PartitionIds:      make([]entity.PartitionID, 0, 10),
			Private:           config.Conf().PS.Private,
			Version: &entity.BuildVersion{
				BuildVersion: config.GetBuildVersion(),
				BuildTime:    config.GetBuildTime(),
				CommitID:     config.GetCommitID(),
			},
		}
		var leaseId clientv3.LeaseID = 0
		var lastPartitionIds []entity.PartitionID

		if s.stopping.Get() {
			return
		}

		server.PartitionIds = psutil.GetAllPartitions(config.Conf().GetDatas())
		ctx := context.Background()
		keepaliveC, err := s.client.Master().KeepAlive(ctx, server)
		if err != nil {
			log.Error("KeepAlive err: ", err.Error())
			return
		}
		lastPartitionIds = server.PartitionIds

		go func() {
			for {
				time.Sleep(1 * time.Minute)

				if leaseId == 0 {
					log.Info("leaseId == 0, continue...")
					continue
				}

				server.PartitionIds = psutil.GetAllPartitions(config.Conf().GetDatas())
				if slice.EqualUint32(lastPartitionIds, server.PartitionIds) {
					log.Debug("PartitionIds not change, do nothing!")
					continue
				}
				log.Info("server.PartitionIds has changed, need to put server to topo again!, leaseId: [%d]", leaseId)

				if err := s.client.Master().PutServerWithLeaseID(ctx, server, leaseId); err != nil {
					log.Error("PutServerWithLeaseID[leaseId: %d] err:", leaseId, err.Error())
				}

				lastPartitionIds = server.PartitionIds
			}
		}()

		for {
			select {
			case <-ctx.Done():
				log.Error("keep alive ctx done!")
				return
			case ka, ok := <-keepaliveC:
				if !ok {
					log.Error("keep alive channel closed!")
					time.Sleep(2 * time.Second)
					keepaliveC, err = s.client.Master().KeepAlive(ctx, server)
					if err != nil {
						log.Error("KeepAlive err: ", err.Error())
					}
					continue
				}
				leaseId = ka.ID
				log.Info("Receive keepalive, leaseId: %d, ttl:%d", ka.ID, ka.TTL)
			}
		}
	}()
}
