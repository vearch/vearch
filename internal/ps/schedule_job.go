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
	"os"
	"slices"
	"time"

	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/ps/psutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func comparePartitionIDs(oldIDs, newIDs []uint32) (bool, []uint32, []uint32) {
	if len(oldIDs) != len(newIDs) {
		return false, nil, nil
	}

	oldMap := make(map[uint32]struct{}, len(oldIDs))
	for _, id := range oldIDs {
		oldMap[id] = struct{}{}
	}

	added := make([]uint32, 0)
	for _, id := range newIDs {
		if _, exists := oldMap[id]; !exists {
			added = append(added, id)
		}
		delete(oldMap, id)
	}

	removed := make([]uint32, 0, len(oldMap))
	for id := range oldMap {
		removed = append(removed, id)
	}

	return len(added) == 0 && len(removed) == 0, added, removed
}

// this job for heartbeat master 1m once
func (s *Server) StartHeartbeatJob() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		const (
			checkInterval  = 5 * time.Second
			reconnectDelay = 2 * time.Second
			maxRetries     = 5
		)
		server := &entity.Server{
			ID:                s.nodeID,
			Ip:                s.ip,
			HostIp:            os.Getenv("VEARCH_HOST_IP"),
			HostRack:          os.Getenv("VEARCH_HOST_RACK"),
			HostZone:          os.Getenv("VEARCH_HOST_ZONE"),
			ResourceName:      config.Conf().Global.ResourceName,
			RpcPort:           config.Conf().PS.RpcPort,
			RaftHeartbeatPort: config.Conf().PS.RaftHeartbeatPort,
			RaftReplicatePort: config.Conf().PS.RaftReplicatePort,
			PartitionIds:      make([]entity.PartitionID, 0, 10),
			Spaces:            make([]*entity.Space, 0, 10),
			Private:           config.Conf().PS.Private,
			Version: &entity.BuildVersion{
				BuildVersion: config.GetBuildVersion(),
				BuildTime:    config.GetBuildTime(),
				CommitID:     config.GetCommitID(),
			},
		}
		var leaseId clientv3.LeaseID = 0
		var lastPartitionIds []entity.PartitionID

		if s.stopping {
			return
		}

		server.PartitionIds = psutil.GetAllPartitions(config.Conf().GetDatas())
		for _, pid := range server.PartitionIds {
			log.Info("partition id: %d", pid)
			partition, err := s.client.Master().QueryPartition(context.Background(), pid)
			if err != nil {
				log.Error("QueryPartition err: %v", err.Error())
				continue
			}
			if partition == nil {
				log.Error("QueryPartition partition is nil")
				continue
			}

			bFound := slices.Contains(partition.Replicas, s.nodeID)

			if !bFound {
				log.Error("partition %d not found in replicas", pid)
				path := config.Conf().GetDataDirBySlot(config.PS, pid)
				psutil.ClearPartition(path, pid)
			}
		}
		ctx := context.Background()
		keepaliveC, err := s.client.Master().KeepAlive(ctx, server)
		if err != nil {
			log.Error("KeepAlive err: %v", err.Error())
			return
		}
		lastPartitionIds = server.PartitionIds

		go func() {
			ticker := time.NewTicker(checkInterval)
			defer ticker.Stop()

			retries := 0

			for {
				select {
				case <-ticker.C:
					s.raftResolver.RangeNodes(s.updateResolver)

					if leaseId == 0 {
						continue
					}

					newPartitionIds := psutil.GetAllPartitions(config.Conf().GetDatas())

					equal, added, removed := comparePartitionIDs(lastPartitionIds, newPartitionIds)
					if equal {
						continue
					}

					if len(added) > 0 {
						log.Info("Partitions added: %v", added)
					}
					if len(removed) > 0 {
						log.Info("Partitions removed: %v", removed)
					}

					server.PartitionIds = newPartitionIds
					err := s.client.Master().PutServerWithLeaseID(ctx, server, leaseId)
					if err != nil {
						retries++
						log.Error("Failed to update server info (attempt %d/%d): %v", retries, maxRetries, err)
						if retries >= maxRetries {
							leaseId = 0
							retries = 0
							log.Warn("Max retries reached, forcing lease renewal")
							continue
						}
					} else {
						retries = 0
						lastPartitionIds = newPartitionIds
						log.Info("Server partition info updated successfully, new count: %d", len(newPartitionIds))
					}

				case <-ctx.Done():
					log.Info("Partition monitor stopped")
					return
				}
			}

		}()

		for {
			select {
			case <-ctx.Done():
				log.Info("Heartbeat job terminated")
				return
			case ka, ok := <-keepaliveC:
				if !ok {
					log.Warn("Keepalive channel closed, reconnecting in %v...", reconnectDelay)
					time.Sleep(reconnectDelay)

					newCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					keepaliveC, err = s.client.Master().KeepAlive(newCtx, server)
					if err != nil {
						log.Error("Failed to reestablish keepalive: %v", err)
						leaseId = 0
					}
					continue
				}

				oldLeaseId := leaseId
				leaseId = ka.ID

				if oldLeaseId != leaseId {
					log.Info("Lease ID updated: %d -> %d (TTL: %d)", oldLeaseId, leaseId, ka.TTL)
				}
				if err := s.client.Master().CheckMasterConfig(ctx); err != nil {
					log.Error("Master config check failed: %v", err)
				}
			}
		}
	}()
}

func (s *Server) updateResolver(key, value any) bool {
	id, _ := key.(entity.NodeID)
	if server, err := s.client.Master().QueryServer(context.Background(), id); err != nil {
		log.Error("partition recovery get server info err: %s", err.Error())
	} else {
		s.raftResolver.UpdateNode(id, server.Replica())
	}
	return true
}
