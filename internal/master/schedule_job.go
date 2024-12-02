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

package master

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const CronInterval = 60

func walkPartitions(masterServer *Server, partitions []*entity.Partition) {
	ctx := masterServer.ctx
	log.Debug("Start Walking Partitions!")
	for _, partition := range partitions {
		if space, err := masterServer.client.Master().QuerySpaceByID(ctx, partition.DBId, partition.SpaceId); err != nil {
			if vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code == vearchpb.ErrorEnum_SPACE_NOT_EXIST {
				log.Warnf("Could not find Space contains partition,PartitionID:[%d] so remove it from etcd!", partition.Id)
				partitionKey := entity.PartitionKey(partition.Id)
				if err := masterServer.client.Master().Delete(ctx, partitionKey); err != nil {
					log.Warnf("error:%s", err.Error())
				}
			} else {
				log.Warnf("Failed to find space according dbid:[%d] spaceid:[%d] partitionID:[%d] err:[%s]", partition.DBId, partition.SpaceId, partition.Id, err.Error())
			}
		} else {
			if space == nil {
				log.Warnf("Could not find Space contains partition,PartitionID:[%d] so remove it from etcd!", partition.Id)
				partitionKey := entity.PartitionKey(partition.Id)
				if err := masterServer.client.Master().Delete(ctx, partitionKey); err != nil {
					log.Warnf(err.Error())
				}
			}
		}
	}
	log.Debug("Complete Walking Partitions!")
}

func walkSpaces(masterServer *Server, spaces []*entity.Space) {
	ctx := masterServer.ctx
	log.Debug("Start Walking Spaces!")
	for _, space := range spaces {
		if db, err := masterServer.client.Master().Get(ctx, entity.DBKeyBody(space.DBId)); err != nil {
			log.Warnf("Failed to get key[%s] from etcd, err: [%s]", entity.DBKeyBody(space.DBId), err.Error())
		} else if db == nil {
			log.Warnf("Could not find database contains space, SpaceName: %s, SpaceID: %s, so remove it!", space.Name, space.Id)
			spaceKey := entity.SpaceKey(space.DBId, space.Id)
			if err := masterServer.client.Master().Delete(ctx, spaceKey); err != nil {
				log.Warnf("error: %s", err.Error())
			}
		}
	}
	log.Debug("Complete Walking Spaces!")
}

func removePartition(partitionServerRpcAddr string, pid entity.PartitionID) error {
	log.Debugf("Removing partition:[%s] from ps:[%s]", pid, partitionServerRpcAddr)
	return client.DeletePartition(partitionServerRpcAddr, pid)
}

func walkServers(masterServer *Server, servers []*entity.Server) {
	ctx := masterServer.ctx
	log.Debug("Start Walking Servers!")
	for _, server := range servers {
		for _, pid := range server.PartitionIds {
			if _, err := masterServer.client.Master().QueryPartition(ctx, pid); err != nil {
				if vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code == vearchpb.ErrorEnum_PARTITION_NOT_EXIST {
					log.Warnf("to remove partition:%d", pid)
					if err := removePartition(server.RpcAddr(), pid); err != nil {
						log.Warnf("Failed to remove partition: %v allocated on server: %v, and err is:%v", pid, server.ID, err)
					}
				} else {
					log.Warnf("Failed to find partition: %v, allocated on server: %v, err: %v", pid, server.ID, err)
				}
			}
		}
	}
	log.Debug("Complete Walking Servers!")
}

var errSkipJob = errors.New("skip job")

func CleanTask(masterServer *Server) {
	var err = masterServer.client.Master().STM(masterServer.ctx, func(stm concurrency.STM) error {
		timeBytes := stm.Get(entity.ClusterCleanJobKey)
		if len(timeBytes) == 0 {
			return nil
		}

		value := binary.LittleEndian.Uint16([]byte(timeBytes))

		if time.Now().UnixNano() > int64(value) {
			bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(time.Now().UnixNano()+int64(CronInterval)))
			stm.Put(entity.ClusterCleanJobKey, string(bytes))
			return nil
		}
		return errSkipJob
	})
	if err == errSkipJob {
		log.Debug("skip clean task .....")
		return
	}
	if err != nil {
		log.Errorf("clean task has err for get ClusterCleanJobKey err: %s", err.Error())
		return
	}

	log.Debug("Start clean task")
	//process partitions
	if partitions, err := masterServer.client.Master().QueryPartitions(masterServer.ctx); err != nil {
		log.Errorf("Failed to get all partitions,err: %s", err.Error())
	} else {
		walkPartitions(masterServer, partitions)
	}

	//process spaces
	if spaces, err := masterServer.client.Master().QuerySpacesByKey(masterServer.ctx, entity.PrefixSpace); err != nil {
		log.Errorf("Failed to get all spaces,err: %s", err.Error())
	} else {
		walkSpaces(masterServer, spaces)
	}

	//process servers
	if servers, err := masterServer.client.Master().QueryServers(masterServer.ctx); err != nil {
		log.Errorf("Failed to get all servers,err: %s", err.Error())
	} else {
		walkServers(masterServer, servers)
	}
}

// WatchServerJob watch ps server put and delete
func (s *Server) WatchServerJob(ctx context.Context, cli *client.Client) error {
	err := client.NewWatchServerCache(ctx, cli)
	if err != nil {
		return err
	}
	return nil
}
