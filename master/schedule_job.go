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
	"fmt"
	"time"

	"github.com/jasonlvhit/gocron"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util/log"
	"go.etcd.io/etcd/clientv3/concurrency"
)

const CronInterval = 60

var spaceChannel = make(chan *entity.Space)

func walkPartitions(masterServer *Server, partitions []*entity.Partition) {
	ctx := masterServer.ctx
	log.Info("Start Walking Partitions!")
	for _, partition := range partitions {
		if space, err := masterServer.client.Master().QuerySpaceById(ctx, partition.DBId, partition.SpaceId); err != nil {
			if pkg.ErrCode(err) == pkg.ERRCODE_SPACE_NOTEXISTS {
				log.Info("Could not find Space contains partition,PartitionID:[%d] so remove it from etcd!", partition.Id)
				partitionKey := entity.PartitionKey(partition.Id)
				if err := masterServer.client.Master().Delete(ctx, partitionKey); err != nil {
					log.Error("error:%s", err.Error())
				}
			} else {
				log.Error("Failed to find space according dbid:[%d] spaceid:[%d] partitionID:[%d] err:[%s]", partition.DBId, partition.SpaceId, partition.Id, err.Error())
			}
		} else {
			if space == nil {
				log.Info("Could not find Space contains partition,PartitionID:[%d] so remove it from etcd!", partition.Id)
				partitionKey := entity.PartitionKey(partition.Id)
				if err := masterServer.client.Master().Delete(ctx, partitionKey); err != nil {
					log.Error(err.Error())
				}
			}
		}
	}
	log.Info("Complete Walking Partitions!")
}

func walkSpaces(masterServer *Server, spaces []*entity.Space) {
	ctx := masterServer.ctx
	log.Info("Start Walking Spaces!")
	for _, space := range spaces {
		spaceChannel <- space
		if db, err := masterServer.client.Master().Get(ctx, entity.DBKeyBody(space.DBId)); err != nil {
			log.Error("Failed to find database contains space,SpaceName:", space.Name, " SpaceID:", space.Id, " err:", err)
		} else if db == nil {
			log.Info("Could not find database contains space,,SpaceName:", space.Name, " SpaceID:", space.Id, " so remove it!")
			spaceKey := entity.SpaceKey(space.DBId, space.Id)
			if err := masterServer.client.Master().Delete(ctx, spaceKey); err != nil {
				log.Error("error:%s", err.Error())
			}
		}
	}
	log.Info("Complete Walking Spaces!")
}

func removePartition(masterServer *Server, partitionServerRpcAddr string, pid entity.PartitionID) error {
	log.Debug("Removing partition:[%s] from ps:[%s]", pid, partitionServerRpcAddr)
	return masterServer.client.PS().B().Admin(partitionServerRpcAddr).DeletePartition(pid)
}

func walkServers(masterServer *Server, servers []*entity.Server) {
	ctx := masterServer.ctx
	log.Info("Start Walking Servers!")
	for _, server := range servers {
		for _, pid := range server.PartitionIds {
			if _, err := masterServer.client.Master().QueryPartition(ctx, pid); err != nil {
				if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
					log.Info("to remove partition:%d", pid)
					if err := removePartition(masterServer, server.RpcAddr(), pid); err != nil {
						log.Warn("Failed to remove partition:%v allocated on server:%v,and err is:%v", pid, server.ID, err)
					}
				} else {
					log.Warn("Failed to find partition:", pid, " allocated on server:", server.ID, " err:", err)
				}
			}
		}
	}
	log.Info("Complete Walking Servers!")
}

var skipJob = fmt.Errorf("skip job")

func cleanTask(masterServer *Server) {

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
		return skipJob
	})
	if err == skipJob {
		log.Info("skip clean task .....")
		return
	}
	if err != nil {
		log.Error("clean task has err for get ClusterCleanJobKey err: %s", err.Error())
		return
	}

	log.Debug("Start clean task")
	//process partitions
	if partitions, err := masterServer.client.Master().QueryPartitions(masterServer.ctx); err != nil {
		log.Error("Failed to get all partitions,err =", err.Error())
	} else {
		walkPartitions(masterServer, partitions)
	}

	//process spaces
	if spaces, err := masterServer.client.Master().QuerySpacesByKey(masterServer.ctx, entity.PrefixSpace); err != nil {
		log.Error("Failed to get all spaces,err =", err.Error())
	} else {
		walkSpaces(masterServer, spaces)
	}

	//process servers
	if servers, err := masterServer.client.Master().QueryServers(masterServer.ctx); err != nil {
		log.Error("Failed to get all servers,err = ", err.Error())
	} else {
		walkServers(masterServer, servers)
	}
}

//the job check partition infos and fix it
func (s *Server) StartCleanJon(ctx context.Context) {
	//diff partition and space remove outside partition , it need lock space by distlock
	//diff space and server partition , to del partition in ps
	scheduler := gocron.NewScheduler()
	scheduler.Every(CronInterval).Seconds().Do(cleanTask, s)
	<-scheduler.Start()
}
