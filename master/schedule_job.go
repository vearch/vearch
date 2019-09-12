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
	"encoding/json"
	"fmt"
	"github.com/vearch/vearch/util/vearchlog"
	"time"

	"github.com/jasonlvhit/gocron"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"go.etcd.io/etcd/clientv3/concurrency"
)

const CronInterval = 60

var spaceChannel = make(chan *entity.Space)

func walkPartitions(masterServer *Server, partitions []*entity.Partition) {
	ctx := masterServer.ctx
	log.Info("Start Walking Partitions!")
	for _, partition := range partitions {
		if space, err := masterServer.client.Master().QuerySpaceById(ctx, partition.DBId, partition.SpaceId); err != nil {
			if err == pkg.ErrMasterSpaceNotExists {
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
				if err == pkg.ErrPartitionNotExist {
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

	if masterServer.monitor != nil {
		masterServer.monitor.Alive() //add alive monitor
	}

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
	//check space frozen
	go s.frozenJob(ctx)

	//diff partition and space remove outside partition , it need lock space by distlock
	//diff space and server partition , to del partition in ps
	scheduler := gocron.NewScheduler()
	scheduler.Every(CronInterval).Seconds().Do(cleanTask, s)
	<-scheduler.Start()
}

//frozen job fro destroy partition or get partition min max value
func (s *Server) frozenJob(ctx context.Context) {
	for space := range spaceChannel {
		func() {
			if !space.CanFrozen() || space.Engine.ZoneField == "" {
				return
			}

			delPartition := make(map[entity.PartitionID]bool, 0)
			changePartition := make(map[entity.PartitionID]struct{ min, max int64 }, 0)

			for _, partition := range space.Partitions {
				if !partition.Frozen {
					continue
				}

				log.Info("to find partition min and max value")

				if partition.MaxValue == 0 && partition.MinValue == 0 {

					current, err := s.client.Master().QueryPartition(ctx, partition.Id)
					if err != nil {
						log.Error(err.Error())
						continue
					}

					server, err := s.client.Master().QueryServer(context.Background(), current.LeaderID)
					if err != nil {
						log.Error(err.Error())
						continue
					}

					if time.Now().UnixNano()-partition.UpdateTime < int64(5*time.Minute) {
						log.Info("partition:[%d] can less 6 minute so skip get min and max value ", partition.Id)
						continue
					}

					max, min, err := s.client.PS().B().Admin(server.RpcAddr()).MaxMinValueByZoneFile(partition.Id)
					if err != nil {
						log.Error("go min max value from:[%s] err:%s ", server.RpcAddr(), err.Error())
						continue
					}

					changePartition[partition.Id] = struct{ min, max int64 }{min: min, max: max}
				} else if space.CanExpire() && time.Now().UnixNano()-int64(partition.MaxValue) > space.Engine.ExpireMinute*60e9 {
					delPartition[partition.Id] = true
				}
			}

			if len(changePartition) > 0 || len(delPartition) > 0 {

				log.Info("space:[%s] has changed so to update space", space.Name)

				dbName, err := s.client.Master().QueryDBId2Name(ctx, space.DBId)
				if err != nil {
					log.Error(err.Error())
					return
				}

				lock := s.client.Master().NewLock(ctx, entity.LockSpaceKey(dbName, space.Name), 30*time.Second)

				err = lock.Lock()
				if err != nil {
					log.Error(err.Error())
					return
				}

				defer vearchlog.FunIfNotNil(lock.Unlock)

				nowSpace, err := s.client.Master().QuerySpaceById(ctx, space.DBId, space.Id)
				if err != nil {
					log.Error(err.Error())
					return
				}

				newPartitions := make([]*entity.Partition, 0, len(nowSpace.Partitions))
				for _, p := range nowSpace.Partitions {

					if delPartition[p.Id] {
						continue
					}

					if maxMin, ok := changePartition[p.Id]; ok {
						p.MinValue = maxMin.min
						p.MaxValue = maxMin.max
					}

					newPartitions = append(newPartitions, p)
				}

				nowSpace.Version++
				nowSpace.Partitions = newPartitions
				nowSpace.PartitionNum = len(nowSpace.Partitions)
				marshal, err := json.Marshal(nowSpace)
				if err != nil {
					log.Error(err.Error())
					return
				}
				if err = s.client.Master().Update(ctx, entity.SpaceKey(nowSpace.DBId, nowSpace.Id), marshal); err != nil {
					log.Error(err.Error())
					return
				}
			}
		}()
	}

}
