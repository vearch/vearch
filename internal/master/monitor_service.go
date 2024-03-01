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
	context "context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/internal/client"
	"github.com/vearch/vearch/internal/config"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/util/log"
	"github.com/vearch/vearch/internal/util/metrics/mserver"
	"github.com/vearch/vearch/internal/util/monitoring"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

func newMonitorService(masterService *masterService, Server *etcdserver.EtcdServer) *monitorService {
	return &monitorService{masterService: masterService, etcdServer: Server}
}

// masterService is used for master administrator purpose.It should not be used by router and partition server program
type monitorService struct {
	*masterService
	etcdServer *etcdserver.EtcdServer
}

func (ms *monitorService) statsService(ctx context.Context) ([]*mserver.ServerStats, error) {
	servers, err := ms.Master().QueryServers(ctx)
	if err != nil {
		return nil, err
	}

	statsChan := make(chan *mserver.ServerStats, len(servers))

	for _, s := range servers {
		go func(s *entity.Server) {
			defer func() {
				if r := recover(); r != nil {
					statsChan <- mserver.NewErrServerStatus(s.RpcAddr(), errors.New(cast.ToString(r)))
				}
			}()
			statsChan <- client.ServerStats(s.RpcAddr())
		}(s)
	}

	result := make([]*mserver.ServerStats, 0, len(servers))

	for {
		select {
		case s := <-statsChan:
			result = append(result, s)
		case <-ctx.Done():
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_TIMEOUT, nil)
		default:
			time.Sleep(time.Millisecond * 10)
			if len(result) >= len(servers) {
				close(statsChan)
				goto out
			}
		}
	}

out:

	return result, nil
}

func (ms *monitorService) Register() {
	msConf := config.Conf().Masters.Self()
	if msConf != nil && msConf.MonitorPort > 0 {
		log.Info("register master monitor")
		monitoring.RegisterMaster(ms.monitorCallBack)
	} else {
		log.Info("skip register master monitor")
	}
}

func (ms *monitorService) partitionInfo(ctx context.Context, dbName string, spaceName string, detail string) ([]map[string]interface{}, error) {
	dbNames := make([]string, 0)
	if dbName != "" {
		dbNames = strings.Split(dbName, ",")
	}

	if len(dbNames) == 0 {
		dbs, err := ms.queryDBs(ctx)
		if err != nil {
			return nil, err
		}
		dbNames = make([]string, len(dbs))
		for i, db := range dbs {
			dbNames[i] = db.Name
		}
	}

	color := []string{"green", "yellow", "red"}

	var errors []string

	spaceNames := strings.Split(spaceName, ",")

	detail_info := false
	if detail == "true" {
		detail_info = true
	}

	resultInsideDbs := make([]map[string]interface{}, 0)
	for i := range dbNames {
		dbName := dbNames[i]

		dbId, err := ms.Master().QueryDBName2Id(ctx, dbName)
		if err != nil {
			errors = append(errors, dbName+" find dbID err: "+err.Error())
			continue
		}

		spaces, err := ms.Master().QuerySpaces(ctx, dbId)
		if err != nil {
			errors = append(errors, dbName+" find space err: "+err.Error())
			continue
		}

		dbStatus := 0

		resultInsideSpaces := make([]map[string]interface{}, 0, len(spaces))
		for _, space := range spaces {

			spaceName := space.Name

			if len(spaceNames) > 1 || spaceNames[0] != "" { //filter spaceName by user define
				var index = -1

				for i, name := range spaceNames {
					if name == spaceName {
						index = i
						break
					}
				}

				if index < 0 {
					continue
				}
			}

			spaceStatus := 0
			resultInsidePartition := make([]*entity.PartitionInfo, 0)
			for _, spacePartition := range space.Partitions {
				p, err := ms.Master().QueryPartition(ctx, spacePartition.Id)
				if err != nil {
					errors = append(errors, fmt.Sprintf("partition:[%d] not found in space: [%s]", spacePartition.Id, spaceName))
					continue
				}

				pStatus := 0

				nodeID := p.LeaderID
				if nodeID == 0 {
					errors = append(errors, fmt.Sprintf("partition:[%d] no leader in space: [%s]", spacePartition.Id, spaceName))
					pStatus = 2
					nodeID = p.Replicas[0]
				}

				server, err := ms.Master().QueryServer(ctx, nodeID)
				if err != nil {
					errors = append(errors, fmt.Sprintf("server:[%d] not found in space: [%s] , partition:[%d]", nodeID, spaceName, spacePartition.Id))
					pStatus = 2
					continue
				}

				partitionInfo, err := client.PartitionInfo(server.RpcAddr(), p.Id, detail_info)
				if err != nil {
					errors = append(errors, fmt.Sprintf("query space:[%s] server:[%d] partition:[%d] info err :[%s]", spaceName, nodeID, spacePartition.Id, err.Error()))
					partitionInfo = &entity.PartitionInfo{}
					pStatus = 2
				} else {
					if len(partitionInfo.Unreachable) > 0 {
						pStatus = 1
					}
				}

				replicasStatus := make(map[entity.NodeID]string)
				for nodeID, status := range p.ReStatusMap {
					if status == entity.ReplicasOK {
						replicasStatus[nodeID] = "ReplicasOK"
					} else {
						replicasStatus[nodeID] = "ReplicasNotReady"
					}
				}

				//this must from space.Partitions
				partitionInfo.PartitionID = spacePartition.Id
				partitionInfo.Color = color[pStatus]
				partitionInfo.ReplicaNum = len(p.Replicas)
				partitionInfo.Ip = server.Ip
				partitionInfo.NodeID = server.ID
				partitionInfo.RepStatus = replicasStatus

				resultInsidePartition = append(resultInsidePartition, partitionInfo)

				if pStatus > spaceStatus {
					spaceStatus = pStatus
				}
			}

			docNum := uint64(0)
			size := int64(0)
			for _, p := range resultInsidePartition {
				docNum += cast.ToUint64(p.DocNum)
				size += cast.ToInt64(p.Size)
			}
			resultSpace := make(map[string]interface{})
			resultSpace["name"] = spaceName
			resultSpace["partition_num"] = len(resultInsidePartition)
			resultSpace["replica_num"] = int(space.ReplicaNum)
			resultSpace["doc_num"] = docNum
			resultSpace["size"] = size
			resultSpace["partitions"] = resultInsidePartition
			resultSpace["status"] = color[spaceStatus]
			resultInsideSpaces = append(resultInsideSpaces, resultSpace)

			if spaceStatus > dbStatus {
				dbStatus = spaceStatus
			}
		}

		docNum := uint64(0)
		size := int64(0)
		for _, s := range resultInsideSpaces {
			docNum += cast.ToUint64(s["doc_num"])
			size += cast.ToInt64(s["size"])
		}
		resultDb := make(map[string]interface{})
		resultDb["db_name"] = dbName
		resultDb["space_num"] = len(spaces)
		resultDb["doc_num"] = docNum
		resultDb["size"] = size
		resultDb["spaces"] = resultInsideSpaces
		resultDb["status"] = color[dbStatus]
		resultDb["errors"] = errors
		resultInsideDbs = append(resultInsideDbs, resultDb)
	}

	return resultInsideDbs, nil
}

func (ms *monitorService) monitorCallBack(masterMonitor *monitoring.MasterMonitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	ip := config.Conf().Masters.Self().Address
	stats := mserver.NewServerStats()

	masterMonitor.Mem.WithLabelValues("master", ip).Set(stats.Mem.UsedPercent)
	masterMonitor.FS.WithLabelValues("master", ip).Set(float64(stats.Fs.Free))
	masterMonitor.NetIn.WithLabelValues("master", ip).Set(float64(stats.Net.InPerSec))
	masterMonitor.NetOut.WithLabelValues("master", ip).Set(float64(stats.Net.OutPerSec))
	masterMonitor.GC.WithLabelValues("master", ip).Set(float64(stats.GC.GcCount))
	masterMonitor.Routines.WithLabelValues("master", ip).Set(float64(stats.GC.Goroutines))
	servers, err := ms.masterService.Master().QueryServers(ctx)
	if err != nil {
		log.Error("got server by prefix err:[%s]", err.Error())
	}
	masterMonitor.ServerNum.Set(float64(len(servers)))

	dbs, err := ms.masterService.queryDBs(ctx)
	if err != nil {
		log.Error("got db by prefix err:[%s]", err.Error())
	}
	masterMonitor.DBNum.Set(float64(len(dbs)))
	spaces, err := ms.masterService.Master().QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		log.Error("got space by prefix err:[%s]", err.Error())
	}
	masterMonitor.SpaceNum.Set(float64(len(spaces)))
	statsChan := make(chan *mserver.ServerStats, len(servers))
	for _, s := range servers {
		go func(s *entity.Server) {
			defer func() {
				if r := recover(); r != nil {
					statsChan <- mserver.NewErrServerStatus(s.RpcAddr(), errors.New(cast.ToString(r)))
				}
			}()
			statsChan <- client.ServerStats(s.RpcAddr())
		}(s)
	}

	result := make([]*mserver.ServerStats, 0, len(servers))

	for {
		select {
		case s := <-statsChan:
			masterMonitor.CPU.WithLabelValues("ps", s.Ip).Set(1 - s.Cpu.IdlePercent)
			masterMonitor.Mem.WithLabelValues("ps", s.Ip).Set(s.Mem.UsedPercent)
			masterMonitor.FS.WithLabelValues("ps", s.Ip).Set(float64(s.Fs.Free))
			masterMonitor.NetIn.WithLabelValues("ps", s.Ip).Set(float64(s.Net.InPerSec))
			masterMonitor.NetOut.WithLabelValues("ps", s.Ip).Set(float64(s.Net.OutPerSec))
			masterMonitor.GC.WithLabelValues("ps", s.Ip).Set(float64(s.GC.GcCount))
			masterMonitor.Routines.WithLabelValues("ps", s.Ip).Set(float64(s.GC.Goroutines))

			masterMonitor.PartitionNum.WithLabelValues("ps", s.Ip).Set(float64(len(s.PartitionInfos)))

			leaderNum := float64(0)
			for _, p := range s.PartitionInfos {
				if p.RaftStatus.Leader == p.RaftStatus.NodeID {
					leaderNum++
				}
			}
			masterMonitor.PSLeaderNum.WithLabelValues(s.Ip).Set(leaderNum)

			result = append(result, s)
		case <-ctx.Done():
			log.Error("monitor timeout")
			return
		default:
			time.Sleep(time.Millisecond * 10)
			if len(result) >= len(servers) {
				close(statsChan)
				goto out
			}
		}
	}
out:
	spacePartitionIDMap := make(map[entity.PartitionID]*entity.Space)

	for _, s := range spaces {
		for _, p := range s.Partitions {
			spacePartitionIDMap[p.Id] = s
		}
	}

	dbMap := make(map[entity.DBID]string)
	for _, db := range dbs {
		dbMap[db.Id] = db.Name
	}

	partitionNum := 0
	docNumMap := make(map[*entity.Space]uint64)
	sizeMap := make(map[*entity.Space]int64)
	for _, s := range result {
		for _, p := range s.PartitionInfos {
			if p.RaftStatus.Leader == p.RaftStatus.NodeID {
				partitionNum++
				docNumMap[spacePartitionIDMap[p.PartitionID]] += p.DocNum
				masterMonitor.PSPartitionDoc.WithLabelValues(ip, cast.ToString(p.PartitionID)).Set(float64(p.DocNum))
				masterMonitor.PSPartitionSize.WithLabelValues(ip, cast.ToString(p.PartitionID)).Set(float64(p.Size))
			}
			sizeMap[spacePartitionIDMap[p.PartitionID]] += p.Size

		}
	}

	masterMonitor.PartitionNum.WithLabelValues("master", ip).Set(float64(partitionNum))

	for space, value := range docNumMap {
		masterMonitor.SpaceDoc.WithLabelValues(dbMap[space.DBId], space.Name, cast.ToString(space.Id)).Set(float64(value))
	}

	for space, value := range sizeMap {
		masterMonitor.SpaceSize.WithLabelValues(dbMap[space.DBId], space.Name, cast.ToString(space.Id)).Set(float64(value))
	}

	masterMonitor.CPU.WithLabelValues("master", ip).Set(1 - stats.Cpu.IdlePercent) //set it end  so changed
}
