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
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	"github.com/vearch/vearch/v3/internal/pkg/monitoring"
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

func (ms *monitorService) Register() {
	msConf := config.Conf().Masters.Self()
	if msConf != nil && msConf.MonitorPort > 0 {
		log.Info("register master monitor")
		monitoring.RegisterMaster(ms.monitorCallBack)
	} else {
		log.Info("skip register master monitor")
	}
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

	dbs, err := ms.masterService.DB().QueryDBs(ctx)
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
