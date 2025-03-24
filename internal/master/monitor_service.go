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
	"net/http"
	"time"

	"github.com/pkg/errors"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

func newMonitorService(masterService *masterService, server *etcdserver.EtcdServer) *monitorService {
	return &monitorService{masterService: masterService, etcdServer: server}
}

var masterCallBack func(masterMonitor *MasterMonitor)

type MasterMonitor struct {
	CPU      *prometheus.GaugeVec
	Mem      *prometheus.GaugeVec
	FS       *prometheus.GaugeVec
	NetIn    *prometheus.GaugeVec
	NetOut   *prometheus.GaugeVec
	GC       *prometheus.GaugeVec
	Routines *prometheus.GaugeVec

	// all has
	PartitionNum *prometheus.GaugeVec

	// schema num
	ServerNum prometheus.Gauge
	DBNum     prometheus.Gauge
	SpaceNum  prometheus.Gauge
	SpaceDoc  *prometheus.GaugeVec
	SpaceSize *prometheus.GaugeVec

	// ps value
	PSLeaderNum     *prometheus.GaugeVec
	PSPartitionSize *prometheus.GaugeVec
	PSPartitionDoc  *prometheus.GaugeVec

	// cluster
	ClusterHealth prometheus.Gauge
}

func RegisterMaster(call func(masterMonitor *MasterMonitor)) {
	masterCallBack = call

	mm := &MasterMonitor{
		CPU:          newGaugeVec("cpu", "cpu", "type", "ip"),
		Mem:          newGaugeVec("mem", "memory", "type", "ip"),
		FS:           newGaugeVec("fs", "file disk", "type", "ip"),
		NetIn:        newGaugeVec("net_in", "net in per second", "type", "ip"),
		NetOut:       newGaugeVec("net_out", "net out per second", "type", "ip"),
		GC:           newGaugeVec("gc", "go gc", "type", "ip"),
		Routines:     newGaugeVec("routines", "go routines", "type", "ip"),
		ServerNum:    newGauge("server_num", "server number"),
		DBNum:        newGauge("db_num", "database number"),
		SpaceNum:     newGauge("space_num", "space number"),
		SpaceDoc:     newGaugeVec("space_doc", "space document number", "db_name", "space_name", "space_id"),
		SpaceSize:    newGaugeVec("space_size", "space disk size", "db_name", "space_name", "space_id"),
		PartitionNum: newGaugeVec("partition_num", "space document number", "type", "ip"),

		PSLeaderNum:     newGaugeVec("leader_num", "partition has number", "ip"),
		PSPartitionSize: newGaugeVec("partition_size", "single partition size", "ip", "partition_id"),
		PSPartitionDoc:  newGaugeVec("partition_doc", "single partition doc number", "ip", "partition_id"),
		ClusterHealth:   newGauge("cluster_health", "cluster health"),
	}

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(mm.CPU, mm.Mem, mm.FS, mm.NetIn, mm.NetOut, mm.GC, mm.Routines, mm.ServerNum, mm.DBNum, mm.SpaceNum, mm.SpaceDoc, mm.SpaceSize, mm.PartitionNum)
	registry.MustRegister(mm.PSLeaderNum, mm.PSPartitionSize, mm.PSPartitionDoc)

	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Error("monitor has err:[%v]", e)
			}
		}()
		for {
			time.Sleep(time.Second * 15)
			if masterCallBack != nil {
				masterCallBack(mm)
			}
		}
	}()

	go func() {
		self := config.Conf().Masters.Self()
		http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		if err := http.ListenAndServe(":"+cast.ToString(self.MonitorPort), nil); err != nil {
			panic(err)
		}
	}()
}

func newCount(name, help string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: name,
			Help: help,
		},
		nil,
	)
}

func newGaugeVec(Name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: Name,
		Help: help,
	}, labels)
}

func newGauge(Name, help string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{
		Name: Name,
		Help: help,
	})
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
		RegisterMaster(ms.monitorCallBack)
	} else {
		log.Info("skip register master monitor")
	}
}

func (ms *monitorService) monitorCallBack(masterMonitor *MasterMonitor) {
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
