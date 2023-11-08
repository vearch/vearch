// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package monitor

import (
	"context"
	"errors"

	"github.com/coreos/etcd/etcdserver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/util/errutil"
	"github.com/vearch/vearch/util/metrics/mserver"

	"net/http"
	"sync"
	"time"

	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util/log"
)

const (
	tp50  = 0.50
	tp90  = 0.90
	tp95  = 0.95
	tp99  = 0.99
	tp999 = 0.999
	max   = 1
)

var once sync.Once

type MonitorService struct {
	summaryDesc  *prometheus.Desc // summary
	dbDesc       *prometheus.Desc // gauge
	mutex        sync.Mutex
	masterClient *client.Client
	etcdServer   *etcdserver.EtcdServer
}

func Register(masterClient *client.Client, etcdServer *etcdserver.EtcdServer, monitorPort uint16) {
	var err error
	defer errutil.CatchError(&err)
	once.Do(func() {
		prometheus.MustRegister(NewMetricCollector(masterClient, etcdServer))
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			if monitorPort > 0 {
				log.Info("monitoring start in Port:", monitorPort)
				if err := http.ListenAndServe(":"+cast.ToString(monitorPort), nil); err != nil {
					log.Error("Error occur when start server %v", err)
				}
			} else {
				log.Info("skip register monitoring")
			}
		}()
	})
}

func NewMetricCollector(masterClient *client.Client, etcdServer *etcdserver.EtcdServer) prometheus.Collector {
	return &MonitorService{
		masterClient: masterClient,
		etcdServer:   etcdServer,
		summaryDesc: prometheus.NewDesc(
			"vearch_request_duration_milliseconds",
			"metric for request api",
			[]string{"key", "method"},
			nil,
		),
		dbDesc: prometheus.NewDesc(
			"vearch_db_info",
			"vearch database info",
			[]string{"metric", "tag1", "tag2"}, nil),
	}
}

// Describe returns all descriptions of the collector.
func (collector *MonitorService) Describe(ch chan<- *prometheus.Desc) {
	ch <- collector.summaryDesc
	ch <- collector.dbDesc
}

// current node is master
func (ms *MonitorService) isMaster() bool {
	if ms.masterClient == nil {
		return false
	}
	if ms.etcdServer == nil {
		return false
	}
	if uint64(ms.etcdServer.ID()) == ms.etcdServer.Lead() {
		return true
	}
	return false
}

// Collect returns the current state of all metrics of the collector.
func (ms *MonitorService) Collect(ch chan<- prometheus.Metric) {
	var collectErr *error
	defer errutil.CatchError(collectErr)
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	metrics := SliceMetric()
	if len(metrics) == 0 {
		ch <- prometheus.MustNewConstSummary(ms.summaryDesc,
			0, 0, map[float64]float64{1: 0}, "nil", "nil",
		)
	} else {
		for _, element := range metrics {
			ch <- prometheus.MustNewConstSummary(
				ms.summaryDesc,
				uint64(element.Digest.Count()), element.Sum,
				map[float64]float64{
					tp50:  element.Digest.Quantile(tp50),
					tp90:  element.Digest.Quantile(tp90),
					tp95:  element.Digest.Quantile(tp95),
					tp99:  element.Digest.Quantile(tp99),
					tp999: element.Digest.Quantile(tp999),
					max:   element.Digest.Quantile(max),
				},
				element.Name, config.Conf().Global.Name,
			)
		}
	}

	if !ms.isMaster() {
		ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, 0, "nil", "nil", "nil")
		return
	}

	// start collect business info
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	ip := config.Conf().Masters.Self().Address
	stats := mserver.NewServerStats()
	servers, err := ms.masterClient.Master().QueryServers(ctx)
	if err != nil {
		log.Error("got server by prefix err:[%s]", err.Error())
	}

	ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(len(servers)), "ServerNum", "*", "*")

	dbs, err := ms.masterClient.Master().QueryDBs(ctx)
	if err != nil {
		log.Error("got db by prefix err:[%s]", err.Error())
	}

	ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(len(dbs)), "DBNum", "*", "*")
	spaces, err := ms.masterClient.Master().QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		log.Error("got space by prefix err:[%s]", err.Error())
	}

	ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(len(spaces)), "SpaceNum", "*", "*")

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
			ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(len(spaces)), "SpaceNum", s.Ip, "*")
			leaderNum := float64(0)
			for _, p := range s.PartitionInfos {
				if p.RaftStatus == nil {
					continue
				}
				if p.RaftStatus.Leader == p.RaftStatus.NodeID {
					leaderNum++
				}
			}
			ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(leaderNum), "leaderNum", s.Ip, "*")
			result = append(result, s)
		case <-ctx.Done():
			log.Error("monitor timeout")
			return
		default:
			time.Sleep(time.Millisecond * 10)
			if len(result) >= len(servers) {
				close(statsChan)
			}
		}
		if len(result) >= len(servers) {
			break
		}
	}

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
			if p.RaftStatus == nil {
				continue
			}
			if p.RaftStatus.Leader == p.RaftStatus.NodeID {
				partitionNum++
				docNumMap[spacePartitionIDMap[p.PartitionID]] += p.DocNum
				ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(p.DocNum), "PartitionDoc", p.Ip, cast.ToString(p.PartitionID))
				ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(p.Size), "PartitionSize", p.Ip, cast.ToString(p.PartitionID))
			}
			sizeMap[spacePartitionIDMap[p.PartitionID]] += p.Size
		}
	}
	ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(partitionNum), "partitionNum", "master", "*")

	for space, value := range docNumMap {
		if space == nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(value), "docNum", dbMap[space.DBId], space.Name)
	}

	for space, value := range sizeMap {
		ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(value), "sizeMap", dbMap[space.DBId], space.Name)
	}
	ch <- prometheus.MustNewConstMetric(ms.dbDesc, prometheus.CounterValue, float64(1-stats.Cpu.IdlePercent), "Cpu", "master", ip)
}
