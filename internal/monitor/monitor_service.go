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
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/master/services"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	"go.etcd.io/etcd/server/v3/etcdserver"

	"net/http"
	"sync"
	"time"

	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
)

var (
	once           sync.Once
	metricRegistry = prometheus.NewRegistry()
)

type MonitorService struct {
	mutex        sync.Mutex
	masterClient *client.Client
	etcdServer   *etcdserver.EtcdServer

	requestDuration *prometheus.HistogramVec
	requestCount    *prometheus.CounterVec

	serverCount    prometheus.Gauge
	dbCount        prometheus.Gauge
	spaceCount     prometheus.Gauge
	clusterHealth  prometheus.Gauge
	partitionCount prometheus.Gauge

	spaceDocs     *prometheus.GaugeVec
	spaceSize     *prometheus.GaugeVec
	partitionDocs *prometheus.GaugeVec
	partitionSize *prometheus.GaugeVec
	leaderCount   *prometheus.GaugeVec

	diskTotal       *prometheus.GaugeVec
	diskFree        *prometheus.GaugeVec
	diskUsed        *prometheus.GaugeVec
	diskUsedPercent *prometheus.GaugeVec
	cpuUsage        *prometheus.GaugeVec
}

func Register(masterClient *client.Client, etcdServer *etcdserver.EtcdServer, monitorPort uint16) {
	once.Do(func() {
		collector := newMetricCollector(masterClient, etcdServer)
		metricRegistry.MustRegister(collector)

		http.Handle("/metrics", promhttp.HandlerFor(
			prometheus.Gatherers{metricRegistry, prometheus.DefaultGatherer},
			promhttp.HandlerOpts{
				EnableOpenMetrics: true,
				Registry:          metricRegistry,
			},
		))

		go func() {
			if monitorPort > 0 {
				log.Info("monitoring start in Port: %v", monitorPort)
				if err := http.ListenAndServe(":"+cast.ToString(monitorPort), nil); err != nil {
					log.Error("Error occur when start server %v", err)
				}
			} else {
				log.Info("skip register monitoring")
			}
		}()
	})
}

func newMetricCollector(masterClient *client.Client, etcdServer *etcdserver.EtcdServer) *MonitorService {
	return &MonitorService{
		masterClient: masterClient,
		etcdServer:   etcdServer,

		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "vearch_request_duration_milliseconds",
				Help:    "Vearch API request durations in milliseconds",
				Buckets: prometheus.ExponentialBuckets(1, 2, 15),
			},
			[]string{"cluster", "api"},
		),

		requestCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "vearch_request_count",
				Help: "Total number of Vearch API requests",
			},
			[]string{"cluster", "api"},
		),

		serverCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "vearch_server_count",
				Help: "Number of Vearch servers",
			},
		),
		dbCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "vearch_db_count",
				Help: "Number of Vearch databases",
			},
		),
		spaceCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "vearch_space_count",
				Help: "Number of Vearch spaces",
			},
		),
		clusterHealth: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "vearch_cluster_health",
				Help: "Vearch cluster health (0=healthy, 0.5=warning, 1=unhealthy)",
			},
		),
		partitionCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "vearch_partition_count",
				Help: "Number of Vearch partitions",
			},
		),

		spaceDocs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_space_documents",
				Help: "Number of documents in Vearch space",
			},
			[]string{"db", "space"},
		),
		spaceSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_space_size_bytes",
				Help: "Size of Vearch space in bytes",
			},
			[]string{"db", "space"},
		),
		partitionDocs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_partition_documents",
				Help: "Number of documents in Vearch partition",
			},
			[]string{"cluster", "partition_id"},
		),
		partitionSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_partition_size_bytes",
				Help: "Size of Vearch partition in bytes",
			},
			[]string{"cluster", "partition_id"},
		),
		leaderCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_leader_count",
				Help: "Number of partition leaders on this node",
			},
			[]string{"cluster"},
		),

		diskTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_disk_total_bytes",
				Help: "Total disk space in bytes",
			},
			[]string{"ip"},
		),
		diskFree: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_disk_free_bytes",
				Help: "Free disk space in bytes",
			},
			[]string{"ip"},
		),
		diskUsed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_disk_used_bytes",
				Help: "Used disk space in bytes",
			},
			[]string{"ip"},
		),
		diskUsedPercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_disk_used_percent",
				Help: "Percentage of disk space used",
			},
			[]string{"ip"},
		),
		cpuUsage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vearch_cpu_usage",
				Help: "CPU usage (1 - idle percent)",
			},
			[]string{"node_type", "ip"},
		),
	}
}

func (ms *MonitorService) Describe(ch chan<- *prometheus.Desc) {
	ms.requestDuration.Describe(ch)
	ms.requestCount.Describe(ch)

	ms.serverCount.Describe(ch)
	ms.dbCount.Describe(ch)
	ms.spaceCount.Describe(ch)
	ms.clusterHealth.Describe(ch)
	ms.partitionCount.Describe(ch)

	ms.spaceDocs.Describe(ch)
	ms.spaceSize.Describe(ch)
	ms.partitionDocs.Describe(ch)
	ms.partitionSize.Describe(ch)
	ms.leaderCount.Describe(ch)

	ms.diskTotal.Describe(ch)
	ms.diskFree.Describe(ch)
	ms.diskUsed.Describe(ch)
	ms.diskUsedPercent.Describe(ch)
	ms.cpuUsage.Describe(ch)
}

func (ms *MonitorService) isMaster() bool {
	if ms.masterClient == nil || ms.etcdServer == nil {
		return false
	}
	return uint64(ms.etcdServer.ID()) == ms.etcdServer.Lead()
}

func collectServerStats(ctx context.Context, servers []*entity.Server) []*mserver.ServerStats {
	results := make([]*mserver.ServerStats, 0, len(servers))
	resultCh := make(chan *mserver.ServerStats, len(servers))

	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Add(1)
		go func(s *entity.Server) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error("Panic while collecting stats from %s: %v", s.RpcAddr(), r)
					resultCh <- mserver.NewErrServerStatus(s.RpcAddr(), errors.New(cast.ToString(r)))
				}
			}()

			serverCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			select {
			case <-serverCtx.Done():
				log.Warn("Timeout while collecting stats from %s", s.RpcAddr())
				resultCh <- mserver.NewErrServerStatus(s.RpcAddr(), serverCtx.Err())
			case resultCh <- client.ServerStats(s.RpcAddr()):
			}
		}(s)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for stat := range resultCh {
		results = append(results, stat)
	}

	return results
}

// Collect all metrics
func (ms *MonitorService) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 4096)
			length := runtime.Stack(stack, false)
			log.Error("Panic in metrics collection: %v\nStack:\n%s", r, stack[:length])
		}
	}()

	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	// Update request metrics
	metrics := SliceMetric()
	if len(metrics) == 0 {
		// No request statistics, add an empty metric
		ms.requestDuration.WithLabelValues("nil", "nil").Observe(0)
	} else {
		for _, element := range metrics {
			histogram := ms.requestDuration.WithLabelValues(config.Conf().Global.Name, element.Name)

			count := element.Digest.Count()
			if count > 0 {
				histogram.Observe(element.Sum / float64(count))
				ms.requestCount.WithLabelValues(config.Conf().Global.Name, element.Name).Add(float64(count))
			}
		}
	}

	if !ms.isMaster() {
		ms.requestDuration.Collect(ch)
		ms.requestCount.Collect(ch)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	ip := config.Conf().Masters.Self().Address
	stats := mserver.NewServerStats()

	servers, err := ms.masterClient.Master().QueryServers(ctx)
	if err != nil {
		log.Error("Failed to query servers: %s", err.Error())
	} else {
		ms.serverCount.Set(float64(len(servers)))
	}

	dbs, err := ms.masterClient.Master().QueryDBs(ctx)
	if err != nil {
		log.Error("Failed to query databases: %s", err.Error())
	} else {
		ms.dbCount.Set(float64(len(dbs)))
	}

	spacePartitionIDMap := make(map[entity.PartitionID]*entity.Space)
	spaces, err := ms.masterClient.Master().QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		log.Error("Failed to query spaces: %s", err.Error())
	} else {
		ms.spaceCount.Set(float64(len(spaces)))

		for _, s := range spaces {
			for _, p := range s.Partitions {
				spacePartitionIDMap[p.Id] = s
			}
		}
	}

	partitionService := services.NewPartitionService(ms.masterClient)
	dbService := services.NewDBService(ms.masterClient)
	spaceService := services.NewSpaceService(ms.masterClient)
	health, err := partitionService.PartitionInfo(ctx, dbService, spaceService, "", "", "")
	if err != nil {
		log.Error("Failed to get partition info: %s", err.Error())
	} else {
		clusterHealth := 0.0
		for _, h := range health {
			if h["status"] == "red" {
				clusterHealth = 1.0
				break
			} else if h["status"] == "yellow" {
				clusterHealth = 0.5
			}
		}
		log.Debug("Cluster health: %f", clusterHealth)
		ms.clusterHealth.Set(clusterHealth)
	}

	result := collectServerStats(ctx, servers)

	docNumMap := make(map[*entity.Space]uint64)
	sizeMap := make(map[*entity.Space]int64)
	partitionNum := 0

	for _, s := range result {
		if s.Fs != nil {
			ms.diskTotal.WithLabelValues(s.Ip).Set(float64(s.Fs.Total))
			ms.diskFree.WithLabelValues(s.Ip).Set(float64(s.Fs.Free))
			ms.diskUsed.WithLabelValues(s.Ip).Set(float64(s.Fs.Used))
			ms.diskUsedPercent.WithLabelValues(s.Ip).Set(float64(s.Fs.UsedPercent))
		}

		leaderNum := float64(0)
		for _, p := range s.PartitionInfos {
			if p.RaftStatus == nil {
				continue
			}
			if p.RaftStatus.Leader == p.RaftStatus.NodeID {
				leaderNum++
				partitionNum++

				// Update partition document count and size
				if space, ok := spacePartitionIDMap[p.PartitionID]; ok {
					docNumMap[space] += p.DocNum
				}

				// Set partition-level metrics
				ms.partitionDocs.WithLabelValues(s.Ip, cast.ToString(p.PartitionID)).Set(float64(p.DocNum))
				ms.partitionSize.WithLabelValues(s.Ip, cast.ToString(p.PartitionID)).Set(float64(p.Size))

				// Accumulate space size
				if space, ok := spacePartitionIDMap[p.PartitionID]; ok {
					sizeMap[space] += p.Size
				}
			}
		}

		ms.leaderCount.WithLabelValues(s.Ip).Set(leaderNum)
	}

	ms.partitionCount.Set(float64(partitionNum))

	dbMap := make(map[entity.DBID]string)
	for _, db := range dbs {
		dbMap[db.Id] = db.Name
	}

	for space, value := range docNumMap {
		if space == nil {
			continue
		}
		ms.spaceDocs.WithLabelValues(dbMap[space.DBId], space.Name).Set(float64(value))
	}

	for space, value := range sizeMap {
		if space == nil {
			continue
		}
		ms.spaceSize.WithLabelValues(dbMap[space.DBId], space.Name).Set(float64(value))
	}

	ms.cpuUsage.WithLabelValues("master", ip).Set(1.0 - stats.Cpu.IdlePercent)

	ms.requestDuration.Collect(ch)
	ms.requestCount.Collect(ch)
	ms.serverCount.Collect(ch)
	ms.dbCount.Collect(ch)
	ms.spaceCount.Collect(ch)
	ms.clusterHealth.Collect(ch)
	ms.partitionCount.Collect(ch)
	ms.spaceDocs.Collect(ch)
	ms.spaceSize.Collect(ch)
	ms.partitionDocs.Collect(ch)
	ms.partitionSize.Collect(ch)
	ms.leaderCount.Collect(ch)
	ms.diskTotal.Collect(ch)
	ms.diskFree.Collect(ch)
	ms.diskUsed.Collect(ch)
	ms.diskUsedPercent.Collect(ch)
	ms.cpuUsage.Collect(ch)
}
