// Copyright 2015 The Cockroach Authors.
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

package mserver

import (
	"context"
	"sync"

	"github.com/tiglabs/log"
	"github.com/vearch/vearch/util/metrics/export"

	"github.com/vearch/vearch/util/metrics"
	"github.com/vearch/vearch/util/metrics/sysstat"
)

var ms = &metricServer{}

type metricServer struct {
	Ip                string
	MetricRegistry    *metrics.Registry
	MetricExport      metrics.Exporter
	MetricExportTimer *metrics.ExportTimer
	MetricRuntime     *sysstat.RuntimeStatSampler
	once              sync.Once
	lock              sync.Mutex
}

func Start(ctx context.Context, diskPaths []string) {
	ms.once.Do(func() {
		// set metrics
		ms.MetricRegistry = metrics.NewRegistry()
		ms.MetricExport = export.NewMemoryExporter()
		ms.MetricRuntime = sysstat.NewRuntimeStatSampler(sysstat.RuntimeStatOption{
			Ctx:        ctx,
			HeapSample: false,
			DiskPath:   diskPaths,
		})
		ms.MetricRegistry.AddMetricStruct(ms.MetricRuntime)
		ms.MetricExportTimer = metrics.NewExportTimer(ctx, 0)

		if err := ms.MetricRuntime.Start(); err != nil {
			log.Error("start metric runtime err: %s", err.Error())
		}
		if err := ms.MetricExportTimer.Start(); err != nil {
			log.Error("start metric runtime err: %s", err.Error())
		}
	})
}

func NewServerStats() *ServerStats {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	return newServerStats(ms.Ip, ms.MetricRegistry.GetLabels(), ms)
}

func SetIp(ip string, replace bool) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.Ip != "" {
		if replace {
			ms.Ip = ip
		}
	} else {
		ms.Ip = ip
	}

}

func AddLabel(key, value string) {
	ms.MetricRegistry.AddLabel(key, value)
}
