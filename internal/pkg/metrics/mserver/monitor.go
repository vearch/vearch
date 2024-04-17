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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, eitherproto/entity/space.go express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mserver

import (
	"net/http"

	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/pkg/metrics"
	"github.com/vearch/vearch/internal/pkg/metrics/sysstat"
	"github.com/vearch/vearch/internal/proto/vearchpb"
)

func NewErrServerStatus(ip string, err error) *ServerStats {
	vErr := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err)
	return &ServerStats{
		Ip:     ip,
		Status: int64(http.StatusInternalServerError),
		Err:    vErr.Error(),
	}
}

// stats machine infos
func newServerStats(ip string, lables []metrics.LabelPair, ss *metricServer) *ServerStats {
	return &ServerStats{
		Ip:     ip,
		Labels: lables,
		Mem:    NewMemStats(ss.MetricRuntime),
		Fs:     NewFsStats(ss.MetricRuntime),
		Swap:   NewSwapStats(ss.MetricRuntime),
		Cpu:    NewCpuStats(ss.MetricRuntime),
		Net:    NewNetStats(ss.MetricRuntime),
		GC:     NewGCStats(ss.MetricRuntime),
	}
}

type ServerStats struct {
	Status         int64                   `json:"status"`
	Ip             string                  `json:"ip"`
	Labels         []metrics.LabelPair     `json:"labels"`
	Mem            *MemStats               `json:"mem,omitempty"`
	Swap           *SwapStats              `json:"swap,omitempty"`
	Fs             *FsStats                `json:"fs,omitempty"`
	Cpu            *CpuStats               `json:"cpu,omitempty"`
	Net            *NetStats               `json:"net,omitempty"`
	GC             *GCStats                `json:"gc,omitempty"`
	Err            string                  `json:"err,omitempty"`
	ActiveConn     int                     `json:"active_conn,omitempty"`
	PartitionInfos []*entity.PartitionInfo `json:"partition_infos,omitempty"`
}

func NewMemStats(rss *sysstat.RuntimeStatSampler) *MemStats {
	if rss == nil {
		return nil
	}

	return &MemStats{
		BaseStats: BaseStats{
			Total:       rss.MemTotal.Value(),
			Free:        rss.MemFree.Value(),
			Used:        rss.MemUsed.Value(),
			UsedPercent: rss.MemUsedPercent.Value(),
		},
	}
}

type BaseStats struct {
	Total       int64   `json:"total_in_bytes,omitempty"`
	Free        int64   `json:"free_in_bytes,omitempty"`
	Used        int64   `json:"used_in_bytes,omitempty"`
	UsedPercent float64 `json:"used_percent,omitempty"`
}

type MemStats struct {
	BaseStats
}

func NewSwapStats(rss *sysstat.RuntimeStatSampler) *SwapStats {
	if rss == nil {
		return nil
	}

	return &SwapStats{
		BaseStats: BaseStats{
			Total:       rss.SwapTotal.Value(),
			Free:        rss.SwapFree.Value(),
			Used:        rss.SwapUsed.Value(),
			UsedPercent: rss.SwapUsedPercent.Value(),
		},
	}
}

type SwapStats struct {
	BaseStats
}

func NewFsStats(rss *sysstat.RuntimeStatSampler) *FsStats {
	if rss == nil {
		return nil
	}

	return &FsStats{
		Paths: rss.DiskPath,
		BaseStats: BaseStats{
			Total:       rss.DiskUsage.DiskTotal.Value(),
			Free:        rss.DiskUsage.DiskFree.Value(),
			Used:        rss.DiskUsage.DiskUsed.Value(),
			UsedPercent: float64(rss.DiskUsage.DiskUsed.Value() * 100 / (rss.DiskUsage.DiskTotal.Value() + 1)),
		},
	}
}

type FsStats struct {
	BaseStats
	Paths []string `json:"paths,omitempty"`
}

func NewCpuStats(rss *sysstat.RuntimeStatSampler) *CpuStats {
	if rss == nil {
		return nil
	}

	return &CpuStats{
		Total:         rss.CPUTotal.Value(),
		UserPercent:   rss.CPUUserPercent.Value(),
		SysPercent:    rss.CPUSysPercent.Value(),
		IoWaitPercent: rss.CPUIowaitPercent.Value(),
		IdlePercent:   rss.CPUIdlePercent.Value(),
	}
}

type CpuStats struct {
	Total         int64   `json:"total_in_bytes,omitempty"`
	UserPercent   float64 `json:"user_percent,omitempty"`
	SysPercent    float64 `json:"sys_percent,omitempty"`
	IoWaitPercent float64 `json:"io_wait_percent,omitempty"`
	IdlePercent   float64 `json:"idle_percent,omitempty"`
}

func NewNetStats(rss *sysstat.RuntimeStatSampler) *NetStats {
	if rss == nil {
		return nil
	}

	return &NetStats{
		InPerSec:  rss.NetInPerSec.Value(),
		OutPerSec: rss.NetOutPerSec.Value(),
		Connect:   rss.NetConnect.Value(),
	}
}

type NetStats struct {
	InPerSec  int64 `json:"in_pre_second,omitempty"`
	OutPerSec int64 `json:"out_pre_second,omitempty"`
	Connect   int64 `json:"connect,omitempty"`
}

func NewGCStats(rss *sysstat.RuntimeStatSampler) *GCStats {
	if rss == nil {
		return nil
	}

	return &GCStats{
		CgoCalls:       rss.CgoCalls.Value(),
		Goroutines:     rss.Goroutines.Value(),
		GcCount:        rss.GcCount.Value(),
		GCCountPercent: rss.GCCountPercent.Value(),
		GcPauseNS:      rss.GcPauseNS.Value(),
		GcPausePercent: rss.GcPausePercent.Value(),
	}
}

type GCStats struct {
	CgoCalls       int64   `json:"calls,omitempty"`
	Goroutines     int64   `json:"routines,omitempty"`
	GcCount        int64   `json:"count,omitempty"`
	GCCountPercent float64 `json:"count_percent,omitempty"`
	GcPauseNS      int64   `json:"pause_nanoseconds,omitempty"`
	GcPausePercent float64 `json:"pause_percent,omitempty"`
}
