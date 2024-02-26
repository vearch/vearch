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

package sysstat

import (
	"context"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	"github.com/shirou/gopsutil/process"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/internal/util/log"
	"github.com/vearch/vearch/internal/util/metrics"
	"github.com/vearch/vearch/internal/util/metrics/metric"
	"github.com/vearch/vearch/internal/util/routine"
)

const (
	defaultSampleInterval = 10 * time.Second

	CgoCallsMetric       = "sys_cgocalls"
	GoroutinesMetric     = "sys_goroutines"
	GCCountMetric        = "sys_gc_count"
	GCCountPercentMetric = "sys_gc_count_percent"
	GCPauseNSMetric      = "sys_gc_pause_ns"
	GCPausePercentMetric = "sys_gc_pause_percent"

	MemTotalMetric       = "sys_mem_total"
	MemUsedMetric        = "sys_mem_used"
	MemFreeMetric        = "sys_mem_free"
	MemRSSMetric         = "sys_mem_rss"
	MemUsedPercentMetric = "sys_mem_used_percent"
	GoAllocBytesMetric   = "sys_go_allocbytes"
	GoTotalBytesMetric   = "sys_go_totalbytes"

	SwapTotalMetric       = "sys_swap_total"
	SwapUsedMetric        = "sys_swap_used"
	SwapFreeMetric        = "sys_swap_free"
	SwapUsedPercentMetric = "sys_swap_used_percent"

	CPUTotalMetric         = "sys_cpu_total"
	CPUUserPercentMetric   = "sys_cpu_user_percent"
	CPUSysPercentMetric    = "sys_cpu_sys_percent"
	CPUIowaitPercentMetric = "sys_cpu_iowait_percent"
	CPUIdlePercentMetric   = "sys_cpu_idle_percent"

	DiskTotalMetric = "sys_disk_total"
	DiskUsedMetric  = "sys_disk_used"
	DiskFreeMetric  = "sys_disk_free"
	FDOpenMetric    = "sys_fd_open"

	NetInPerSecMetric  = "sys_net_in_s"
	NetOutPerSecMetric = "sys_net_out_s"
	NetConnectMetric   = "sys_net_connect"
	UptimeMetric       = "sys_uptime"
	BuildTimeMetric    = "build_timestamp"
)

var (
	// GC Meta
	metaCgoCalls       = metrics.Metadata{Name: CgoCallsMetric, Help: "Total number of cgo calls", Unit: metrics.Unit_COUNT}
	metaGoroutines     = metrics.Metadata{Name: GoroutinesMetric, Help: "Current number of goroutines", Unit: metrics.Unit_COUNT}
	metaGCCount        = metrics.Metadata{Name: GCCountMetric, Help: "Total number of GC runs", Unit: metrics.Unit_COUNT}
	metaGCCountPercent = metrics.Metadata{Name: GCCountPercentMetric, Help: "Current GC runs percentage", Unit: metrics.Unit_PERCENT}
	metaGCPauseNS      = metrics.Metadata{Name: GCPauseNSMetric, Help: "Total GC pause", Unit: metrics.Unit_NANOSECONDS}
	metaGCPausePercent = metrics.Metadata{Name: GCPausePercentMetric, Help: "Current GC pause percentage", Unit: metrics.Unit_PERCENT}
	// Memory Meta
	metaMemTotal       = metrics.Metadata{Name: MemTotalMetric, Help: "Total memory of OS", Unit: metrics.Unit_BYTES}
	metaMemUsed        = metrics.Metadata{Name: MemUsedMetric, Help: "Current used memory of OS", Unit: metrics.Unit_BYTES}
	metaMemFree        = metrics.Metadata{Name: MemFreeMetric, Help: "Current free memory of OS", Unit: metrics.Unit_BYTES}
	metaMemRSS         = metrics.Metadata{Name: MemRSSMetric, Help: "Current process RSS", Unit: metrics.Unit_BYTES}
	metaMemUsedPercent = metrics.Metadata{Name: MemUsedPercentMetric, Help: "Current memory percentage of OS", Unit: metrics.Unit_PERCENT}
	metaGoAllocBytes   = metrics.Metadata{Name: GoAllocBytesMetric, Help: "Current bytes of memory allocated by go", Unit: metrics.Unit_BYTES}
	metaGoTotalBytes   = metrics.Metadata{Name: GoTotalBytesMetric, Help: "Total bytes of memory allocated by go, but not released", Unit: metrics.Unit_BYTES}
	// Swap Meta
	metaSwapTotal       = metrics.Metadata{Name: SwapTotalMetric, Help: "Total swap of OS", Unit: metrics.Unit_BYTES}
	metaSwapUsed        = metrics.Metadata{Name: SwapUsedMetric, Help: "Current used swap of OS", Unit: metrics.Unit_BYTES}
	metaSwapFree        = metrics.Metadata{Name: SwapFreeMetric, Help: "Current free swap of OS", Unit: metrics.Unit_BYTES}
	metaSwapUsedPercent = metrics.Metadata{Name: SwapUsedPercentMetric, Help: "Current swap percentage of OS", Unit: metrics.Unit_PERCENT}
	// CPU Meta
	metaCPUTotal         = metrics.Metadata{Name: CPUTotalMetric, Help: "Total cpu of OS", Unit: metrics.Unit_COUNT}
	metaCPUUserPercent   = metrics.Metadata{Name: CPUUserPercentMetric, Help: "Current user cpu percentage", Unit: metrics.Unit_PERCENT}
	metaCPUSysPercent    = metrics.Metadata{Name: CPUSysPercentMetric, Help: "Current system cpu percentage", Unit: metrics.Unit_PERCENT}
	metaCPUIowaitPercent = metrics.Metadata{Name: CPUIowaitPercentMetric, Help: "Current iowait cpu percentage", Unit: metrics.Unit_PERCENT}
	metaCPUIdlePercent   = metrics.Metadata{Name: CPUIdlePercentMetric, Help: "Current idle cpu percentage", Unit: metrics.Unit_PERCENT}
	// Disk Meta
	metaDiskTotal = metrics.Metadata{Name: DiskTotalMetric, Help: "Total disk of dir", Unit: metrics.Unit_BYTES}
	metaDiskUsed  = metrics.Metadata{Name: DiskUsedMetric, Help: "Current used disk of dir", Unit: metrics.Unit_BYTES}
	metaDiskFree  = metrics.Metadata{Name: DiskFreeMetric, Help: "Current free disk of dir", Unit: metrics.Unit_BYTES}
	metaFDOpen    = metrics.Metadata{Name: FDOpenMetric, Help: "Process open file descriptors", Unit: metrics.Unit_COUNT}
	// Net Meta
	metaNetInPerSec  = metrics.Metadata{Name: NetInPerSecMetric, Help: "Current network incoming traffic per second", Unit: metrics.Unit_BYTES}
	metaNetOutPerSec = metrics.Metadata{Name: NetOutPerSecMetric, Help: "Current network output traffic per second", Unit: metrics.Unit_BYTES}
	metaNetConnect   = metrics.Metadata{Name: NetConnectMetric, Help: "Current number of network connections ", Unit: metrics.Unit_COUNT}
	// Run Time
	metaUptime    = metrics.Metadata{Name: UptimeMetric, Help: "Process uptime", Unit: metrics.Unit_SECONDS}
	metaBuildTime = metrics.Metadata{Name: BuildTimeMetric, Help: "Build information", Unit: metrics.Unit_SECONDS}
)

type RuntimeStatOption struct {
	Ctx        context.Context
	HeapSample bool
	Interval   time.Duration
	DiskPath   []string
	DiskQuota  uint64
}

type DiskCap struct {
	DiskTotal *metric.Gauge
	DiskUsed  *metric.Gauge
	DiskFree  *metric.Gauge
}

// RuntimeStatSampler is used to periodically sample the runtime environment for useful statistics.
type RuntimeStatSampler struct {
	RuntimeStatOption
	fdUsage    bool
	startTime  time.Time
	lastNow    time.Time
	lastNumGC  uint32
	lastNetIn  uint64
	lastNetOut uint64
	lastPtime  uint64
	lastAtime  float64
	lastUtime  float64
	lastStime  float64
	lastItime  float64
	lastIOtime float64

	// GC Metric
	CgoCalls       *metric.Gauge
	Goroutines     *metric.Gauge
	GcCount        *metric.Gauge
	GCCountPercent *metric.GaugeFloat64
	GcPauseNS      *metric.Gauge
	GcPausePercent *metric.GaugeFloat64
	// Memory Metric
	MemTotal       *metric.Gauge
	MemUsed        *metric.Gauge
	MemFree        *metric.Gauge
	MemRss         *metric.Gauge
	MemUsedPercent *metric.GaugeFloat64
	GoAllocBytes   *metric.Gauge
	GoTotalBytes   *metric.Gauge
	// Swap Metric
	SwapTotal       *metric.Gauge
	SwapUsed        *metric.Gauge
	SwapFree        *metric.Gauge
	SwapUsedPercent *metric.GaugeFloat64
	// CPU Metric
	CPUTotal         *metric.Gauge
	CPUUserPercent   *metric.GaugeFloat64
	CPUSysPercent    *metric.GaugeFloat64
	CPUIowaitPercent *metric.GaugeFloat64
	CPUIdlePercent   *metric.GaugeFloat64
	// Disk Metric
	DiskUsage DiskCap
	FDOpen    *metric.Gauge
	// Net Metric
	NetInPerSec  *metric.Gauge
	NetOutPerSec *metric.Gauge
	NetConnect   *metric.Gauge
	// Run Time
	Uptime         *metric.Gauge
	BuildTimestamp *metric.Gauge
}

// NewRuntimeStatSampler create a RuntimeStatSampler object.
func NewRuntimeStatSampler(option RuntimeStatOption) *RuntimeStatSampler {
	if option.Ctx == nil {
		option.Ctx, _ = context.WithCancel(context.Background())
	}
	if option.Interval <= 0 {
		option.Interval = defaultSampleInterval
	}

	// Build Info

	metaBuildTime.AddLabel("commit_id", config.GetCommitID())
	metaBuildTime.AddLabel("build_version", config.GetBuildVersion())

	s := &RuntimeStatSampler{
		RuntimeStatOption: option,
		startTime:         time.Now(),
		CgoCalls:          metric.NewGauge(metaCgoCalls),
		Goroutines:        metric.NewGauge(metaGoroutines),
		GcCount:           metric.NewGauge(metaGCCount),
		GCCountPercent:    metric.NewGaugeFloat64(metaGCCountPercent),
		GcPauseNS:         metric.NewGauge(metaGCPauseNS),
		GcPausePercent:    metric.NewGaugeFloat64(metaGCPausePercent),

		MemTotal:       metric.NewGauge(metaMemTotal),
		MemUsed:        metric.NewGauge(metaMemUsed),
		MemFree:        metric.NewGauge(metaMemFree),
		MemRss:         metric.NewGauge(metaMemRSS),
		MemUsedPercent: metric.NewGaugeFloat64(metaMemUsedPercent),
		GoAllocBytes:   metric.NewGauge(metaGoAllocBytes),
		GoTotalBytes:   metric.NewGauge(metaGoTotalBytes),

		SwapTotal:       metric.NewGauge(metaSwapTotal),
		SwapUsed:        metric.NewGauge(metaSwapUsed),
		SwapFree:        metric.NewGauge(metaSwapFree),
		SwapUsedPercent: metric.NewGaugeFloat64(metaSwapUsedPercent),

		CPUTotal:         metric.NewGauge(metaCPUTotal),
		CPUUserPercent:   metric.NewGaugeFloat64(metaCPUUserPercent),
		CPUSysPercent:    metric.NewGaugeFloat64(metaCPUSysPercent),
		CPUIowaitPercent: metric.NewGaugeFloat64(metaCPUIowaitPercent),
		CPUIdlePercent:   metric.NewGaugeFloat64(metaCPUIdlePercent),

		DiskUsage: DiskCap{
			DiskTotal: metric.NewGauge(metaDiskTotal),
			DiskUsed:  metric.NewGauge(metaDiskUsed),
			DiskFree:  metric.NewGauge(metaDiskFree),
		},

		FDOpen: metric.NewGauge(metaFDOpen),

		NetInPerSec:  metric.NewGauge(metaNetInPerSec),
		NetOutPerSec: metric.NewGauge(metaNetOutPerSec),
		NetConnect:   metric.NewGauge(metaNetConnect),

		Uptime:         metric.NewGauge(metaUptime),
		BuildTimestamp: metric.NewGauge(metaBuildTime),
	}

	s.CPUTotal.Update(int64(runtime.NumCPU()))
	return s
}

// StartSample queries the runtime system for various interesting metrics, storing the resulting values in the set of metric gauges .
func (s *RuntimeStatSampler) Start() error {
	return routine.RunWorkDaemon("RuntimeStat-Sampler", func() {
		timer := time.NewTimer(s.Interval)
		defer timer.Stop()

		for {
			select {
			case <-s.Ctx.Done():
				return

			case <-timer.C:
				s.MemorySample()
				s.SwapSample()
				s.CpuSample()
				s.DiskSample()
				s.NetSample()
				s.UpdateTime()
			}

			timer.Reset(s.Interval)
		}
	}, s.Ctx.Done())
}

func (s *RuntimeStatSampler) MemorySample() error {
	return routine.RunWork("RuntimeSample-Memory", func() error {
		if memStat, err := mem.VirtualMemory(); err != nil {
			log.Error("RuntimeSample get memory stats of os error: %s", err)
		} else if memStat.Total > 0 && memStat.Used > 0 {
			s.MemTotal.Update(int64(memStat.Total))
			s.MemUsed.Update(int64(memStat.Used))
			s.MemFree.Update(int64(memStat.Free))
			s.MemUsedPercent.Update(memStat.UsedPercent)
		}

		if proc, err := process.NewProcess(int32(os.Getpid())); err != nil {
			log.Error("RuntimeSample get process[%d] error: %s", os.Getpid(), err)
		} else {
			if procMem, err := proc.MemoryInfo(); err != nil {
				log.Debug("RuntimeSample get memory stats of process[%d] error: %s", os.Getpid(), err)
			} else {
				s.MemRss.Update(int64(procMem.RSS))
			}
		}

		s.CgoCalls.Update(runtime.NumCgoCall())
		s.Goroutines.Update(int64(runtime.NumGoroutine()))

		if s.HeapSample {
			// NOTE: ReadMemStats will stop the world while collecting stats.
			ms := runtime.MemStats{}
			runtime.ReadMemStats(&ms)

			s.GcCount.Update(int64(ms.NumGC))
			s.GcPauseNS.Update(int64(ms.PauseTotalNs))
			s.GoAllocBytes.Update(int64(ms.Alloc))
			s.GoTotalBytes.Update(int64(ms.Sys - ms.HeapReleased))
			if !s.lastNow.IsZero() {
				dur := time.Since(s.lastNow).Seconds()
				gcPerc := float64((ms.NumGC - s.lastNumGC)) / dur
				pausePerc := float64(ms.PauseTotalNs-s.lastPtime) / dur

				s.GCCountPercent.Update(gcPerc)
				s.GcPausePercent.Update(pausePerc)
			}
			s.lastNumGC = ms.NumGC
			s.lastPtime = ms.PauseTotalNs
		}

		return nil
	})
}

func (s *RuntimeStatSampler) SwapSample() {
	routine.RunWork("RuntimeSample-Swap", func() error {
		if swapStat, err := mem.SwapMemory(); err != nil {
			log.Debug("RuntimeSample get swap stats of os error: %s", err)
		} else {
			s.SwapTotal.Update(int64(swapStat.Total))
			s.SwapUsed.Update(int64(swapStat.Used))
			s.SwapFree.Update(int64(swapStat.Free))
			s.SwapUsedPercent.Update(swapStat.UsedPercent)
		}
		return nil
	})
}

func (s *RuntimeStatSampler) CpuSample() {
	routine.RunWork("RuntimeSample-CPU", func() error {
		if cpuStats, err := cpu.Times(false); err != nil {
			log.Debug("RuntimeSample get cpu stats of os error: %s", err)
		} else {
			cpuStat := cpuStats[0]
			allTime := cpuStat.User + cpuStat.System + cpuStat.Idle + cpuStat.Nice + cpuStat.Iowait + cpuStat.Irq + cpuStat.Softirq +
				cpuStat.Steal + cpuStat.Guest + cpuStat.GuestNice

			if !s.lastNow.IsZero() {
				total := allTime - s.lastAtime

				s.CPUUserPercent.Update((cpuStat.User - s.lastUtime) / total)
				s.CPUSysPercent.Update((cpuStat.System - s.lastStime) / total)
				s.CPUIdlePercent.Update((cpuStat.Idle - s.lastItime) / total)
				s.CPUIowaitPercent.Update((cpuStat.Iowait - s.lastIOtime) / total)
			}
			s.lastAtime = allTime
			s.lastUtime = cpuStat.User
			s.lastStime = cpuStat.System
			s.lastItime = cpuStat.Idle
			s.lastIOtime = cpuStat.Iowait
		}
		return nil
	})
}

func (s *RuntimeStatSampler) DiskSample() {
	routine.RunWork("RuntimeSample-Disk", func() error {
		for _, diskdir := range s.DiskPath {
			if diskdir == "" {
				continue
			}
			diskStat, err := disk.Usage(diskdir)
			if err != nil {
				log.Debug("RuntimeSample get disk(%s) stats error: %s", diskdir, err)
				return err
			}
			s.DiskUsage.DiskUsed.Update(int64(diskStat.Used))
			if s.DiskQuota == 0 {
				s.DiskUsage.DiskTotal.Update(int64(diskStat.Total))
				s.DiskUsage.DiskFree.Update(int64(diskStat.Free))
			} else {
				s.DiskUsage.DiskTotal.Update(int64(s.DiskQuota))
				s.DiskUsage.DiskFree.Update(int64(s.DiskQuota - diskStat.Used))
			}

			if proc, err := process.NewProcess(int32(os.Getpid())); err != nil {
				log.Debug("RuntimeSample get process[%d] error: %s", os.Getpid(), err)
			} else {
				if fdNum, err := proc.NumFDs(); err != nil {
					//log.Debug("RuntimeSample get NumFDs of process[%d] error: %s", os.Getpid(), err)
				} else {
					s.FDOpen.Update(int64(fdNum))
				}
			}
		}
		return nil
	})
}

func (s *RuntimeStatSampler) NetSample() {
	routine.RunWork("RuntimeSample-Net", func() error {
		ioStat, err := net.IOCounters(false)
		if err != nil {
			log.Debug("RuntimeSample get net stats error: %s", err)
			return err
		}
		if len(ioStat) == 0 {
			log.Debug("RuntimeSample get invalid net IO stats")
			return nil
		}
		tcpProto, err := net.ProtoCounters([]string{"tcp"})
		if err != nil {
			//log.Debug("RuntimeSample get tcp proto stats error: %s", err)
			return err
		}

		s.NetConnect.Update(tcpProto[0].Stats["CurrEstab"])
		netIoInBytes := ioStat[0].BytesRecv
		netIoOutBytes := ioStat[0].BytesSent
		if !s.lastNow.IsZero() {
			dur := time.Since(s.lastNow).Seconds()
			s.NetInPerSec.Update(int64(float64(netIoInBytes-s.lastNetIn) / dur))
			s.NetOutPerSec.Update(int64(float64(netIoOutBytes-s.lastNetOut) / dur))
		}
		s.lastNetIn = netIoInBytes
		s.lastNetOut = netIoOutBytes

		return nil
	})
}

func (s *RuntimeStatSampler) UpdateTime() {
	s.lastNow = time.Now()
	s.Uptime.Update(int64(s.lastNow.Sub(s.startTime).Seconds()))
}
