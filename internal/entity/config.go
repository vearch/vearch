package entity

import (
	"os"

	vearch_os "github.com/vearch/vearch/v3/internal/pkg/runtime/os"
	"github.com/vearch/vearch/v3/internal/router/document/gctuner"
	"golang.org/x/time/rate"
)

const (
	RequestLimitConfigKey  = "request_limit_config"
	MemoryLimitConfigKey   = "memory_limit_config"
	SlowSearchIsolationKey = "slow_search_isolation"
)

type SlowSearchIsolationCfg struct {
	SlowSearchIsolationEnabled bool `json:"slow_search_isolation_enabled"`
}

type RequestLimitCfg struct {
	RequestLimitEnabled bool    `json:"request_limit_enabled"`
	TotalReadLimit      float64 `json:"read_request_limit_count,omitempty"`
	TotalWriteLimit     float64 `json:"write_request_limit_count,omitempty"`
}

type MemoryLimitCfg struct {
	MemoryLimitEnabled bool `json:"memory_limit_enabled"`
	RouterMemoryLimit  int  `json:"router_memory_limit"`
	PsMemoryLimit      int  `json:"ps_memory_limit"`
}

type Config struct {
	RouterCount        float64
	RequestLimitConfig *RequestLimitCfg
	MemoryLimitConfig  *MemoryLimitCfg
}

const (
	DefaultReadRequestLimitCount    = 1000000.0
	DefaultWriteRequestLimitCount   = 1000000.0
	DefaultRouterMemoryLimitPercent = 90
	DefaultPsMemoryLimitPercent     = 90
)

var (
	HostIp   = os.Getenv("VEARCH_HOST_IP")
	HostRack = os.Getenv("VEARCH_HOST_RACK")
	HostZone = os.Getenv("VEARCH_HOST_ZONE")

	ReadLimiter  = rate.NewLimiter(rate.Limit(rate.Inf), 0)
	WriteLimiter = rate.NewLimiter(rate.Limit(rate.Inf), 0)

	SlowSearchIsolationEnabled = true

	MemoryLimitUsage uint64
	SystemMemory, _  = vearch_os.GetSystemMemory()

	check_count = 0
)

var ConfigInfo = &Config{
	RouterCount:        0.0,
	RequestLimitConfig: &RequestLimitCfg{},
	MemoryLimitConfig:  &MemoryLimitCfg{},
}

func SetRequestLimit(requestLimit *RequestLimitCfg) {
	if requestLimit.RequestLimitEnabled {
		ConfigInfo.RequestLimitConfig.RequestLimitEnabled = true

		if requestLimit.TotalReadLimit > 0 {
			ConfigInfo.RequestLimitConfig.TotalReadLimit = requestLimit.TotalReadLimit
		} else {
			ConfigInfo.RequestLimitConfig.TotalReadLimit = DefaultReadRequestLimitCount
		}

		if requestLimit.TotalWriteLimit > 0 {
			ConfigInfo.RequestLimitConfig.TotalWriteLimit = requestLimit.TotalWriteLimit

		} else {
			ConfigInfo.RequestLimitConfig.TotalWriteLimit = DefaultWriteRequestLimitCount
		}

		if ConfigInfo.RouterCount > 0 {
			var limit rate.Limit
			limit = rate.Limit(ConfigInfo.RequestLimitConfig.TotalReadLimit / ConfigInfo.RouterCount)
			ReadLimiter.SetLimit(limit)
			ReadLimiter.SetBurst(int(limit * 1.1))

			limit = rate.Limit(ConfigInfo.RequestLimitConfig.TotalWriteLimit / ConfigInfo.RouterCount)
			WriteLimiter.SetLimit(limit)
			WriteLimiter.SetBurst(int(limit * 1.1))
		}
	} else {
		ConfigInfo.RequestLimitConfig.RequestLimitEnabled = false

		ReadLimiter.SetLimit(rate.Inf)
		ReadLimiter.SetBurst(0)

		WriteLimiter.SetLimit(rate.Inf)
		WriteLimiter.SetBurst(0)
	}
}

func SetRouterCount(add bool) {
	if add {
		ConfigInfo.RouterCount++
	} else {
		ConfigInfo.RouterCount--
	}

	if ConfigInfo.RequestLimitConfig.RequestLimitEnabled && ConfigInfo.RouterCount > 0 {
		limit := rate.Limit(ConfigInfo.RequestLimitConfig.TotalReadLimit / ConfigInfo.RouterCount)
		ReadLimiter.SetLimit(limit)
		ReadLimiter.SetBurst(int(limit * 1.1))

		limit = rate.Limit(ConfigInfo.RequestLimitConfig.TotalWriteLimit / ConfigInfo.RouterCount)
		WriteLimiter.SetLimit(limit)
		WriteLimiter.SetBurst(int(limit * 1.1))
	}
}

func SetMemoryLimit(memLimit *MemoryLimitCfg, router bool) {
	if memLimit.MemoryLimitEnabled {
		ConfigInfo.MemoryLimitConfig.MemoryLimitEnabled = true

		if memLimit.RouterMemoryLimit > 0 {
			ConfigInfo.MemoryLimitConfig.RouterMemoryLimit = memLimit.RouterMemoryLimit
		} else {
			ConfigInfo.MemoryLimitConfig.RouterMemoryLimit = DefaultRouterMemoryLimitPercent
		}
		if router {
			MemoryLimitUsage = SystemMemory * uint64(ConfigInfo.MemoryLimitConfig.RouterMemoryLimit) / 100
			gctuner.GlobalMemoryLimitTuner.SetTotalMemory(SystemMemory)
			gctuner.GlobalMemoryLimitTuner.SetPercentage(float64(ConfigInfo.MemoryLimitConfig.RouterMemoryLimit))
			gctuner.GlobalMemoryLimitTuner.EnableAdjustMemoryLimit()
		}

		if memLimit.PsMemoryLimit > 0 {
			ConfigInfo.MemoryLimitConfig.PsMemoryLimit = memLimit.PsMemoryLimit
		} else {
			ConfigInfo.MemoryLimitConfig.PsMemoryLimit = DefaultPsMemoryLimitPercent
		}

		if !router {
			MemoryLimitUsage = SystemMemory * uint64(ConfigInfo.MemoryLimitConfig.PsMemoryLimit) / 100
		}
	} else {
		ConfigInfo.MemoryLimitConfig.MemoryLimitEnabled = false
		MemoryLimitUsage = 0
		if router {
			gctuner.GlobalMemoryLimitTuner.DisableAdjustMemoryLimit()
		}
	}
}

func CheckVirtualMemExceed(estimatedSize int) bool {
	if MemoryLimitUsage == 0 {
		return false
	}

	if estimatedSize > 0 {
		check_count = (check_count + 1) % 10
		if check_count != 0 {
			return false
		}
	}

	VirtualMemUsage, _ := vearch_os.GetMemoryUsage()
	if VirtualMemUsage != 0 && VirtualMemUsage > MemoryLimitUsage {
		return true
	}

	return false
}

func SetSlowSearchIsolation(cfg *SlowSearchIsolationCfg) {
	if cfg.SlowSearchIsolationEnabled {
		SlowSearchIsolationEnabled = true
	} else {
		SlowSearchIsolationEnabled = false
	}
}
