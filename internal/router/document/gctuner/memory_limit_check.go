package gctuner

import (
	"time"
)

var exitCh chan struct{}

func Run() {
	tickInterval := time.Second * time.Duration(1)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			CheckMemoryLimitExceed(0)
		case <-exitCh:
			return
		}
	}
}

func CheckMemoryLimitExceed(estimatedSize uint64) bool {
	MemUsed := readMemoryInuse()
	memoryLimitUsage := GlobalMemoryLimitTuner.serverMemLimitUsage.Load()

	if memoryLimitUsage > 0 && MemUsed+estimatedSize > memoryLimitUsage {
		return true
	}
	return false
}
