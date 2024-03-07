// Copyright 2014 beego Author. All Rights Reserved.
// Modified work copyright (C) 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package gogc

import (
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"time"
)

var startTime = time.Now()
var lastgcpercent float64 = 100

// PrintGCSummary print gc information to io.Writer
func PrintGCSummary(w io.Writer) {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	gcstats := &debug.GCStats{PauseQuantiles: make([]time.Duration, 100)}
	debug.ReadGCStats(gcstats)

	printGC(memStats, gcstats, w)
}

func printGC(memStats *runtime.MemStats, gcstats *debug.GCStats, w io.Writer) {
	//gcMemory := float64(524288000) // 500MB
	if gcstats.NumGC > 0 {
		lastPause := gcstats.Pause[0]
		elapsed := time.Now().Sub(startTime)
		overhead := float64(gcstats.PauseTotal) / float64(elapsed) * 100
		allocatedRate := float64(memStats.TotalAlloc) / elapsed.Seconds()

		fmt.Fprintf(w, "NumGC:%d Pause:%s Pause(Avg):%s Overhead:%3.2f%% Alloc:%s Sys:%s Alloc(Rate):%s/s HeapObjects:%d,HeapInuse:%s,HeapReleased:%s,HeapSys:%s,HeapAlloc:%s, Histogram:%s %s %s num goroutine %d\n",
			gcstats.NumGC,
			toS(lastPause),
			toS(avg(gcstats.Pause)),
			overhead,
			toH(memStats.Alloc),
			toH(memStats.Sys),
			toH(uint64(allocatedRate)),
			memStats.HeapObjects,
			toH(memStats.HeapInuse),
			toH(memStats.HeapReleased),
			toH(memStats.HeapSys),
			toH(memStats.HeapAlloc),
			toS(gcstats.PauseQuantiles[94]),
			toS(gcstats.PauseQuantiles[98]),
			toS(gcstats.PauseQuantiles[99]),
			runtime.NumGoroutine())
		//==========================================================================
		//auto set gc percent
		//curGcMemory := float64(memStats.HeapInuse) * lastgcpercent / 100
		//if float64(memStats.HeapInuse) > gcMemory*4 {
		//
		//	if float64(memStats.HeapInuse)*lastgcpercent/100 > gcMemory*2 {
		//		gcpercent := gcMemory / float64(memStats.HeapInuse) * 100
		//		lastgcpercent = gcpercent
		//		old := debug.SetGCPercent(int(gcpercent))
		//		fmt.Fprintf(w, "[force gc] old percent:%v,cur percent:%v ", old, gcpercent)
		//	}
		//
		//} else if curGcMemory < gcMemory {
		//	var gcpercent float64
		//	gcpercent = 100
		//	if lastgcpercent < gcpercent {
		//		lastgcpercent = gcpercent
		//		old := debug.SetGCPercent(int(gcpercent))
		//		fmt.Fprintf(w, "[force gc] old percent:%v,cur percent:%v ", old, gcpercent)
		//	}
		//}
		//==========================================================================
	} else {
		// while GC has disabled
		elapsed := time.Now().Sub(startTime)
		allocatedRate := float64(memStats.TotalAlloc) / elapsed.Seconds()

		fmt.Fprintf(w, "Alloc:%s Sys:%s Alloc(Rate):%s/s\n",
			toH(memStats.Alloc),
			toH(memStats.Sys),
			toH(uint64(allocatedRate)))
	}
}

func avg(items []time.Duration) time.Duration {
	var sum time.Duration
	for _, item := range items {
		sum += item
	}
	return time.Duration(int64(sum) / int64(len(items)))
}

// format bytes number friendly
func toH(bytes uint64) string {
	switch {
	case bytes < 1024:
		return fmt.Sprintf("%dB", bytes)
	case bytes < 1024*1024:
		return fmt.Sprintf("%.2fK", float64(bytes)/1024)
	case bytes < 1024*1024*1024:
		return fmt.Sprintf("%.2fM", float64(bytes)/1024/1024)
	default:
		return fmt.Sprintf("%.2fG", float64(bytes)/1024/1024/1024)
	}
}

// short string format
func toS(d time.Duration) string {

	u := uint64(d)
	if u < uint64(time.Second) {
		switch {
		case u == 0:
			return "0"
		case u < uint64(time.Microsecond):
			return fmt.Sprintf("%.2fns", float64(u))
		case u < uint64(time.Millisecond):
			return fmt.Sprintf("%.2fus", float64(u)/1000)
		default:
			return fmt.Sprintf("%.2fms", float64(u)/1000/1000)
		}
	} else {
		switch {
		case u < uint64(time.Minute):
			return fmt.Sprintf("%.2fs", float64(u)/1000/1000/1000)
		case u < uint64(time.Hour):
			return fmt.Sprintf("%.2fm", float64(u)/1000/1000/1000/60)
		default:
			return fmt.Sprintf("%.2fh", float64(u)/1000/1000/1000/60/60)
		}
	}

}
