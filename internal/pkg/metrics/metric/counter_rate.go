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

package metric

import (
	"sync"
	"time"

	"github.com/vearch/vearch/v3/internal/pkg/metrics"
)

var _ metrics.Metric = &CounterRate{}

// CounterRate is a wrapper around a counter.
type CounterRate struct {
	Counter
	mu        sync.Mutex
	lastCount int64
	lastTime  time.Time
}

// NewCounterRate creates a CounterRate.
func NewCounterRate(metadata metrics.Metadata) *CounterRate {
	return &CounterRate{
		Counter:  Counter{Metadata: metadata, count: 0},
		lastTime: time.Now(),
	}
}

// GetType returns the type enum for this metric.
func (c *CounterRate) GetType() metrics.MetricType {
	return metrics.MetricType_COUNTERRATE
}

// Clear sets the counter to zero.
func (c *CounterRate) Clear() {
	c.mu.Lock()
	c.lastCount = 0
	c.lastTime = time.Now()
	c.Counter.Clear()
	c.mu.Unlock()
}

// ExportMetric returns a filled-in metric data of the right type for the given metric.
func (c *CounterRate) ExportMetric() *metrics.MetricData {
	c.mu.Lock()
	cur := c.Counter.Count()
	win := cur - c.lastCount
	now := time.Now()
	avg := float64(0)
	if c.lastTime.Before(now) {
		avg = float64(win) / now.Sub(c.lastTime).Seconds()
	}
	c.lastCount = cur
	c.lastTime = now
	c.mu.Unlock()

	return &metrics.MetricData{
		CounterRate: &metrics.CounterRateData{TotalValue: float64(cur), WindowValue: float64(win), AvgValue: float64(avg)},
		Labels:      c.Labels[:],
		Unit:        c.Unit,
		TimestampNs: time.Now().UnixNano(),
	}
}
