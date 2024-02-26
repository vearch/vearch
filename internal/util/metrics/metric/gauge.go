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
	"sync/atomic"
	"time"

	"github.com/vearch/vearch/internal/util/metrics"
)

var _ metrics.Metric = &Gauge{}

// Gauge atomically stores a single integer value.
//
// An incremental and subtractable dashboard
type Gauge struct {
	metrics.Metadata
	value int64
}

// NewGauge creates a Gauge.
func NewGauge(metadata metrics.Metadata) *Gauge {
	return &Gauge{Metadata: metadata}
}

// GetMetadata return the metadata of Gauge.
func (g *Gauge) GetMetadata() metrics.Metadata {
	return g.Metadata
}

// GetType returns the type enum for this metric.
func (g *Gauge) GetType() metrics.MetricType {
	return metrics.MetricType_GAUGE
}

// Inspect calls the closure with the empty string and the receiver.
func (g *Gauge) Inspect(f func(metrics.Metric)) {
	f(g)
}

// Update updates the gauge's value.
func (g *Gauge) Update(v int64) {
	atomic.StoreInt64(&g.value, v)
}

// Inc increments the gauge's value.
func (g *Gauge) Inc(i int64) {
	atomic.AddInt64(&g.value, i)
}

// Dec decrements the gauge's value.
func (g *Gauge) Dec(i int64) {
	atomic.AddInt64(&g.value, -i)
}

// Value returns the gauge's current value.
func (g *Gauge) Value() int64 {
	return atomic.LoadInt64(&g.value)
}

// Snapshot returns a read-only copy of the gauge.
func (g *Gauge) Snapshot() *Gauge {
	gg := NewGauge(g.Metadata)
	gg.value = g.Value()
	return gg
}

// ExportMetric returns a filled-in metric data of the right type for the given metric.
func (g *Gauge) ExportMetric() *metrics.MetricData {
	return &metrics.MetricData{
		Gauge:       &metrics.GaugeData{Value: float64(g.Value())},
		Labels:      g.Labels[:],
		Unit:        g.Unit,
		TimestampNs: time.Now().UnixNano(),
	}
}
