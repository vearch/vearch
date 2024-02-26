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
	"math"
	"sync/atomic"
	"time"

	"github.com/vearch/vearch/util/metrics"
)

var _ metrics.Metric = &GaugeFloat64{}

// A GaugeFloat64 atomically stores a single float64 value.
type GaugeFloat64 struct {
	metrics.Metadata
	value uint64
}

// NewGaugeFloat64 creates a GaugeFloat64.
func NewGaugeFloat64(metadata metrics.Metadata) *GaugeFloat64 {
	return &GaugeFloat64{Metadata: metadata}
}

// GetMetadata return the metadata of Gauge.
func (g *GaugeFloat64) GetMetadata() metrics.Metadata {
	return g.Metadata
}

// GetType returns the type enum for this metric.
func (g *GaugeFloat64) GetType() metrics.MetricType {
	return metrics.MetricType_GAUGE
}

// Inspect calls the closure with the empty string and the receiver.
func (g *GaugeFloat64) Inspect(f func(metrics.Metric)) {
	f(g)
}

// Update updates the gauge's value.
func (g *GaugeFloat64) Update(v float64) {
	atomic.StoreUint64(&g.value, math.Float64bits(v))
}

// Value returns the gauge's current value.
func (g *GaugeFloat64) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.value))
}

// Snapshot returns a read-only copy of the gauge.
func (g *GaugeFloat64) Snapshot() *GaugeFloat64 {
	gg := NewGaugeFloat64(g.Metadata)
	gg.value = atomic.LoadUint64(&g.value)
	return gg
}

// ExportMetric returns a filled-in metric data of the right type for the given metric.
func (g *GaugeFloat64) ExportMetric() *metrics.MetricData {
	return &metrics.MetricData{
		Gauge:       &metrics.GaugeData{Value: g.Value()},
		Labels:      g.Labels[:],
		Unit:        g.Unit,
		TimestampNs: time.Now().UnixNano(),
	}
}
