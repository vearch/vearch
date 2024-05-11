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

	"github.com/vearch/vearch/v3/internal/pkg/metrics"
)

var _ metrics.Metric = &Counter{}

// A Counter holds a single mutable atomic value.
//
// Can be used to record indicator types that will only increase and not decrease.
type Counter struct {
	metrics.Metadata
	count int64
}

// NewCounter creates a counter.
func NewCounter(metadata metrics.Metadata) *Counter {
	return &Counter{Metadata: metadata, count: 0}
}

// GetMetadata return the metadata of Counter.
func (c *Counter) GetMetadata() metrics.Metadata {
	return c.Metadata
}

// GetType returns the type enum for this metric.
func (c *Counter) GetType() metrics.MetricType {
	return metrics.MetricType_COUNTER
}

// Inspect calls the closure with the empty string and the receiver.
func (c *Counter) Inspect(f func(metrics.Metric)) {
	f(c)
}

// Clear sets the counter to zero.
func (c *Counter) Clear() {
	atomic.StoreInt64(&c.count, 0)
}

// Count returns the current count.
func (c *Counter) Count() int64 {
	return atomic.LoadInt64(&c.count)
}

// Inc increments the counter by the given amount.
func (c *Counter) Inc(i int64) {
	atomic.AddInt64(&c.count, i)
}

func (c *Counter) Snapshot() *Counter {
	cc := NewCounter(c.Metadata)
	cc.count = c.Count()
	return cc
}

// ExportMetric returns a filled-in metric data of the right type for the given metric.
func (c *Counter) ExportMetric() *metrics.MetricData {
	return &metrics.MetricData{
		Counter:     &metrics.CounterData{Value: float64(c.Count())},
		Labels:      c.Labels[:],
		Unit:        c.Unit,
		TimestampNs: time.Now().UnixNano(),
	}
}
