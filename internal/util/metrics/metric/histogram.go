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

	"github.com/codahale/hdrhistogram"
	"github.com/vearch/vearch/internal/util/metrics"
)

// histogramLatency is the maximum value tracked in latency histograms.
const histogramLatency = 10 * time.Second

var histogramPercentile = []metrics.Percentile{
	{Name: "-max", Unit: 100, Value: 0},
	{Name: "-p99.999", Unit: 99.999, Value: 0},
	{Name: "-p99.99", Unit: 99.99, Value: 0},
	{Name: "-p99.9", Unit: 99.9, Value: 0},
	{Name: "-p99", Unit: 99, Value: 0},
}

var _ metrics.Metric = &Histogram{}

// Histogram collects observed values by keeping bucketed counts.
// There are two sets of buckets: A cumulative set and a windowed set.
// Recording a value in excess of the maxVal results in recording the maxVal instead.
//
// Used primarily to record size or number of events occurring within a specified distribution range (Buckets).
type Histogram struct {
	metrics.Metadata
	maxVal int64

	sync.Mutex
	cumulative *hdrhistogram.Histogram
	window     *slidingHistogram
}

// NewHistogram initializes a given Histogram.
// The window histogram rotates every 'duration'; both the windowed and the cumulative histogram
// track nonnegative values up to 'maxVal' with 'sigFigs' decimal points of precision.
func NewHistogram(metadata metrics.Metadata, duration time.Duration, maxVal int64, sigFigs int) *Histogram {
	h := &Histogram{
		Metadata: metadata,
		maxVal:   maxVal,
		window:   newSlidingHistogram(duration, maxVal, sigFigs),
	}
	h.cumulative = hdrhistogram.New(0, maxVal, sigFigs)
	return h
}

// NewHistogramLatency is a convenience function which returns a histogram with suitable defaults for latency tracking.
func NewHistogramLatency(metadata metrics.Metadata, duration time.Duration) *Histogram {
	return NewHistogram(metadata, duration, histogramLatency.Nanoseconds(), 1)
}

// GetMetadata return the metadata of Histogram
func (h *Histogram) GetMetadata() metrics.Metadata {
	return h.Metadata
}

// GetType returns the type enum for this metric.
func (h *Histogram) GetType() metrics.MetricType {
	return metrics.MetricType_HISTOGRAM
}

// Inspect calls the closure with the empty string and the receiver.
func (h *Histogram) Inspect(f func(metrics.Metric)) {
	h.Lock()
	maybeTick(h.window)
	f(h)
	h.Unlock()
}

// ExportMetric returns a filled-in metric data of the right type for the given metric.
func (h *Histogram) ExportMetric() *metrics.MetricData {
	h.Lock()
	maybeTick(h.window)
	labels := h.Labels[:]
	bars := h.cumulative.Distribution()
	window := cloneHistogram(h.window.current())
	h.Unlock()

	hist := &metrics.HistogramData{Buckets: make([]metrics.Bucket, 0, len(bars))}
	for _, bar := range bars {
		if bar.Count == 0 {
			continue
		}

		upperBound := float64(bar.To)
		hist.SampleSum += upperBound * float64(bar.Count)
		hist.SampleCount += uint64(bar.Count)
		hist.Buckets = append(hist.Buckets, metrics.Bucket{
			CumulativeCount: hist.SampleCount,
			UpperBound:      upperBound,
		})
	}

	for _, pt := range histogramPercentile {
		hist.Pts = append(hist.Pts, metrics.Percentile{Name: pt.Name, Unit: pt.Unit, Value: float64(window.ValueAtQuantile(pt.Unit))})
	}

	return &metrics.MetricData{
		Histogram:   hist,
		Labels:      labels,
		Unit:        h.Unit,
		TimestampNs: time.Now().UnixNano(),
	}
}

// Windowed returns a copy of the current windowed histogram data and its rotation interval.
func (h *Histogram) Windowed() (hist *hdrhistogram.Histogram, duration time.Duration) {
	h.Lock()
	hist = cloneHistogram(h.window.current())
	duration = h.window.duration
	h.Unlock()
	return
}

// Snapshot returns a copy of the cumulative histogram data.
func (h *Histogram) Snapshot() (hist *hdrhistogram.Histogram) {
	h.Lock()
	hist = cloneHistogram(h.cumulative)
	h.Unlock()
	return
}

// RecordValue adds the given value to the histogram.
func (h *Histogram) RecordValue(v int64) {
	h.Lock()
	if h.window.recordValue(v) != nil {
		h.window.recordValue(h.maxVal)
	}
	if h.cumulative.RecordValue(v) != nil {
		h.cumulative.RecordValue(h.maxVal)
	}
	h.Unlock()
}

// TotalCount returns the (cumulative) number of samples.
func (h *Histogram) TotalCount() (count int64) {
	h.Lock()
	count = h.cumulative.TotalCount()
	h.Unlock()
	return
}

// Min returns the minimum.
func (h *Histogram) Min() (min int64) {
	h.Lock()
	min = h.cumulative.Min()
	h.Unlock()
	return
}

func cloneHistogram(in *hdrhistogram.Histogram) *hdrhistogram.Histogram {
	return hdrhistogram.Import(in.Export())
}
