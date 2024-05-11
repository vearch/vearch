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

package tests

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vearch/vearch/v3/internal/pkg/metrics"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/metric"
)

var emptyMetadata = metrics.Metadata{}

func TestCounter(t *testing.T) {
	c := metric.NewCounter(emptyMetadata)
	c.Inc(90)
	if v := c.Count(); v != 90 {
		t.Fatalf("unexpected value: %d", v)
	}
}

func TestGauge(t *testing.T) {
	g := metric.NewGauge(emptyMetadata)
	g.Update(10)
	if v := g.Value(); v != 10 {
		t.Fatalf("unexpected value: %d", v)
	}
	var wg sync.WaitGroup
	for i := int64(0); i < 10; i++ {
		wg.Add(2)
		go func(i int64) { g.Inc(i); wg.Done() }(i)
		go func(i int64) { g.Dec(i); wg.Done() }(i)
	}
	wg.Wait()
	if v := g.Value(); v != 10 {
		t.Fatalf("unexpected value: %d", v)
	}
}

func TestGaugeFloat64(t *testing.T) {
	g := metric.NewGaugeFloat64(emptyMetadata)
	g.Update(10.55)
	if v := g.Value(); v != 10.55 {
		t.Fatalf("unexpected value: %f", v)
	}
}

func TestHistogram(t *testing.T) {
	h := metric.NewHistogram(emptyMetadata, time.Hour, 10, 1)
	h.RecordValue(1)
	h.RecordValue(5)
	h.RecordValue(5)
	h.RecordValue(10)
	h.RecordValue(15000)

	act := h.ExportMetric().Histogram
	exp := &metrics.HistogramData{
		SampleCount: uint64(5),
		SampleSum:   float64(1*1 + 2*5 + 2*10),
		Buckets: []metrics.Bucket{
			{CumulativeCount: uint64(1), UpperBound: float64(1)},
			{CumulativeCount: uint64(3), UpperBound: float64(5)},
			{CumulativeCount: uint64(5), UpperBound: float64(10)},
		},
	}

	if !reflect.DeepEqual(act, exp) {
		t.Fatalf("Histogram returned %v, expected %v", act, exp)
	}
}

func TestHistogramRotate(t *testing.T) {
	duration := 2 * time.Second
	h := metric.NewHistogram(emptyMetadata, duration, 1000+10*2, 3)

	for i := 0; i < 3*2; i++ {
		v := int64(10 * i)
		h.RecordValue(v)
		time.Sleep(time.Second)

		cur, windowDuration := h.Windowed()
		if windowDuration != duration {
			t.Fatalf("window changed: is %s, should be %s", windowDuration, duration)
		}

		expMin := int64((1 + i - (2 - 1)) * 10)
		if expMin < 0 {
			expMin = 0
		}

		if min := cur.Min(); min != expMin {
			t.Fatalf("%d: unexpected minimum %d, expected %d", i, min, expMin)
		}
		if max, expMax := cur.Max(), v; max != expMax {
			t.Fatalf("%d: unexpected maximum %d, expected %d", i, max, expMax)
		}
	}
}
