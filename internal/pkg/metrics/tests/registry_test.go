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
	"testing"
	"time"

	"github.com/vearch/vearch/internal/pkg/metrics"
	"github.com/vearch/vearch/internal/pkg/metrics/metric"
)

func getCounter(r *metrics.Registry, name string) *metric.Counter {
	m := r.FindMetric(name)
	if m == nil {
		return nil
	}

	counter, ok := m.(*metric.Counter)
	if !ok {
		return nil
	}
	return counter
}

func getGauge(r *metrics.Registry, name string) *metric.Gauge {
	m := r.FindMetric(name)
	if m == nil {
		return nil
	}

	gauge, ok := m.(*metric.Gauge)
	if !ok {
		return nil
	}
	return gauge
}

func TestRegistry(t *testing.T) {
	r := metrics.NewRegistry()

	gauge := metric.NewGauge(metrics.Metadata{Name: "test.gauge"})
	r.AddMetric(gauge)

	counter := metric.NewCounter(metrics.Metadata{Name: "test.counter"})
	r.AddMetric(counter)

	hist := metric.NewHistogram(metrics.Metadata{Name: "test.histogram"}, time.Minute, 1000, 3)
	r.AddMetric(hist)

	ms := &struct {
		StructGauge     *metric.Gauge
		StructGauge64   *metric.GaugeFloat64
		StructCounter   *metric.Counter
		StructHistogram *metric.Histogram

		privateStructGauge   *metric.Gauge
		privateStructGauge64 *metric.GaugeFloat64
		NotAMetric           int
		NotBMetric           string
	}{
		StructGauge:          metric.NewGauge(metrics.Metadata{Name: "struct.gauge"}),
		StructGauge64:        metric.NewGaugeFloat64(metrics.Metadata{Name: "struct.gauge64"}),
		StructCounter:        metric.NewCounter(metrics.Metadata{Name: "struct.counter"}),
		StructHistogram:      metric.NewHistogram(metrics.Metadata{Name: "struct.histogram"}, time.Minute, 1000, 3),
		privateStructGauge:   metric.NewGauge(metrics.Metadata{Name: "struct.private-gauge"}),
		privateStructGauge64: metric.NewGaugeFloat64(metrics.Metadata{Name: "struct.private-gauge64"}),
		NotAMetric:           0,
		NotBMetric:           "NotBMetric",
	}
	r.AddMetricStruct(ms)

	expNames := map[string]struct{}{
		"test.gauge":     {},
		"test.counter":   {},
		"test.histogram": {},

		"struct.gauge":     {},
		"struct.gauge64":   {},
		"struct.counter":   {},
		"struct.histogram": {},
	}

	r.Each(func(_ string, val metrics.Metric) {
		name := val.GetMetadata().Name
		if _, exist := expNames[name]; !exist {
			t.Errorf("unexpected name: %s", name)
		}
		delete(expNames, name)
	})
	if len(expNames) > 0 {
		t.Fatalf("missed names: %v", expNames)
	}

	if g := getGauge(r, "test.gauge"); g != gauge {
		t.Errorf("getGauge returned %v, expected %v", g, gauge)
	}
	if g := getGauge(r, "bad"); g != nil {
		t.Errorf("getGauge returned non-nil %v, expected nil", g)
	}
	if g := getGauge(r, "test.histogram"); g != nil {
		t.Errorf("getGauge returned non-nil %v of type %T when requesting non-gauge, expected nil", g, g)
	}

	if c := getCounter(r, "test.counter"); c != counter {
		t.Errorf("getCounter returned %v, expected %v", c, counter)
	}
	if c := getCounter(r, "bad"); c != nil {
		t.Errorf("getCounter returned non-nil %v, expected nil", c)
	}
	if c := getCounter(r, "test.histogram"); c != nil {
		t.Errorf("getCounter returned non-nil %v of type %T when requesting non-counter, expected nil", c, c)
	}
}
