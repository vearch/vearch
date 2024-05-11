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

package export

import (
	"testing"

	"github.com/vearch/vearch/v3/internal/pkg/bufalloc"
	"github.com/vearch/vearch/v3/internal/pkg/metrics"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/metric"
)

func TestMemoryExporter(t *testing.T) {
	r1, r2 := metrics.NewRegistry(), metrics.NewRegistry()
	r2.AddLabel("registry", "two")

	r1.AddMetric(metric.NewGauge(metrics.Metadata{Name: "one.gauge"}))
	r2.AddMetric(metric.NewGauge(metrics.Metadata{Name: "two.gauge"}))

	c1Meta := metrics.Metadata{Name: "shared.counter"}
	c1Meta.AddLabel("counter", "one")
	c2Meta := metrics.Metadata{Name: "shared.counter"}
	c2Meta.AddLabel("counter", "two")
	r1.AddMetric(metric.NewCounter(c1Meta))
	r2.AddMetric(metric.NewCounter(c2Meta))

	exp := NewMemoryExporter()
	exp.DumpRegistry([]*metrics.Registry{r1, r2})

	type metricLabels map[string]string
	type family struct {
		metrics []metricLabels
	}
	expected := map[string]family{
		"one_gauge": {[]metricLabels{
			{},
		}},
		"two_gauge": {[]metricLabels{
			{"registry": "two"},
		}},
		"shared_counter": {[]metricLabels{
			{"counter": "one"},
			{"counter": "two", "registry": "two"},
		}},
	}

	if lenExpected, lenExporter := len(expected), len(exp.metrics); lenExpected != lenExporter {
		t.Errorf("wrong number of families, expected %d, got %d", lenExpected, lenExporter)
	}

	for name, fam := range expected {
		var (
			ok   bool
			fam2 *metrics.MetricFamily
		)
		for _, f := range exp.metrics {
			if f.Name == name {
				ok = true
				fam2 = f
				break
			}
		}

		if !ok {
			t.Errorf("exporter does not have metric family named %s", name)
		}
		if lenExpected, lenExporter := len(fam.metrics), len(fam2.Metrics); lenExpected != lenExporter {
			t.Errorf("wrong number of metrics for family %s, expected %d, got %d", name, lenExpected, lenExporter)
		}
		for i, m := range fam2.Metrics {
			expectedLabels := fam.metrics[i]
			if lenExpected, lenExporter := len(expectedLabels), len(m.Labels); lenExpected != lenExporter {
				t.Errorf("wrong number of labels for metric %d in family %s, expected %d, got %d",
					i, name, lenExpected, lenExporter)
			}
			for _, l := range m.Labels {
				if val, ok := expectedLabels[l.Name]; !ok {
					t.Errorf("unexpected label name %s for metric %d in family %s", l.Name, i, name)
				} else if val != l.Value {
					t.Errorf("label %s for metric %d in family %s has value %s, expected %s", l.Name, i, name, l.Value, val)
				}
			}
		}
	}

	buf := bufalloc.AllocBuffer(1024)
	exp.PrintText(buf)
	t.Log(buf.String())
	bufalloc.FreeBuffer(buf)
}
