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
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/vearch/vearch/util/metrics"
)

var _ metrics.Exporter = &MemoryExporter{}

type MemoryExporter struct {
	sync.RWMutex
	metrics map[string]*metrics.MetricFamily
}

// NewMemoryExporter create a MemoryExporter object.
func NewMemoryExporter() *MemoryExporter {
	return new(MemoryExporter)
}

func (e *MemoryExporter) DumpRegistry(registrys []*metrics.Registry) {
	familys := make(map[string]*metrics.MetricFamily)

	for _, r := range registrys {
		labels := r.GetLabels()
		r.Each(func(name string, v metrics.Metric) {
			m := v.ExportMetric()
			m.Labels = append(labels, m.Labels...)
			family, ok := familys[name]
			if !ok {
				family = &metrics.MetricFamily{
					Name: name,
					Help: v.GetMetadata().Help,
					Type: v.GetType(),
				}
				familys[name] = family
			}
			family.Metrics = append(family.Metrics, *m)
		})
	}

	e.Lock()
	e.metrics = familys
	e.Unlock()
}

func (e *MemoryExporter) GetMetric() (metrics map[string]*metrics.MetricFamily) {
	e.RLock()
	metrics = e.metrics
	e.RUnlock()
	return
}

// PrintText converts metrics into text format and writes the resulting lines to 'out'.
func (e *MemoryExporter) PrintText(out io.Writer) error {
	e.RLock()
	familys := make(metrics.MetricFamilySlice, 0, len(e.metrics))
	for _, f := range e.metrics {
		familys = append(familys, f)
	}
	e.RUnlock()

	sort.Sort(familys)
	for _, family := range familys {
		if len(family.Metrics) == 0 {
			return fmt.Errorf("MetricFamily has no metrics: %s", family)
		}

		name := family.Name
		if name == "" {
			return fmt.Errorf("MetricFamily has no name: %s", family)
		}

		var err error
		for _, m := range family.Metrics {
			switch family.Type {
			case metrics.MetricType_COUNTER:
				if m.Counter == nil {
					return fmt.Errorf("expected counter in metric %s %v", name, m)
				}
				err = WriteSample(name, &m, "", "", m.Counter.Value, out)

			case metrics.MetricType_COUNTERRATE:
				if m.CounterRate == nil {
					return fmt.Errorf("expected counter-rate in metric %s %v", name, m)
				}
				err = WriteSample(name+"-total", &m, "", "", m.CounterRate.TotalValue, out)
				err = WriteSample(name+"-window", &m, "", "", m.CounterRate.WindowValue, out)
				err = WriteSample(name+"-avg", &m, "", "", m.CounterRate.AvgValue, out)

			case metrics.MetricType_GAUGE:
				if m.Gauge == nil {
					return fmt.Errorf("expected gauge in metric %s %v", name, m)
				}
				err = WriteSample(name, &m, "", "", m.Gauge.Value, out)

			case metrics.MetricType_HISTOGRAM:
				if m.Histogram == nil {
					return fmt.Errorf("expected histogram in metric %s %v", name, m)
				}

				for _, p := range m.Histogram.Pts {
					if err = WriteSample(name+p.Name, &m, "", "", p.Value, out); err != nil {
						return err
					}
				}

			default:
				return fmt.Errorf("unexpected type in metric %s %v", name, m)
			}

			if err != nil {
				return err
			}
		}
	}
	return nil
}
