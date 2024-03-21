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

package metrics

import (
	"reflect"
	"sync"

	"github.com/vearch/vearch/internal/pkg/vjson"
)

// Registry is a list of metrics. It provides a simple way of iterating over them.
// A registry can have label pairs that will be applied to all its metrics when exported.
type Registry struct {
	sync.RWMutex
	labels  []LabelPair
	metrics []Metric
}

// MetricStruct can be implemented by the types of members of a metric container,
// so that the members get automatically registered.
type MetricStruct interface {
	MetricStruct()
}

// NewRegistry create a Registry.
func NewRegistry() *Registry {
	return new(Registry)
}

// AddLabel adds a label/value pair for this registry.
func (r *Registry) AddLabel(name, value string) {
	r.Lock()
	r.labels = append(r.labels,
		LabelPair{
			Name:  ExportedLabel(name),
			Value: value,
		})
	r.Unlock()
}

// GetLabels return the label/value pairs of registry.
func (r *Registry) GetLabels() (labels []LabelPair) {
	r.RLock()
	labels = r.labels[:]
	r.RUnlock()
	return
}

// AddMetric adds the passed-in metric to the registry.
func (r *Registry) AddMetric(metric ...Metric) {
	r.Lock()
	r.metrics = append(r.metrics, metric...)
	r.Unlock()
}

// AddMetricStruct adds all Metric or MetricStruct fields to the registry.
func (r *Registry) AddMetricStruct(metricStruct interface{}) {
	v := reflect.ValueOf(metricStruct)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < v.NumField(); i++ {
		vfield := v.Field(i)
		if !vfield.CanInterface() {
			continue
		}
		val := vfield.Interface()
		switch typ := val.(type) {
		case Metric:
			r.AddMetric(typ)
		case MetricStruct:
			r.AddMetricStruct(typ)
		}
	}
}

// Each calls the given closure for all metrics.
func (r *Registry) Each(f func(name string, val Metric)) {
	r.RLock()
	for _, metric := range r.metrics {
		metric.Inspect(func(v Metric) {
			f(ExportedName(metric.GetMetadata().Name), v)
		})
	}
	r.RUnlock()
}

// FindMetric looking for metric based on name.
func (r *Registry) FindMetric(name string) (m Metric) {
	r.RLock()
	for _, metric := range r.metrics {
		if metric.GetMetadata().Name == name {
			m = metric
		}
	}
	r.RUnlock()
	return
}

// MarshalJSON marshals to JSON.
func (r *Registry) MarshalJSON() ([]byte, error) {
	m := make(map[string]Metric)
	r.Each(func(nm string, v Metric) {
		m[nm] = v
	})
	return vjson.Marshal(m)
}
