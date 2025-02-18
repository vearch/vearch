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
	"regexp"
	"strings"
)

var (
	metricNameReplaceRE  = regexp.MustCompile("^[^a-zA-Z_:]|[^a-zA-Z0-9_:]")
	metricLabelReplaceRE = regexp.MustCompile("^[^a-zA-Z_]|[^a-zA-Z0-9_]")
)

// Metric provides a method for metric objects.
type Metric interface {
	// GetMetadata returns the metric's metadata.
	GetMetadata() Metadata
	// GetType returns the type enum for this metric.
	GetType() MetricType
	// Inspect calls the given closure with each contained item.
	Inspect(func(Metric))
	// ExportMetric returns a filled-in metric data of the right type for the given metric.
	ExportMetric() *MetricData
}

// AddLabel adds a label/value pair for this metric.
func (m *Metadata) AddLabel(name, value string) {
	m.Labels = append(m.Labels,
		&LabelPair{
			Name:  ExportedLabel(name),
			Value: value,
		})
}

// ExportedName generates a valid metric name.
func ExportedName(name string) string {
	return metricNameReplaceRE.ReplaceAllString(name, "_")
}

// ExportedLabel generates a valid metric label.
func ExportedLabel(name string) string {
	return metricLabelReplaceRE.ReplaceAllString(name, "_")
}

type MetricFamilySlice []*MetricFamily

func (s MetricFamilySlice) Len() int {
	return len(s)
}

func (s MetricFamilySlice) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s MetricFamilySlice) Less(i int, j int) bool {
	return strings.Compare(s[i].Name, s[j].Name) < 0
}
