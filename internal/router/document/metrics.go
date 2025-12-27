// Copyright 2019 The Vearch Authors.
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

package document

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTP request metrics
	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "vearch_router_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint", "status"},
	)

	httpRequestTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vearch_router_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)

	// Document operation metrics
	documentOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "vearch_router_document_operation_duration_seconds",
			Help:    "Document operation duration in seconds",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"operation", "status"},
	)

	documentOperationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vearch_router_document_operations_total",
			Help: "Total number of document operations",
		},
		[]string{"operation", "status"},
	)

	// Bulk operation metrics
	bulkDocumentsProcessed = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "vearch_router_bulk_documents_count",
			Help:    "Number of documents in bulk operations",
			Buckets: []float64{1, 10, 50, 100, 500, 1000, 5000, 10000},
		},
		[]string{"operation"},
	)

	// Cache metrics
	cacheLookupTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vearch_router_cache_lookups_total",
			Help: "Total number of cache lookups",
		},
		[]string{"cache_type", "hit"},
	)

	cacheSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vearch_router_cache_size",
			Help: "Current size of cache",
		},
		[]string{"cache_type"},
	)

	// Error metrics
	errorTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vearch_router_errors_total",
			Help: "Total number of errors",
		},
		[]string{"error_type", "operation"},
	)

	// Timeout metrics
	timeoutTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vearch_router_timeouts_total",
			Help: "Total number of timeouts",
		},
		[]string{"operation"},
	)

	// Connection pool metrics (if applicable)
	activeConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "vearch_router_active_connections",
			Help: "Number of active connections to partition servers",
		},
	)
)

// MetricsRecorder provides methods to record various metrics
type MetricsRecorder struct{}

// NewMetricsRecorder creates a new metrics recorder
func NewMetricsRecorder() *MetricsRecorder {
	return &MetricsRecorder{}
}

// RecordHTTPRequest records HTTP request metrics
func (m *MetricsRecorder) RecordHTTPRequest(method, endpoint, status string, duration time.Duration) {
	httpRequestDuration.WithLabelValues(method, endpoint, status).Observe(duration.Seconds())
	httpRequestTotal.WithLabelValues(method, endpoint, status).Inc()
}

// RecordDocumentOperation records document operation metrics
func (m *MetricsRecorder) RecordDocumentOperation(operation, status string, duration time.Duration) {
	documentOperationDuration.WithLabelValues(operation, status).Observe(duration.Seconds())
	documentOperationTotal.WithLabelValues(operation, status).Inc()
}

// RecordBulkOperation records bulk operation metrics
func (m *MetricsRecorder) RecordBulkOperation(operation string, documentCount int) {
	bulkDocumentsProcessed.WithLabelValues(operation).Observe(float64(documentCount))
}

// RecordCacheLookup records cache lookup metrics
func (m *MetricsRecorder) RecordCacheLookup(cacheType string, hit bool) {
	hitStr := "miss"
	if hit {
		hitStr = "hit"
	}
	cacheLookupTotal.WithLabelValues(cacheType, hitStr).Inc()
}

// UpdateCacheSize updates cache size metric
func (m *MetricsRecorder) UpdateCacheSize(cacheType string, size int) {
	cacheSize.WithLabelValues(cacheType).Set(float64(size))
}

// RecordError records error metrics
func (m *MetricsRecorder) RecordError(errorType, operation string) {
	errorTotal.WithLabelValues(errorType, operation).Inc()
}

// RecordTimeout records timeout metrics
func (m *MetricsRecorder) RecordTimeout(operation string) {
	timeoutTotal.WithLabelValues(operation).Inc()
}

// UpdateActiveConnections updates active connections metric
func (m *MetricsRecorder) UpdateActiveConnections(count int) {
	activeConnections.Set(float64(count))
}
