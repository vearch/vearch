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
	"context"
	"io"
	"sync"
	"time"

	"github.com/vearch/vearch/util/routine"
)

const defaultExportInterval = 10 * time.Second

// Exporter export metrics in Registry.
type Exporter interface {
	DumpRegistry([]*Registry)
	GetMetric() map[string]*MetricFamily
	PrintText(io.Writer) error
}

type ExportTimer struct {
	ctx      context.Context
	interval time.Duration

	sync.RWMutex
	registrys []*Registry
	exporters []Exporter
}

func NewExportTimer(ctx context.Context, interval time.Duration) *ExportTimer {
	if ctx == nil {
		//Error:the cancel function returned by context.WithCancel should be called, not discarded, to avoid a context leak
		ctx, _ = context.WithCancel(context.Background())
	}
	if interval <= 0 {
		interval = defaultExportInterval
	}
	return &ExportTimer{ctx: ctx, interval: interval}
}

func (t *ExportTimer) AddRegistry(registry ...*Registry) {
	t.Lock()
	t.registrys = append(t.registrys, registry...)
	t.Unlock()
}

func (t *ExportTimer) AddExporter(exporter ...Exporter) {
	t.Lock()
	t.exporters = append(t.exporters, exporter...)
	t.Unlock()
}

func (t *ExportTimer) Start() error {
	return routine.RunWorkDaemon("METRICS-EXPORTTIMER", t.export, t.ctx.Done())
}

func (t *ExportTimer) export() {
	timer := time.NewTimer(t.interval)
	defer timer.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return

		case <-timer.C:
			t.RLock()
			r := t.registrys[:]
			for _, e := range t.exporters {
				routine.RunWorkAsync("METRICS-EXPORT", func() {
					e.DumpRegistry(r)
				})
			}
			t.RUnlock()
		}

		timer.Reset(t.interval)
	}
}
