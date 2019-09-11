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

package sysstat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/vearch/vearch/util/bufalloc"
	"github.com/vearch/vearch/util/metrics"
	"github.com/vearch/vearch/util/metrics/export"
)

func TestRuntimeStatSampler(t *testing.T) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	run := NewRuntimeStatSampler(RuntimeStatOption{
		Ctx:        ctx,
		HeapSample: true,
		Interval:   2 * time.Second,
		DiskPath:   []string{os.TempDir()},
	})
	run.Start()

	r := metrics.NewRegistry()
	r.AddMetricStruct(run)
	e := export.NewMemoryExporter()

	expTimer := metrics.NewExportTimer(ctx, 5*time.Second)
	expTimer.AddExporter(e)
	expTimer.AddRegistry(r)
	expTimer.Start()

	time.Sleep(6 * time.Second)
	buf := bufalloc.AllocBuffer(2048)
	e.PrintText(buf)
	t.Log(buf.String())
	bufalloc.FreeBuffer(buf)

	ctxCancel()
}
