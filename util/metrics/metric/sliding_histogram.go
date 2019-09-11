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
	"time"

	"github.com/codahale/hdrhistogram"
)

const (
	// The number of histograms to keep in rolling window.
	histWinNum      = 2
	histWinDuration = 10 * time.Second
)

var _ ticker = &slidingHistogram{}

// slidingHistogram is a wrapper around an hdrhistogram.WindowedHistogram.
// Data is kept in the active window for approximately the given duration.
type slidingHistogram struct {
	window   *hdrhistogram.WindowedHistogram
	nextT    time.Time
	duration time.Duration
}

// newSlidingHistogram creates a new windowed HDRHistogram.
func newSlidingHistogram(duration time.Duration, maxVal int64, sigFigs int) *slidingHistogram {
	if duration <= 0 {
		duration = histWinDuration
	}
	return &slidingHistogram{
		nextT:    time.Now(),
		duration: duration,
		window:   hdrhistogram.NewWindowed(histWinNum, 0, maxVal, sigFigs),
	}
}

func (h *slidingHistogram) tick() {
	h.nextT = h.nextT.Add(h.duration / histWinNum)
	h.window.Rotate()
}

func (h *slidingHistogram) nextTick() time.Time {
	return h.nextT
}

func (h *slidingHistogram) current() *hdrhistogram.Histogram {
	maybeTick(h)
	return h.window.Merge()
}

func (h *slidingHistogram) recordValue(v int64) error {
	return h.window.Current.RecordValue(v)
}
