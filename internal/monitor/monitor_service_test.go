// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package monitor

import (
	"testing"
	"time"
)

func TestProfiler(t *testing.T) {
	type args struct {
		key       string
		startTime time.Time
	}
	now := time.Now()
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Profile operation with immediate start",
			args: args{
				key:       "operation1",
				startTime: now,
			},
		},
		{
			name: "Profile operation with past start time",
			args: args{
				key:       "operation2",
				startTime: now.Add(-5 * time.Minute), // started 5 minutes ago
			},
		},
		// TODO: Add more test cases with different keys and start times as needed.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Profiler(tt.args.key, tt.args.startTime)
		})
	}
}
