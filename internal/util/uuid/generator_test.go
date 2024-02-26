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

package uuid

import (
	"runtime"
	"sync"
	"testing"
)

func TestFlakeUUID(t *testing.T) {
	var m sync.Map
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 5000000; j++ {
				uuid := FlakeUUID()
				if _, ok := m.LoadOrStore(uuid, true); ok {
					t.Fatal("FlakeUUID occur conflict.")
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
