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

package algorithm

import (
	"sync"

	"github.com/vearch/vearch/internal/pkg/atomic"
)

type RoundRobin[K comparable, V any] struct {
	counterMap sync.Map
}

func NewRoundRobin[K comparable, V any]() *RoundRobin[K, V] {
	return &RoundRobin[K, V]{}
}

func (rr *RoundRobin[K, V]) Next(k K, v []V) V {
	if len(v) <= 0 {
		var zeroValue V
		return zeroValue
	}

	var ix uint64
	counter, ok := rr.counterMap.LoadOrStore(k, atomic.NewCounter(1))
	if ok {
		// loaded
		newValue := counter.(*atomic.AtomicCounter).Incr()
		ix = (newValue - 1) % uint64(len(v))
	}
	return v[ix]
}
