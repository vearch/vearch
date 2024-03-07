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

package atomic

import "sync/atomic"

type AtomicInt64 struct {
	v int64
}

func NewAtomicInt64(v int64) *AtomicInt64 {
	return &AtomicInt64{v: v}
}

func (a *AtomicInt64) Get() int64 {
	return atomic.LoadInt64(&a.v)
}

func (a *AtomicInt64) Set(v int64) {
	atomic.StoreInt64(&a.v, v)
}

func (a *AtomicInt64) Add(v int64) int64 {
	return atomic.AddInt64(&a.v, v)
}

func (a *AtomicInt64) Incr() int64 {
	return atomic.AddInt64(&a.v, 1)
}

func (a *AtomicInt64) Decr() int64 {
	return atomic.AddInt64(&a.v, -1)
}

func (a *AtomicInt64) CompareAndSwap(o, n int64) bool {
	return atomic.CompareAndSwapInt64(&a.v, o, n)
}
