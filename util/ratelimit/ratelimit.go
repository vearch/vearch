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
package ratelimit

import (
	"github.com/juju/ratelimit"
	"sync"
	"time"
)

type RateLimit interface {
	Wait(int64) bool
}

type BucketRateLimit struct {
	timeout time.Duration
	bucket  *ratelimit.Bucket
}

func NewBucketRateLimit(rate int, capacity int64, waitTimeout time.Duration) RateLimit {
	return &BucketRateLimit{timeout: waitTimeout, bucket: ratelimit.NewBucketWithRate(float64(rate), capacity)}
}

func (r *BucketRateLimit) Wait(n int64) bool {
	if r.timeout == 0 {
		r.bucket.Wait(n)
		return true
	} else {
		return r.bucket.WaitMaxDuration(n, r.timeout)
	}
}

type NullRateLimit struct{}

func NewNullRateLimit() RateLimit {
	return &NullRateLimit{}
}

func (r *NullRateLimit) Wait(n int64) bool {
	return true
}

type ConcurrentLimit struct {
	timeout time.Duration
	done    chan struct{}
	sem     chan struct{}
	once    sync.Once
}

func NewConcurrentLimit(c int, timeout time.Duration) *ConcurrentLimit {
	if c <= 0 {
		panic("invalid concurrent")
	}
	if timeout == 0 {
		// set big time for default
		timeout = time.Minute * 10
	}
	return &ConcurrentLimit{
		sem:     make(chan struct{}, c),
		done:    make(chan struct{}),
		timeout: timeout}
}

func (c *ConcurrentLimit) GetToken() bool {
	select {
	case c.sem <- struct{}{}:
	case <-c.done:
		return false
	case <-time.After(c.timeout):
		return false
	}
	return true
}

func (c *ConcurrentLimit) PutToken() {
	<-c.sem
}

func (c *ConcurrentLimit) Close() {
	c.once.Do(func() { close(c.done) })
}
