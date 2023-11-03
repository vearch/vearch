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
	"sync"
	"time"

	"github.com/caio/go-tdigest"
)

// type KeyHistogram string
// const (
//
//	Query   KeyHistogram = "query"
//	Operate KeyHistogram = "operate"
//
// )
var mutex sync.Mutex

var metricMap = map[string]*Digest{}

func Profiler(key string, startTime time.Time) {
	mutex.Lock()
	digest, ok := metricMap[key]
	if !ok {
		digest = NewDigest(key, 0.0)
		metricMap[key] = digest
	}

	mutex.Unlock()

	digest.Lock()
	costTime := (time.Since(startTime).Seconds()) * 1000
	//fmt.Printf("start:%v end:%v cost = %v   \n", startNanosecond, time.Now().Nanosecond(), costTime/1000000)
	digest.Digest.Add(float64(costTime))
	digest.Sum = digest.Sum + float64(costTime)

	digest.Unlock()
}

func SliceMetric() map[string]*Digest {
	mutex.Lock()
	newMap := metricMap
	metricMap = make(map[string]*Digest)
	mutex.Unlock()

	return newMap
}

type Digest struct {
	Name   string
	Sum    float64
	Digest tdigest.TDigest
	sync.Mutex
}

func NewDigest(Name string, Sum float64) *Digest {
	digest, _ := tdigest.New(tdigest.Compression(100))
	h := &Digest{
		Name:   Name,
		Sum:    Sum,
		Digest: *digest,
	}
	return h
}
