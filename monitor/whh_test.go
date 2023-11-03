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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/caio/go-tdigest"
)

func TestTdigest(t *testing.T) {
	td, _ := tdigest.New(tdigest.Compression(100))
	for i := 0; i < 10000; i++ {
		td.Add(rand.Float64())
	}

	tp50 := td.Quantile(0.5)

	fmt.Printf("count:%v\n", td.Count())
	fmt.Printf("p(.5) = %.6f\n", tp50)
	fmt.Printf("CDF(Quantile(.5)) = %.6f\n", td.CDF(td.Quantile(0.5)))

	_startTime := time.Now().Nanosecond()
	fmt.Println(_startTime)

	for i := 0; i < 10000; i++ {
		Profiler("bulk_insert_"+string(rand.Intn(10)+65), time.Now().Nanosecond())
	}

	my := SliceMetric()
	for _, element := range my {
		fmt.Printf("key:%v----Sum=%v---Count=%v-- max=%v--\n", element.Name, element.Sum, element.Digest.Count(), element.Digest.Quantile(1))
	}

	Profiler("bulk_insert", time.Now().Nanosecond())

	defer Profiler("defer", time.Now().Nanosecond())

	fmt.Printf("time Sleep = %v  \n", time.Now().Nanosecond())
	time.Sleep(time.Duration(2) * time.Second)
	fmt.Printf("time Sleep after = %v \n", time.Now().Nanosecond())
}
