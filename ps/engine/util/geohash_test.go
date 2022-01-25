// Copyright 2018 The Couchbase Authors.
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

package util

import (
	"testing"

	"github.com/mmcloughlin/geohash"
)

func TestGeohash(t *testing.T) {
	lat := float64(52.374081)
	lon := float64(4.912350)
	precistion := uint(3)
	aa := geohash.EncodeWithPrecision(lat, lon, precistion)

	println(aa)
}
