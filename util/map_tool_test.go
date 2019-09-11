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

package util

import (
	"gotest.tools/assert"
	"testing"
)

func TestMergeMap(t *testing.T) {
	dest := make(map[string]interface{})
	subDist := make(map[string]interface{})
	dest["sub"] = subDist
	dest["xxx"] = 123

	src := make(map[string]interface{})
	subSrc := make(map[string]interface{})
	src["sub"] = subSrc

	dest["a1"] = "a1"
	src["a1"] = "src_a1"

	subDist["a2"] = "a2"
	subSrc["a2"] = "src_a2"

	MergeMap(dest, src)

	assert.DeepEqual(t, dest["xxx"], 123)
	assert.DeepEqual(t, dest["a2"], src["a2"])
	assert.DeepEqual(t, dest["sub"].(map[string]interface{})["a2"], src["sub"].(map[string]interface{})["a2"])

}

func TestMergeMapDiffType(t *testing.T) {
	dist := make(map[string]interface{})
	subDist := make(map[string]interface{})
	dist["sub"] = 123

	src := make(map[string]interface{})
	subSrc := make(map[string]interface{})
	src["sub"] = subSrc

	dist["a1"] = "a1"
	src["a1"] = "src_a1"

	subDist["a2"] = "a2"
	subSrc["a2"] = "src_a2"

	MergeMap(dist, src)

	assert.DeepEqual(t, dist["sub"], src["sub"])

}

func TestMergeMapOtherType(t *testing.T) {
	dist := make(map[string]interface{})
	subDist := make(map[string]string)
	subDist["xxx"] = "xxx"
	dist["sub"] = subDist

	src := make(map[string]interface{})
	subSrc := make(map[string]string)

	src["sub"] = subSrc

	dist["a1"] = "a1"
	src["a1"] = "src_a1"

	subDist["a2"] = "a2"
	subSrc["a2"] = "src_a2"

	MergeMap(dist, src)

	assert.DeepEqual(t, dist["sub"], src["sub"])

}

func TestMergeMapOtherType2(t *testing.T) {
	dist := make(map[string]interface{})
	subDist := make(map[string]string)
	dist["sub"] = "XXXX"

	src := make(map[string]interface{})
	subSrc := make(map[string]string)

	src["sub"] = subSrc

	dist["a1"] = "a1"
	src["a1"] = "src_a1"

	subDist["a2"] = "a2"
	subSrc["a2"] = "src_a2"

	MergeMap(dist, src)

	assert.DeepEqual(t, dist["sub"], src["sub"])

}
