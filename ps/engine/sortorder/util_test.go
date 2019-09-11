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
package sortorder

import (
	"math"
	"testing"
)

func TestSelectMid(t *testing.T) {
	array := []float64{1.0, 2.3, 19.4, 4.78, 9.87, 0.33, -12.4}
	m := SelectMid(array)
	if m-array[0] > 0 && array[0]-m > 0 {
		t.Fatalf("select mid failed %f", m)
	}

	array1 := []float64{1, 2, 3, 4, 5, 6}
	m = SelectMid(array1)
	expect := (3.0 + 4.0) / 2
	if m-expect > 0 && expect-m > 0 {
		t.Fatalf("select mid failed %f", m)
	}

	array2 := []float64{2, 1, 3}
	m = SelectMid(array2)
	expect = float64(1)
	if math.Abs(expect-m) > 0 {
		t.Fatalf("select mid failed %f %f", m, expect)
	}
}
