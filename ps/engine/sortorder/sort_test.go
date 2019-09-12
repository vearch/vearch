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
	"github.com/dustin/gojson"
	"strings"
	"testing"
)

func TestInfinitySortValue(t *testing.T) {
	a := &InfinitySortValue{}
	b := &InfinitySortValue{}
	if a.Compare(b) != 0 {
		t.Fatal("InfinitySortValue compare failed")
	}

	c := &InfinitySortValue{Negative: true}
	if !(a.Compare(c) > 0) {
		t.Fatal("InfinitySortValue compare failed")
	}

	d := &IntSortValue{Val: 10}
	if !(a.Compare(d) > 0) {
		t.Fatal("InfinitySortValue compare failed")
	}

	e := &InfinitySortValue{Negative: true}
	f := &InfinitySortValue{Negative: true}
	if e.Compare(f) != 0 {
		t.Fatal("InfinitySortValue compare failed")
	}
	svs := []SortValue{&InfinitySortValue{Typ: ValueType_Int}, &InfinitySortValue{Typ: ValueType_Int, Negative: true},
		&InfinitySortValue{Typ: ValueType_Int}, &InfinitySortValue{Typ: ValueType_Int, Negative: true},
		&InfinitySortValue{Typ: ValueType_Float}, &InfinitySortValue{Typ: ValueType_Float, Negative: true},
		&InfinitySortValue{Typ: ValueType_String}}
	expect := []string{`9223372036854776000`, `-9223372036854776000`,
		`9223372036854776000`, `-9223372036854776000`,
		`"Infinity"`, `"-Infinity"`, `null`}

	for i, sv := range svs {
		data, err := json.Marshal(sv.Value())
		if err != nil {
			t.Fatal(err)
		}
		if strings.Compare(string(data), expect[i]) != 0 {
			t.Fatal("Infinity sort value invalid")
		}
	}
}

func TestNumberSortValue(t *testing.T) {
	x := &InfinitySortValue{}
	y := &InfinitySortValue{Negative: true}

	a := &IntSortValue{Val: 10}
	b := &IntSortValue{Val: 11}
	if a.Compare(b) >= 0 {
		t.Fatal("int compare faield")
	}
	if b.Compare(a) <= 0 {
		t.Fatal("int compare faield")
	}
	if a.Compare(x) >= 0 {
		t.Fatal("int compare faield")
	}

	if a.Compare(y) <= 0 {
		t.Fatal("int compare faield")
	}

	c := &FloatSortValue{Val: 10}
	d := &IntSortValue{Val: 11}
	if c.Compare(d) >= 0 {
		t.Fatal("int compare faield")
	}
	if d.Compare(c) <= 0 {
		t.Fatal("int compare faield")
	}

	if c.Compare(x) >= 0 {
		t.Fatal("int compare faield")
	}

	if c.Compare(y) <= 0 {
		t.Fatal("int compare faield")
	}

	s1 := &FloatSortValue{Val: 0.7126753330230713}
	s2 := &FloatSortValue{Val: 1.000000238418579}

	if s1.Compare(s2) >= 0 {
		t.Fatal("int compare faield")
	}
}

func TestGeoDistanceSortValue(t *testing.T) {
	x := &InfinitySortValue{}
	y := &InfinitySortValue{Negative: true}

	a := &GeoDistanceSortValue{Val: 10}
	b := &GeoDistanceSortValue{Val: 11}
	if a.Compare(b) >= 0 {
		t.Fatal("int compare faield")
	}
	if b.Compare(a) <= 0 {
		t.Fatal("int compare faield")
	}
	if a.Compare(x) >= 0 {
		t.Fatal("int compare faield")
	}

	if a.Compare(y) <= 0 {
		t.Fatal("int compare faield")
	}
}
