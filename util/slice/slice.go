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

package slice

import (
	"reflect"
	"sort"
)

type FilterFunc func(element string) bool

func GetFirstNotNullElement(slice []string, f FilterFunc) string {
	for _, value := range slice {
		if f(value) == true {
			return value
		}
	}
	return ""
}

// Equal tells whether a and b contain the same elements.
// A nil argument is equivalent to an empty slice.
func EqualInt(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// Equal tells whether a and b contain the same elements.
// A nil argument is equivalent to an empty slice.
func EqualUint32(a, b []uint32) bool {
	aa := make([]int, len(a))
	bb := make([]int, len(b))
	for i, item := range a {
		aa[i] = int(item)
	}
	for i, item := range b {
		bb[i] = int(item)
	}
	sort.Ints(aa)
	sort.Ints(bb)
	return EqualInt(aa, bb)
}

// IsExistSlice tells whether item exist this slice
func IsExistSlice(item interface{}, arr interface{}) (bool, int) {
	if arr != nil {
		switch reflect.TypeOf(arr).Kind() {
		case reflect.Slice:
			s := reflect.ValueOf(arr)
			for i := 0; i < s.Len(); i++ {
				if reflect.DeepEqual(item, s.Index(i).Interface()) {
					return true, i
				}
			}
		}
	}
	return false, -1
}
