//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sortorder

import (
	"fmt"
	"math/big"
	"strings"
	"time"
)

type ValueType int

const (
	ValueType_Null ValueType = iota
	ValueType_String
	ValueType_Int
	ValueType_Float
	ValueType_Bool
	ValueType_Date
)

var NegativeInfinity *big.Int
var PositiveInfinity *big.Int

type SortValue interface {
	Value() interface{}
	Compare(SortValue) int
}

type InfinitySortValue struct {
	Typ      ValueType
	Negative bool
}

func (sv *InfinitySortValue) Value() interface{} {
	switch sv.Typ {
	case ValueType_String:
		return nil
	case ValueType_Float:
		if sv.Negative {
			return "-Infinity"
		} else {
			return "Infinity"
		}
	case ValueType_Int, ValueType_Bool:
		if sv.Negative {
			return NegativeInfinity
		} else {
			return PositiveInfinity
		}
	}
	return nil
}

func (sv *InfinitySortValue) Compare(other SortValue) int {
	switch s := other.(type) {
	case *InfinitySortValue:
		if sv.Negative && !s.Negative {
			return -1
		} else if !sv.Negative && s.Negative {
			return 1
		} else {
			return 0
		}
	default:
		if sv.Negative {
			return -1
		} else {
			return 1
		}
	}
}

type StringSortValue struct {
	Val      string
	SortName string
}

func (sv *StringSortValue) Type() ValueType {
	return ValueType_String
}

func (sv *StringSortValue) Value() interface{} {
	return sv.Val
}

func (sv *StringSortValue) Compare(other SortValue) int {
	switch s := other.(type) {
	case *StringSortValue:
		return strings.Compare(sv.Val, s.Val)
	case *InfinitySortValue:
		return -1 * s.Compare(sv)
	}
	return 1
}

type IntSortValue struct {
	Val      int64
	SortName string
}

func (sv *IntSortValue) Value() interface{} {
	return sv.Val
}

func (sv *IntSortValue) Compare(other SortValue) int {
	switch s := other.(type) {
	case *IntSortValue:
		c := sv.Val - s.Val
		switch {
		case c > 0:
			return 1
		case c < 0:
			return -1
		default:
			return 0
		}
	case *FloatSortValue:
		c := float64(sv.Val) - s.Val
		switch {
		case c > 0:
			return 1
		case c < 0:
			return -1
		default:
			return 0
		}
	case *InfinitySortValue:
		return -1 * s.Compare(sv)
	}
	return -1
}

type FloatSortValue struct {
	Val      float64
	SortName string
}

func (sv *FloatSortValue) Value() interface{} {
	return sv.Val
}

func (sv *FloatSortValue) Compare(other SortValue) int {
	switch s := other.(type) {
	case *FloatSortValue:
		c := sv.Val - s.Val
		switch {
		case c > 0:
			return 1
		case c < 0:
			return -1
		default:
			return 0
		}
	case *IntSortValue:
		c := sv.Val - float64(s.Val)
		switch {
		case c > 0:
			return 1
		case c < 0:
			return -1
		default:
			return 0
		}
	case *InfinitySortValue:
		return -1 * s.Compare(sv)
	}
	return -1
}

type GeoDistanceSortValue struct {
	Val  float64
	Unit string
}

func (sv *GeoDistanceSortValue) Value() interface{} {
	return fmt.Sprintf("%f%s", sv.Val, sv.Unit)
}

func (sv *GeoDistanceSortValue) Compare(other SortValue) int {
	switch s := other.(type) {
	case *GeoDistanceSortValue:
		c := sv.Val - s.Val
		switch {
		case c > 0:
			return 1
		case c < 0:
			return -1
		default:
			return 0
		}
	case *InfinitySortValue:
		return -1 * s.Compare(sv)
	}
	return -1
}

type DateSortValue struct {
	Val time.Time
}

func (sv *DateSortValue) Value() interface{} {
	return sv.Val
}

func (sv *DateSortValue) Compare(other SortValue) int {
	switch s := other.(type) {
	case *DateSortValue:
		c := sv.Val.UnixNano() - s.Val.UnixNano()
		switch {
		case c > 0:
			return 1
		case c < 0:
			return -1
		default:
			return 0
		}
	}
	return -1
}

type SortValues []SortValue

func (svs SortValues) Less(i, j int) bool {
	if svs[i].Compare(svs[j]) < 0 {
		return true
	}
	return false
}

func (svs SortValues) Len() int {
	return len(svs)
}

// Swap swaps the elements with indexes i and j.
func (svs SortValues) Swap(i, j int) {
	svs[i], svs[j] = svs[j], svs[i]
}

func (vs SortValues) Values() []interface{} {
	if vs == nil {
		return nil
	}
	vals := make([]interface{}, 0, len(vs))
	for _, v := range vs {
		vals = append(vals, v.Value())
	}
	return vals
}

func (vs SortValues) Reset() SortValues {
	return vs[:0]
}

type Sort interface {
	Compare(i, j SortValue) int
	SortField() string
	GetSortOrder() bool
}

type SortOrder []Sort

// Compare will compare two document matches using the specified sort order
// if both are numbers, we avoid converting back to term
func (so SortOrder) Compare(a, b SortValues) int {
	// compare the documents on all search sorts until a differences is found
	for i, s := range so {
		c := s.Compare(a[i], b[i])
		if c == 0 {
			continue
		}
		return c
	}
	return 0
}

// SortFieldMode describes the behavior if the field has multiple values
type SortFieldMode int

const (
	// SortFieldDefault uses the first (or only) value, this is the default zero-value
	SortFieldDefault SortFieldMode = iota // FIXME name is confusing
	// SortFieldMin uses the minimum value
	SortFieldMin
	// SortFieldMax uses the maximum value
	SortFieldMax
	// Use the sum of all values as sort value. Only applicable for number based array fields.
	SortFieldSum
	// Use the average of all values as sort value. Only applicable for number based array fields.
	SortFieldAvg
	// Use the median of all values as sort value. Only applicable for number based array fields.
	SortFieldMedian
)

// SortFieldMissing controls where documents missing a field value should be sorted
type SortFieldMissing int

const (
	// SortFieldMissingLast sorts documents missing a field at the end
	SortFieldMissingLast SortFieldMissing = iota

	// SortFieldMissingFirst sorts documents missing a field at the beginning
	SortFieldMissingFirst

	// SortFieldMissingCustom sorts documents missing a field with a custom value
	SortFieldMissingCustom
)

var default_sort = &SortOrder{&SortScore{Desc: true}}

func init() {
	PositiveInfinity = new(big.Int).SetUint64(9223372036854776000)
	NegativeInfinity = new(big.Int).Neg(PositiveInfinity)
}
