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
	"errors"
	"fmt"
	"math"
	"unicode"
)

const ShiftStartFlag byte = 0x20

type Value []byte

func NewPrefixCodedInt64(in int64, shift uint) (Value, error) {
	if shift > 63 {
		return nil, fmt.Errorf("cannot shift %d, must be between 0 and 63", shift)
	}

	nChars := ((63 - shift) / 7) + 1
	rv := make(Value, nChars+1)
	rv[0] = ShiftStartFlag + byte(shift)

	sortBits := int64(uint64(in) ^ 0x8000000000000000)
	sortBits = int64(uint64(sortBits) >> shift)
	for nChars > 0 {
		// Store 7 bits per byte for compatibility with UTF-8
		rv[nChars] = byte(sortBits & 0x7f)
		nChars--
		sortBits = int64(uint64(sortBits) >> 7)
	}
	return rv, nil
}

func PrefixCodedInt64(in int64, shift uint) Value {
	rv, err := NewPrefixCodedInt64(in, shift)
	if err != nil {
		panic(err)
	}
	return rv
}

func PrefixCodedFloat64(float float64, shift uint) Value {
	rv, err := NewPrefixCodedInt64(Float64ToInt64(float), shift)
	if err != nil {
		panic(err)
	}
	return rv
}

func (p Value) Shift() (uint, error) {
	if len(p) > 0 {
		shift := p[0] - ShiftStartFlag
		if shift < 0 || shift < 63 {
			return uint(shift), nil
		}
	}
	return 0, errors.New("invalid prefix sort byte value")
}

func (p Value) Int64() (int64, error) {
	shift, err := p.Shift()
	if err != nil {
		return 0, err
	}
	var sortableBits int64
	for _, inbyte := range p[1:] {
		sortableBits <<= 7
		sortableBits |= int64(inbyte)
	}
	return int64(uint64((sortableBits << shift)) ^ 0x8000000000000000), nil
}

func (p Value) Float64() (float64, error) {
	i64, err := p.Int64()
	if err != nil {
		return 0.0, err
	}
	return Int64ToFloat64(i64), nil
}

func Float64ToInt64(f float64) int64 {
	fasint := int64(math.Float64bits(f))
	if fasint < 0 {
		fasint = fasint ^ 0x7fffffffffffffff
	}
	return fasint
}

func Int64ToFloat64(i int64) float64 {
	if i < 0 {
		i ^= 0x7fffffffffffffff
	}
	return math.Float64frombits(uint64(i))
}

func Normalization(feature []float32) error {
	var sum float32

	for _, v := range feature {
		sum += v * v
	}
	if math.IsNaN(float64(sum)) || math.IsInf(float64(sum), 0) || sum == 0 {
		return fmt.Errorf("normalization err , sum value is:[%v]", sum)
	}

	sum = float32(math.Sqrt(float64(sum)))

	for i, v := range feature {
		feature[i] = v / sum
	}

	return nil
}

func IsNum(s string) bool {
	if len(s) == 0 {
		return false
	}

	for _, v := range s {
		if !unicode.IsNumber(v) {
			return false
		}
	}
	return true
}
