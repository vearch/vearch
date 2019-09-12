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

func SelectKthMin(s []float64, k int) float64 {
	k--
	lo, hi := 0, len(s)-1
	for {
		j := partition(s, lo, hi)
		if j < k {
			lo = j + 1
		} else if j > k {
			hi = j - 1
		} else {
			return s[k]
		}
	}
}

//选出中位数（比一半的元素小，比另一半的大）
func SelectMid(s []float64) float64 {
	return SelectKthMin(s, len(s)/2)
}

//选出k个最小元素，k为1~len(s)
func SelectKMin(s []float64, k int) []float64 {
	lo, hi := 0, len(s)-1
	for {
		j := partition(s, lo, hi)
		if j < k {
			lo = j + 1
		} else if j > k {
			hi = j - 1
		} else {
			return s[:k]
		}
	}
}

func partition(s []float64, lo, hi int) int {
	i, j := lo, hi+1
	for {
		for {
			i++
			if i == hi || s[i] > s[lo] {
				break
			}
		}
		for {
			j--
			if j == lo || s[j] <= s[lo] {
				break
			}
		}
		if i >= j {
			break
		}
		swap(s, i, j)
	}
	swap(s, lo, j)
	return j
}

func swap(s []float64, i int, j int) {
	s[i], s[j] = s[j], s[i]
}
