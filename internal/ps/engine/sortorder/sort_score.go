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
package sortorder

import "github.com/vearch/vearch/v3/internal/ps/engine/mapping"

type SortScore struct {
	Desc bool
}

func (s *SortScore) Compare(i, j SortValue) int {
	c := i.Compare(j)
	if s.Desc {
		return -1 * c
	}
	return c
}
func (s *SortScore) SortField() string {
	return mapping.ScoreField
}
func (s *SortScore) GetSortOrder() bool {
	return s.Desc
}
