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

type SortField struct {
	Field        string
	Desc         bool
	Mode         SortFieldMode
	Missing      SortFieldMissing
	MissingValue interface{}
	valueType    ValueType
}

func NewSortField(field string) *SortField {
	return &SortField{Field: field}
}

func (s *SortField) SetMissing(missingMode SortFieldMissing, missingValue interface{}) {
	s.Missing = missingMode
	s.MissingValue = missingValue
}

func (s *SortField) SetMode(mode SortFieldMode) {
	s.Mode = mode
}

func (s *SortField) SetOrder(desc bool) {
	s.Desc = desc
}

// If i < j return -1; if i == j return 0; if i > j return 1
func (s *SortField) Compare(i, j SortValue) int {
	c := i.Compare(j)
	if s.Desc {
		return -1 * c
	}
	return c
}

func (s *SortField) SortField() string {
	return s.Field
}
