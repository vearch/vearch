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

package request

import (
	"encoding/json"

	"github.com/vearch/vearch/ps/engine/sortorder"
)

type SearchDocumentRequest struct {
	From        int             `json:"from,omitempty"`
	Size        *int            `json:"size,omitempty"`
	Fields      []string        `json:"fields,omitempty"`
	Query       json.RawMessage `json:"query,omitempty"`
	MinScore    float64         `json:"min_score,omitempty"`
	Sort        json.RawMessage `json:"sort,omitempty"`
	Explain     bool            `json:"explain,omitempty"`
	Quick       bool            `json:"quick,omitempty"`
	VectorValue bool            `json:"vector_value,omitempty"`
	Parallel    bool            `json:"parallel,omitempty"`
	sortOrder   sortorder.SortOrder
}

func (this *SearchDocumentRequest) SortOrder() (sortorder.SortOrder, error) {
	if this.sortOrder != nil {
		return this.sortOrder, nil
	}
	var err error
	this.sortOrder, err = sortorder.ParseSort(this.Sort)
	return this.sortOrder, err
}
