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
	From           int             `json:"from,omitempty"`
	Size           *int            `json:"size,omitempty"`
	Fields         []string        `json:"fields,omitempty"`
	Query          json.RawMessage `json:"query,omitempty"`
	MinScore       float64         `json:"min_score,omitempty"`
	Sort           json.RawMessage `json:"sort,omitempty"`
	RetrievalParam json.RawMessage `json:"retrieval_param,omitempty"`
	Explain        bool            `json:"explain,omitempty"`
	Quick          bool            `json:"quick,omitempty"`
	L2Sqrt         bool            `json:"l2_sqrt,omitempty"`
	VectorValue    bool            `json:"vector_value,omitempty"`
	Parallel       bool            `json:"parallel,omitempty"`
	IVFFlat        bool            `json:"ivf_flat,omitempty"`
	OnlineLogLevel string          `json:"online_log_level"`
	IsBruteSearch  int32           `json:"is_brute_search"`
	sortOrder      sortorder.SortOrder
}

type SearchRequestPo struct {
	SearchDocumentRequestArr []*SearchDocumentRequest `json:"search_doc_arr,omitempty"`
}

func (this *SearchDocumentRequest) SortOrder() (sortorder.SortOrder, error) {
	if this.sortOrder != nil {
		return this.sortOrder, nil
	}
	var err error
	this.sortOrder, err = sortorder.ParseSort(this.Sort)
	return this.sortOrder, err
}

type SearchDocument struct {
	Query          Query    `json:"query,omitempty"`
	Fields         []string `json:"fields,omitempty"`
	OnlineLogLevel string   `json:"online_log_level,omitempty"`
	Quick          bool     `json:"quick,omitempty"`
	VectorValue    bool     `json:"vector_value,omitempty"`
	ClietType      string   `json:"client_type,omitempty"`
	Parallel       bool     `json:"parallel,omitempty"`
	L2Sqrt         bool     `json:"l2_sqrt,omitempty"`
	IVFFlat        bool     `json:"ivf_flat,omitempty"`
	Nprobe         int      `json:"nprobe,omitempty"`
	Size           int      `json:"size,omitempty"`
}

type Query struct {
	Sum    []VecQuery      `json:"sum,omitempty"`
	Filter json.RawMessage `json:"filter,omitempty"`
}

type VecQuery struct {
	Field    string          `json:"field,omitempty"`
	Feature  json.RawMessage `json:"feature,omitempty"`
	MinScore float64         `json:"min_score,omitempty"`
	MaxScore float64         `json:"max_score,omitempty"`
	Boost    float64         `json:"boost,omitempty"`
}
