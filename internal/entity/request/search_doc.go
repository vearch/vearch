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

	"github.com/vearch/vearch/internal/ps/engine/sortorder"
)

var (
	SearchResponse string = "SearchResponse"
	QueryResponse  string = "QueryResponse"
	QueryAnd       string = "and"
	QueryVector    string = "vector"
)

type DocumentRequest struct {
	Documents []json.RawMessage `json:"documents,omitempty"`
	DbName    string            `json:"db_name,omitempty"`
	SpaceName string            `json:"space_name,omitempty"`
}

type IndexRequest struct {
	DbName            string `json:"db_name,omitempty"`
	SpaceName         string `json:"space_name,omitempty"`
	DropBeforeRebuild bool   `json:"drop_before_rebuild,omitempty"`
	LimitCPU          int    `json:"limit_cpu,omitempty"`
	Describe          int    `json:"describe,omitempty"`
}

type Condition struct {
	Operator string          `json:"operator"`
	Field    string          `json:"field,omitempty"`
	Value    json.RawMessage `json:"value,omitempty"`
}

type Filter struct {
	Operator   string      `json:"operator"`
	Conditions []Condition `json:"conditions,omitempty"`
}

type SearchDocumentRequest struct {
	Limit         int32             `json:"limit,omitempty"`
	Fields        []string          `json:"fields,omitempty"`
	Filters       *Filter           `json:"filters,omitempty"`
	Vectors       []json.RawMessage `json:"vectors,omitempty"`
	Sort          json.RawMessage   `json:"sort,omitempty"`
	IndexParams   json.RawMessage   `json:"index_params,omitempty"`
	L2Sqrt        bool              `json:"l2_sqrt,omitempty"`
	VectorValue   bool              `json:"vector_value,omitempty"`
	IsBruteSearch int32             `json:"is_brute_search"`
	DbName        string            `json:"db_name,omitempty"`
	SpaceName     string            `json:"space_name,omitempty"`
	LoadBalance   string            `json:"load_balance"`
	DocumentIds   *[]string         `json:"document_ids,omitempty"`
	PartitionId   *string           `json:"partition_id,omitempty"`
	Next          *bool             `json:"next,omitempty"`
	sortOrder     sortorder.SortOrder
}

func (s *SearchDocumentRequest) SortOrder() (sortorder.SortOrder, error) {
	if s.sortOrder != nil {
		return s.sortOrder, nil
	}
	var err error
	s.sortOrder, err = sortorder.ParseSort(s.Sort)
	return s.sortOrder, err
}
