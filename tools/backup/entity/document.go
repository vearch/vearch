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

package entity

import "encoding/json"

type QueryByDocID struct {
	DbName    string `json:"db_name"`
	SpaceName string `json:"space_name"`
	Query     struct {
		DocumentIds []string `json:"document_ids"`
		PartitionID string   `json:"partition_id"`
		Next        bool     `json:"next"`
	} `json:"query"`
	VectorValue bool `json:"vector_value"`
}

type DocInfo struct {
	Code      int    `json:"code"`
	Msg       string `json:"msg"`
	Total     int    `json:"total"`
	Documents []struct {
		ID     string          `json:"_id"`
		Source json.RawMessage `json:"_source"`
	} `json:"documents"`
}

type Document struct {
	DbName    string            `json:"db_name"`
	SpaceName string            `json:"space_name"`
	Documents []json.RawMessage `json:"documents"`
}
