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

type EngineConfig struct {
	EngineCacheSize *int64  `json:"engine_cache_size,omitempty"`
	Path            *string `json:"path,omitempty"`
	LongSearchTime  *int64  `json:"long_search_time,omitempty"`
}

type EngineStatus struct {
	IndexStatus   int32 `json:"index_status,omitempty"`
	BackupStatus  int32 `json:"backup_status,omitempty"`
	DocNum        int32 `json:"doc_num,omitempty"`
	MinIndexedNum int32 `json:"min_indexed_num,omitempty"`
	MaxDocid      int32 `json:"max_docid,omitempty"`
}
