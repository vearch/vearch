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

type DescribeSpaceResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		SpaceName    string `json:"space_name"`
		DbName       string `json:"db_name"`
		PartitionNum int    `json:"partition_num"`
		ReplicaNum   int    `json:"replica_num"`
		Schema       struct {
			Properties json.RawMessage `json:"properties"`
			Engine     json.RawMessage `json:"engine"`
		} `json:"schema"`
		DocNum     int `json:"doc_num"`
		Partitions []struct {
			Pid         int             `json:"pid"`
			ReplicaNum  int             `json:"replica_num"`
			Path        string          `json:"path"`
			Status      int             `json:"status"`
			Color       string          `json:"color"`
			IP          string          `json:"ip"`
			NodeID      int             `json:"node_id"`
			RaftStatus  json.RawMessage `json:"raft_status"`
			IndexStatus int             `json:"index_status"`
			IndexNum    int             `json:"index_num"`
			MaxDocid    int             `json:"max_docid"`
		} `json:"partitions"`
		Status string `json:"status"`
	} `json:"data"`
}

type SpaceSchema struct {
	Name         string          `json:"name"`
	PartitionNum int             `json:"partition_num"`
	ReplicaNum   int             `json:"replica_num"`
	Engine       json.RawMessage `json:"engine"`
	Properties   json.RawMessage `json:"properties"`
}

type CreateDbBody struct {
	Name string `json:"name"`
}

type CreateDbResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"data"`
}

type CreateSpaceResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		ID           int    `json:"id"`
		Name         string `json:"name"`
		ResourceName string `json:"resource_name"`
		Version      int    `json:"version"`
		DbID         int    `json:"db_id"`
		Enabled      bool   `json:"enabled"`
		Partitions   []struct {
			ID                int   `json:"id"`
			SpaceID           int   `json:"space_id"`
			DbID              int   `json:"db_id"`
			PartitionSlot     int   `json:"partition_slot"`
			Replicas          []int `json:"replicas"`
			ResourceExhausted bool  `json:"resourceExhausted"`
		} `json:"partitions"`
		PartitionNum    int             `json:"partition_num"`
		ReplicaNum      int             `json:"replica_num"`
		Properties      json.RawMessage `json:"properties"`
		Engine          json.RawMessage `json:"engine"`
		SpaceProperties json.RawMessage `json:"space_properties"`
	} `json:"data"`
}
