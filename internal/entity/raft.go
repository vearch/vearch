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

import (
	"encoding/json"
	"fmt"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
)

// it use for raft add or remove node
type Replica struct {
	NodeID        NodeID `json:"nodeID,omitempty"`
	HeartbeatAddr string `json:"heartbeat_addr,omitempty"`
	ReplicateAddr string `json:"replicate_addr,omitempty"`
	RpcAddr       string `json:"rpc_addr,omitempty"`
}

func (m Replica) String() string {
	return fmt.Sprintf("Replica{NodeID:%v, HeartbeatAddr:%v, ReplicateAddr:%v, RpcAddr:%v}",
		m.NodeID, m.HeartbeatAddr, m.ReplicateAddr, m.RpcAddr)
}

func (m *Replica) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Replica) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

type FlushEntity struct {
	F      func() error
	FlushC chan error
}

type ChangeMember struct {
	PartitionID PartitionID          `json:"partition_id"`
	NodeID      NodeID               `json:"node_id"`
	Method      proto.ConfChangeType `json:"method"`
}

type ChangeMembers struct {
	PartitionIDs []PartitionID        `json:"partition_ids"`
	NodeID       NodeID               `json:"node_id"`
	Method       proto.ConfChangeType `json:"method"`
}

// RecoverFailServer use for recover fail server
type RecoverFailServer struct {
	FailNodeID   NodeID `json:"fail_node_id"`
	NewNodeID    NodeID `json:"new_node_id"`
	FailNodeAddr string `json:"fail_node_addr"`
	NewNodeAddr  string `json:"new_node_addr"`
}

type DBModify struct {
	DbName    string               `json:"db_name"`
	SpaceName string               `json:"space_name"`
	IPAddr    string               `json:"ip_addr"`
	Method    proto.ConfChangeType `json:"method"`
}
