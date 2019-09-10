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
	"sync"
)

type PartitionStatus uint8

const (
	PA_UNKNOW PartitionStatus = iota
	PA_INVALID
	PA_CLOSED
	PA_READONLY
	PA_READWRITE
)

type PartitionForSearch struct {
	*Partition
	DBName, SpaceName string
}

//partition/[id]:[body]
type Partition struct {
	Id      PartitionID `json:"id,omitempty"`
	SpaceId SpaceID     `json:"space_id,omitempty"`
	DBId    DBID        `json:"db_id,omitempty"`
	//Slot stores the lower limit of the slot range
	Slot       SlotID   `json:"partition_slot"`
	LeaderID   NodeID   `json:"leader_name,omitempty"`
	Replicas   []NodeID `json:"replicas,omitempty"` //leader in replicas
	UpdateTime int64    `json:"update_time,omitempty"`
	Path       string   `json:"-"`

	//for GuiXu log config
	Frozen   bool  `json:"frozen,omitempty"`
	MinValue int64 `json:"min_value,omitempty"`
	MaxValue int64 `json:"max_value,omitempty"`

	status PartitionStatus
	lock   sync.RWMutex
}

//this is safe method for set status
func (p *Partition) SetStatus(s PartitionStatus) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.status = s
}

//this is safe method for get status
func (p *Partition) GetStatus() PartitionStatus {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status
}

//get partition from every partitions
type PartitionInfo struct {
	PartitionID PartitionID     `json:"pid,omitempty"`
	DocNum      uint64          `json:"doc_num,omitempty"`
	Size        int64           `json:"size,omitempty"`
	ReplicaNum  int             `json:"replica_num,omitempty"`
	Path        string          `json:"path,omitempty"`
	Unreachable []uint64        `json:"unreachable,omitempty"`
	Frozen      bool            `json:"frozen,omitempty"`
	MinValue    int64           `json:"min_value,omitempty"`
	MaxValue    int64           `json:"max_value,omitempty"`
	Status      PartitionStatus `json:"status,omitempty"`
	Color       string          `json:"color,omitempty"`
	Ip          string          `json:"ip,omitempty"`
	NodeID      uint64          `json:"node_id,omitempty"`
}
