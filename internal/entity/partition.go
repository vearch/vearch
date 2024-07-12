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
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

type PartitionStatus uint8

const (
	PA_UNKNOW PartitionStatus = iota
	PA_INVALID
	PA_CLOSED
	PA_READONLY
	PA_READWRITE
)

const (
	ReplicasOK       = 1
	ReplicasNotReady = 2
)

type PartitionForSearch struct {
	*Partition
	DBName, SpaceName string
}

// partition/[id]:[body]
type Partition struct {
	Id                PartitionID `json:"id,omitempty"`
	Name              string      `json:"name,omitempty"`
	SpaceId           SpaceID     `json:"space_id,omitempty"`
	DBId              DBID        `json:"db_id,omitempty"`
	Slot              SlotID      `json:"partition_slot"` // Slot stores the lower limit of the slot range
	LeaderID          NodeID      `json:"leader_name,omitempty"`
	Replicas          []NodeID    `json:"replicas,omitempty"` // leader in replicas
	UpdateTime        int64       `json:"update_time,omitempty"`
	AddNum            int64       `json:"add_num,omitempty"`
	ResourceExhausted bool        `json:"resourceExhausted"`
	Path              string      `json:"-"`
	status            PartitionStatus
	lock              sync.RWMutex
	ReStatusMap       map[uint64]uint32 `json:"status,omitempty"` // leader in replicas
}

// this is safe method for set status
func (p *Partition) SetStatus(s PartitionStatus) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.status = s
}

// this is safe method for get status
func (p *Partition) GetStatus() PartitionStatus {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.status
}

// get partition from every partitions
type PartitionInfo struct {
	PartitionID  PartitionID       `json:"pid,omitempty"`
	Name         string            `json:"name"`
	DocNum       uint64            `json:"doc_num"`
	Size         int64             `json:"size,omitempty"`
	ReplicaNum   int               `json:"replica_num,omitempty"`
	RepStatus    map[NodeID]string `json:"replica_status,omitempty"`
	Path         string            `json:"path,omitempty"`
	Unreachable  []uint64          `json:"unreachable,omitempty"`
	Status       PartitionStatus   `json:"status,omitempty"`
	Color        string            `json:"color,omitempty"`
	Ip           string            `json:"ip,omitempty"`
	NodeID       uint64            `json:"node_id,omitempty"`
	RaftStatus   *raft.Status      `json:"raft_status,omitempty"`
	IndexStatus  int               `json:"index_status"`
	BackupStatus int               `json:"backup_status"`
	IndexNum     int               `json:"index_num"`
	MaxDocid     int               `json:"max_docid"`
	Error        string            `json:"error,omitempty"`
}

type ResourceLimit struct {
	Rate              *float64 `json:"rate,omitempty"`
	ResourceExhausted *bool    `json:"resource_exhausted,omitempty"`
	SpaceName         *string  `json:"space_name,omitempty"`
	DbName            *string  `json:"db_name,omitempty"`
}

type PartitionType string

const (
	Drop string = "DROP"
	Add  string = "ADD"
)

const (
	RangePartition     PartitionType = "RANGE"
	HashPartition      PartitionType = "HASH"
	ListPartition      PartitionType = "LIST"
	KeyPartition       PartitionType = "KEY"
	CompositePartition PartitionType = "COMPOSITE"
)

type PartitionRule struct {
	Type       PartitionType `json:"type"`
	Field      string        `json:"field,omitempty"`
	Partitions int           `json:"partitions,omitempty"`
	Ranges     []Range       `json:"ranges,omitempty"`
	// Lists      []List        `json:"lists,omitempty"`
	// SubPartition *PartitionRule `json:"sub_partition,omitempty"`
}

func (pr *PartitionRule) Validate(space *Space, check_field bool) error {
	if check_field {
		if space.PartitionRule.Field == "" {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space partition rule field is empty"))
		}
		if value, exist := space.SpaceProperties[space.PartitionRule.Field]; !exist {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("%s not in space fields", space.PartitionRule.Field))
		} else {
			if value.FieldType != vearchpb.FieldType_DATE {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("currently only partition field with %s data type are supported", vearchpb.FieldType_DATE.String()))
			}
		}
	}
	return pr.ValidateRange(space)
}

func (pr *PartitionRule) ValidateRange(space *Space) error {
	if pr.Type != RangePartition {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("now only support partition type of RANGE"))
	}

	space.PartitionRule.Partitions = len(space.PartitionRule.Ranges)
	if space.PartitionRule.Partitions == 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("empty space partition rule of ranges"))
	}
	if space.PartitionRule.Partitions > MaxPartitions {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space partitions[%d] beyond MaxPartitions[%d]",
			space.PartitionRule.Partitions, MaxPartitions))
	}
	if space.PartitionRule.Partitions*space.PartitionNum > MaxTotalPartitions {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space total partitions[%d] beyond MaxTotalPartitions[%d]",
			space.PartitionRule.Partitions*space.PartitionNum, MaxTotalPartitions))
	}
	// check name and value，value should be increase
	var before = int64(-1)
	nameMap := make(map[string]bool)
	valueMap := make(map[string]bool)

	for _, r := range space.PartitionRule.Ranges {
		if _, nameExists := nameMap[r.Name]; nameExists {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space partition rule range name %s has same one, ranges %v", r.Name, space.PartitionRule.Ranges))
		}
		if _, valueExists := valueMap[r.Value]; valueExists {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space partition rule range value %s has same one, ranges %v", r.Value, space.PartitionRule.Ranges))
		}
		nameMap[r.Name] = true
		valueMap[r.Value] = true

		if r.Value != "" {
			timeNum, err := ToTimestamp(r.Value)
			if err != nil {
				return err
			}
			if timeNum <= before {
				log.Error("r.Value %s, timeNum: %d, before: %d", r.Value, timeNum, before)
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("range partition value needs to be increasing %v", space.PartitionRule.Ranges))
			}
			before = timeNum
		} else {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("range partition %s value is empty", r.Name))
		}
	}
	return nil
}

func ToTimestamp(value string) (int64, error) {
	i, err := cast.ToInt64E(value)
	if err != nil {
		f, err := cast.ToTimeE(value)
		if err != nil {
			return 0, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", value, err.Error()))
		}
		return f.UnixNano(), nil
	}
	return i * 1e9, nil
}

func insertIntoRanges(arr []Range, r Range) ([]Range, error) {
	ts, err := ToTimestamp(r.Value)
	if err != nil {
		return nil, err
	}
	left, right := 0, len(arr)
	for left < right {
		mid := (left + right) / 2
		midValue, err := ToTimestamp(arr[mid].Value)
		if err != nil {
			return nil, err
		}
		if midValue < ts {
			left = mid + 1
		} else {
			right = mid
		}
	}

	arr = append(arr, Range{})
	copy(arr[left+1:], arr[left:])
	arr[left] = r

	return arr, nil
}

func (pr *PartitionRule) AddRanges(ranges []Range) ([]Range, error) {
	// add as increasing
	new_ranges := pr.Ranges
	for _, r := range ranges {
		arr, err := insertIntoRanges(new_ranges, r)
		if err != nil {
			return nil, err
		}
		new_ranges = arr
	}
	return new_ranges, nil
}

func (pr *PartitionRule) RangeIsSame(ranges []Range) (bool, error) {
	nameMap := make(map[string]bool)
	valueMap := make(map[string]bool)
	for _, r := range pr.Ranges {
		if _, nameExists := nameMap[r.Name]; nameExists {
			return true, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space partition rule range name %s has same one, ranges %v", r.Name, pr.Ranges))
		}
		if _, valueExists := valueMap[r.Value]; valueExists {
			return true, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space partition rule range value %s has same one, ranges %v", r.Value, pr.Ranges))
		}
		nameMap[r.Name] = true
		valueMap[r.Value] = true
	}

	for _, r := range ranges {
		if _, nameExists := nameMap[r.Name]; nameExists {
			return true, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space partition rule range name %s has same one, space ranges %v, add ranges %v", r.Name, pr.Ranges, ranges))
		}
		if _, valueExists := valueMap[r.Value]; valueExists {
			return true, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space partition rule range value %s has same one, space ranges %v, add ranges %v", r.Value, pr.Ranges, ranges))
		}
	}
	return false, nil
}

type Range struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type List struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}
