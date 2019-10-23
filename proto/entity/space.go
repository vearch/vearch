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
	"errors"
	"fmt"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/util"
	"strings"
	"unicode"
)

type DynamicType string

func (dy *DynamicType) UnmarshalJSON(bs []byte) error {
	dynamicType := DynamicType(strings.Replace(string(bs), "\"", "", 2))
	*dy = dynamicType
	return nil
}

const (
	Gamma = "gamma"
)

type Engine struct {
	Name        string `json:"name"`
	IndexSize   int64  `json:"index_size"`
	MaxSize     int64  `json:"max_size"`
	Nprobe      *int   `json:"nprobe,omitempty"`
	MetricType  *int   `json:"metric_type,omitempty"`
	Ncentroids  *int   `json:"ncentroids,omitempty"`
	Nsubvector  *int   `json:"nsubvector,omitempty"`
	NbitsPerIdx *int   `json:"nbits_per_idx,omitempty"`
}

func NewDefaultEngine() *Engine {
	return &Engine{
		Name: Gamma,
	}
}

//space/[dbId]/[spaceId]:[spaceBody]
type Space struct {
	Id           SpaceID         `json:"id,omitempty"`
	Name         string          `json:"name,omitempty"` //user setting
	Version      Version         `json:"version,omitempty"`
	DBId         DBID            `json:"db_id,omitempty"`
	Enabled      *bool           `json:"enabled"`    //Enabled flag whether the space can work
	Partitions   []*Partition    `json:"partitions"` // partitionids not sorted
	PartitionNum int             `json:"partition_num"`
	ReplicaNum   uint8           `json:"replica_num"`
	Properties   json.RawMessage `json:"properties"`
	Engine       *Engine         `json:"engine"`

	DynamicSchema    DynamicType     `json:"dynamic_schema,omitempty"`    // has three types true , false , strict
	DefaultField     string          `json:"default_field"`               //default _all
	StoreDynamic     bool            `json:"store_dynamic"`               //default false
	StoreSource      *bool           `json:"store_source"`                //default true
	DocValuesDynamic *bool           `json:"docvalues_dynamic,omitempty"` //default true
	Models           json.RawMessage `json:"models,omitempty"`            //json model config for python plugin

	WorkedPartitions []*Partition `json:"worked_partitions"` // partitionids not sorted
}

func (this *Space) String() string {
	return fmt.Sprintf("%d_%s_%d_%d_%d_%d",
		this.Id, this.Name, this.Version, this.DBId, this.PartitionNum, this.ReplicaNum)
}

func (this *Space) GetPartition(id PartitionID) *Partition {
	for _, p := range this.Partitions {
		if p.Id == id {
			return p
		}
	}
	return nil
}

func (this *Space) PartitionId(slotID SlotID) PartitionID {
	switch this.Engine.Name {
	default:
		if len(this.WorkedPartitions) == 1 {
			return this.WorkedPartitions[0].Id
		}

		arr := this.WorkedPartitions

		maxKey := len(arr) - 1

		low, high, mid := 0, maxKey, 0

		for low <= high {
			mid = (low + high) >> 1

			midVal := arr[mid].Slot

			if midVal > slotID {
				high = mid - 1
			} else if midVal < slotID {
				low = mid + 1
			} else {
				return this.WorkedPartitions[mid].Id
			}
		}

		return this.WorkedPartitions[low-1].Id
	}

}

func (engine *Engine) UnmarshalJSON(bs []byte) error {

	var temp string

	_ = json.Unmarshal(bs, &temp)

	if temp == Gamma {
		*engine = Engine{Name: temp}
		return nil
	}

	tempEngine := &struct {
		Name         string `json:"name"`
		IndexSize    int64  `json:"index_size"`
		MaxSize      int64  `json:"max_size"`
		ZoneField    string ` json:"zone_field"`
		ExpireMinute int64  `json:"expire_minute"`
		Nprobe       *int   `json:"nprobe"`
		MetricType   *int   `json:"metric_type"`
		Ncentroids   *int   `json:"ncentroids"`
		Nsubvector   *int   `json:"nsubvector"`
		NbitsPerIdx  *int   `json:"nbits_per_idx"`
	}{}

	if err := json.Unmarshal(bs, tempEngine); err != nil {
		return err
	}

	switch tempEngine.Name {
	case Gamma:
		if tempEngine.MaxSize <= 0 {
			tempEngine.MaxSize = 10000000
		}

		defVal := util.PInt(-1)

		if tempEngine.Nprobe == nil {
			tempEngine.Nprobe = defVal
		}

		if tempEngine.MetricType == nil {
			tempEngine.MetricType = defVal
		}

		if tempEngine.Ncentroids == nil {
			tempEngine.Ncentroids = defVal
		}

		if tempEngine.Nsubvector == nil {
			tempEngine.Nsubvector = defVal
		}

		if tempEngine.NbitsPerIdx == nil {
			tempEngine.NbitsPerIdx = defVal
		}
	default:
		return pkg.ErrPartitionEngineNameInvalid
	}

	*engine = Engine{
		Name:        tempEngine.Name,
		IndexSize:   tempEngine.IndexSize,
		MaxSize:     tempEngine.MaxSize,
		Nprobe:      tempEngine.Nprobe,
		MetricType:  tempEngine.MetricType,
		Ncentroids:  tempEngine.Ncentroids,
		Nsubvector:  tempEngine.Nsubvector,
		NbitsPerIdx: tempEngine.NbitsPerIdx,
	}

	return nil
}

//check params is ok
func (space *Space) Validate() error {
	if space.DynamicSchema != "true" && space.DynamicSchema != "false" && space.DynamicSchema != "strict" {
		return fmt.Errorf("dynamic only support true , false or strict , but got [%s]", space.DynamicSchema)
	}

	switch space.Engine.Name {
	case Gamma:
	default:
		return errors.New(pkg.ErrMasterInvalidEngine.Error() + " engine name : " + space.Engine.Name)
	}

	rs := []rune(space.Name)

	if len(rs) == 0 {
		return fmt.Errorf("name can not set empty string")
	}

	if unicode.IsNumber(rs[0]) {
		return fmt.Errorf("name : %s can not start with num", space.Name)
	}

	if rs[0] == '_' {
		return fmt.Errorf("name : %s can not start with _", space.Name)
	}

	for _, r := range rs {
		switch r {
		case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '\\', '+', '-', '!', '*', '/', '(', ')', ':', '^', '[', ']', '"', '{', '}', '~', '%', '&', '\'', '<', '>', '?':
			return fmt.Errorf("name : %s can not has char in name ", `'\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0 , '\\','+', '-', '!', '*', '/', '(', ')', ':' , '^','[',']','"','{','}','~','%','&','\'','<','>','?'`)
		}
	}

	return nil
}
