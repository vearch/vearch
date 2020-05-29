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
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/vearchlog"
	"unicode"
)

const (
	Gamma = "gamma"
)

type Engine struct {
	Name           string          `json:"name"`
	IndexSize      int64           `json:"index_size"`
	MaxSize        int64           `json:"max_size"`
	MetricType     string          `json:"metric_type,omitempty"`
	RetrievalType  string          `json:"retrieval_type,omitempty"`
	RetrievalParam json.RawMessage `json:"retrieval_param,omitempty"`
	IdType         string          `json:"id_type,omitempty"`
}

func NewDefaultEngine() *Engine {
	return &Engine{
		Name: Gamma,
	}
}

type RetrievalParam struct {
	Nlinks         int    `json:"nlinks"`
	EfSearch       int    `json:"efSearch"`
	EfConstruction int    `json:"efConstruction"`
	MetricType     string `json:"metric_type,omitempty"`
	Ncentroids     int    `json:"ncentroids"`
	Nprobe         int    `json:"nprobe"`
	Nsubvector     int    `json:"nsubvector"`
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
	Models       json.RawMessage `json:"models,omitempty"` //json model config for python plugin

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
		if len(this.Partitions) == 1 {
			return this.Partitions[0].Id
		}

		arr := this.Partitions

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
				return arr[mid].Id
			}
		}

		return arr[low-1].Id
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
		Name           string          `json:"name"`
		IndexSize      *int64          `json:"index_size"`
		MaxSize        int64           `json:"max_size"`
		RetrievalParam json.RawMessage `json:"retrieval_param,omitempty"`
		MetricType     string          `json:"metric_type,omitempty"`
		RetrievalType  string          `json:"retrieval_type,omitempty"`
		IdType         string          `json:"id_type,omitempty"`
	}{}

	if err := json.Unmarshal(bs, tempEngine); err != nil {
		return err
	}
	switch tempEngine.Name {
	case Gamma:
		if tempEngine.MaxSize <= 0 {
			tempEngine.MaxSize = 100000
		}

		if tempEngine.RetrievalParam != nil {
			if tempEngine.RetrievalType == "" {
				return fmt.Errorf("retrieval_type is not null")
			}

			var v RetrievalParam
			if err := json.Unmarshal(tempEngine.RetrievalParam, &v); err != nil {
				fmt.Errorf("engine UnmarshalJSON RetrievalParam json.Unmarshal err :[%s]", err.Error())
			}

			if tempEngine.RetrievalType == "HNSW" {
				if v.Nlinks == 0 || v.EfSearch == 0 || v.EfConstruction == 0 {
					return fmt.Errorf("HNSW model param is 0")
				}
				if tempEngine.IndexSize == nil || *tempEngine.IndexSize <= 0 {
					tempEngine.IndexSize = util.PInt64(2)
				}
			} else if tempEngine.RetrievalType == "FLAT" {

			} else if tempEngine.RetrievalType == "BINARYIVF" {
				if v.Nprobe == 0 || v.Ncentroids == 0 || v.Ncentroids == 0 {
					return fmt.Errorf(tempEngine.RetrievalType + " model param is 0")
				} else {
					if tempEngine.IndexSize == nil || *tempEngine.IndexSize <= 0 {
						tempEngine.IndexSize = util.PInt64(100000)
					}
				}
				if *tempEngine.IndexSize < 8192 {
					return fmt.Errorf(tempEngine.RetrievalType+" model doc size:[%d] less than 8192 so can not to index", int64(*tempEngine.IndexSize))
				}
			} else {
				if v.Nsubvector == 0 || v.Ncentroids == 0 {
					return fmt.Errorf(tempEngine.RetrievalType + " model param is 0")
				} else {
					if tempEngine.IndexSize == nil || *tempEngine.IndexSize <= 0 {
						tempEngine.IndexSize = util.PInt64(100000)
					}
				}
				if *tempEngine.IndexSize < 8192 {
					return fmt.Errorf(tempEngine.RetrievalType+" model doc size:[%d] less than 8192 so can not to index", int64(*tempEngine.IndexSize))
				}
			}

			if v.MetricType == "" {
				return fmt.Errorf("metric_type is null")
			}

			tempEngine.MetricType = v.MetricType

		} else {
			if tempEngine.IndexSize == nil {
				tempEngine.IndexSize = util.PInt64(100000)
			}
		}
	default:
		return vearchlog.LogErrAndReturn(pkg.CodeErr(pkg.ERRCODE_PARTITON_ENGINENAME_INVALID))
	}

	*engine = Engine{
		Name:           tempEngine.Name,
		IndexSize:      *tempEngine.IndexSize,
		MaxSize:        tempEngine.MaxSize,
		RetrievalParam: tempEngine.RetrievalParam,
		MetricType:     tempEngine.MetricType,
		RetrievalType:  tempEngine.RetrievalType,
		IdType:         tempEngine.IdType,
	}

	return nil
}

//check params is ok
func (space *Space) Validate() error {

	switch space.Engine.Name {
	case Gamma:
	default:
		return pkg.CodeErr(pkg.ERRCODE_INVALID_ENGINE)
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
