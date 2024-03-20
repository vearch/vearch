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
	"strings"
	"unicode"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/vearch/vearch/internal/proto/vearchpb"
)

const (
	FieldType_INT vearchpb.FieldType = iota
	FieldType_LONG
	FieldType_FLOAT
	FieldType_DOUBLE
	FieldType_STRING
	FieldType_VECTOR
	FieldType_BOOL
	FieldType_GEOPOINT
	FieldType_DATE
)

const (
	FieldOption_Null        vearchpb.FieldOption = 0
	FieldOption_Index       vearchpb.FieldOption = 1
	FieldOption_Index_False vearchpb.FieldOption = 2
)

type Index struct {
	Name   string          `json:"name"`
	Type   string          `json:"type,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
}

func NewDefaultIndex() *Index {
	return &Index{}
}

var (
	MinNlinks                   = 8
	MaxNlinks                   = 96
	MinEfConstruction           = 16
	MaxEfConstruction           = 1024
	DefaultMetricType           = "InnerProduct"
	MinNcentroids               = 1
	MaxNcentroids               = 65536
	DefaultTrainingThreshold    = 0
	DefaultMaxPointsPerCentroid = 256
	DefaultMinPointsPerCentroid = 39
)

type IndexParams struct {
	Nlinks            int    `json:"nlinks,omitempty"`
	EfSearch          int    `json:"efSearch,omitempty"`
	EfConstruction    int    `json:"efConstruction,omitempty"`
	MetricType        string `json:"metric_type,omitempty"`
	Ncentroids        int    `json:"ncentroids,omitempty"`
	Nprobe            int    `json:"nprobe,omitempty"`
	Nsubvector        int    `json:"nsubvector,omitempty"`
	TrainingThreshold int    `json:"training_threshold,omitempty"`
}

// space/[dbId]/[spaceId]:[spaceBody]
type Space struct {
	Id              SpaceID                     `json:"id,omitempty"`
	Desc            string                      `json:"desc,omitempty"` //user setting
	Name            string                      `json:"name,omitempty"` //user setting
	ResourceName    string                      `toml:"resource_name,omitempty" json:"resource_name"`
	Version         Version                     `json:"version,omitempty"`
	DBId            DBID                        `json:"db_id,omitempty"`
	Enabled         *bool                       `json:"enabled"`    //Enabled flag whether the space can work
	Partitions      []*Partition                `json:"partitions"` // partitionids not sorted
	PartitionNum    int                         `json:"partition_num"`
	ReplicaNum      uint8                       `json:"replica_num"`
	Fields          json.RawMessage             `json:"fields"`
	Index           *Index                      `json:"index,omitempty"`
	SpaceProperties map[string]*SpaceProperties `json:"space_properties"`
}

type SpaceSchema struct {
	Fields json.RawMessage `json:"fields"`
	Index  *Index          `json:"index"`
}

type SpaceInfo struct {
	SpaceName    string           `json:"space_name"`
	DbName       string           `json:"db_name"`
	DocNum       uint64           `json:"doc_num"`
	PartitionNum int              `json:"partition_num"`
	ReplicaNum   uint8            `json:"replica_num"`
	Schema       *SpaceSchema     `json:"schema"`
	Status       string           `json:"status,omitempty"`
	Partitions   []*PartitionInfo `json:"partitions"`
	Errors       *[]string        `json:"errors,omitempty"`
}

type SpaceDescribeRequest struct {
	SpaceName string `json:"space_name"`
	DbName    string `json:"db_name"`
	Detail    *bool  `json:"detail"`
}

// cache/[dbId]/[spaceId]:[cacheCfg]
type EngineCfg struct {
	CacheModels []*CacheModel `json:"cache_models,omitempty"`
}

type CacheModel struct {
	Name      string `json:"name,omitempty"` //user setting
	CacheSize int32  `json:"cache_size,omitempty"`
}

type SpaceProperties struct {
	FieldType  vearchpb.FieldType   `json:"field_type"`
	Type       string               `json:"type"`
	Index      *Index               `json:"index,omitempty"`
	Format     *string              `json:"format,omitempty"`
	Dimension  int                  `json:"dimension,omitempty"`
	StoreType  *string              `json:"store_type,omitempty"`
	StoreParam json.RawMessage      `json:"store_param,omitempty"`
	Array      bool                 `json:"array,omitempty"`
	Option     vearchpb.FieldOption `json:"option,omitempty"`
}

func (s *Space) String() string {
	return fmt.Sprintf("%d_%s_%d_%d_%d_%d",
		s.Id, s.Name, s.Version, s.DBId, s.PartitionNum, s.ReplicaNum)
}

func (s *Space) GetPartition(id PartitionID) *Partition {
	for _, p := range s.Partitions {
		if p.Id == id {
			return p
		}
	}
	return nil
}

func (s *Space) PartitionId(slotID SlotID) PartitionID {
	if len(s.Partitions) == 1 {
		return s.Partitions[0].Id
	}

	arr := s.Partitions

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

func (index *Index) UnmarshalJSON(bs []byte) error {
	if len(bs) == 0 {
		return fmt.Errorf("space Index json.Unmarshal err: empty json")
	}
	tempIndex := &struct {
		IndexName   string          `json:"name,omitempty"`
		IndexType   string          `json:"type,omitempty"`
		IndexParams json.RawMessage `json:"params,omitempty"`
	}{}
	if err := json.Unmarshal(bs, tempIndex); err != nil {
		return fmt.Errorf("space Index json.Unmarshal err:%v", err)
	}

	indexTypeMap := map[string]string{"IVFPQ": "IVFPQ", "IVFFLAT": "IVFFLAT", "BINARYIVF": "BINARYIVF", "FLAT": "FLAT",
		"HNSW": "HNSW", "GPU": "GPU", "SSG": "SSG", "IVFPQ_RELAYOUT": "IVFPQ_RELAYOUT", "SCANN": "SCANN", "SCALAR": "SCALAR"}
	if tempIndex.IndexType == "" {
		return fmt.Errorf("IndexType is null")
	}
	_, have := indexTypeMap[tempIndex.IndexType]
	if !have {
		return fmt.Errorf("IndexType not support: %s", tempIndex.IndexType)
	}

	if tempIndex.IndexParams != nil && len(tempIndex.IndexParams) != 0 {
		var indexParams IndexParams
		if err := json.Unmarshal(tempIndex.IndexParams, &indexParams); err != nil {
			return fmt.Errorf("IndexParams:%s json.Unmarshal err :[%s]", tempIndex.IndexParams, err.Error())
		}
		if strings.Compare(indexParams.MetricType, "InnerProduct") != 0 &&
			strings.Compare(indexParams.MetricType, "L2") != 0 {
			return fmt.Errorf("IndexParams metric_type not support: %s, should be L2 or InnerProduct", indexParams.MetricType)
		}

		if strings.Compare(tempIndex.IndexType, "HNSW") == 0 {
			if indexParams.Nlinks != 0 {
				if indexParams.Nlinks < MinNlinks || indexParams.Nlinks > MaxNlinks {
					return fmt.Errorf("IndexParams nlinks:%d should in [%d, %d]", indexParams.Nlinks, MinNlinks, MaxNlinks)
				}
			}
			if indexParams.EfConstruction != 0 {
				if indexParams.EfConstruction < MinEfConstruction || indexParams.EfConstruction > MaxEfConstruction {
					return fmt.Errorf("IndexParams efConstruction:%d should in [%d, %d]", indexParams.EfConstruction, MinEfConstruction, MaxEfConstruction)
				}
			}
		} else if strings.Compare(tempIndex.IndexType, "FLAT") == 0 {

		} else if strings.Compare("BINARYIVF", tempIndex.IndexType) == 0 ||
			strings.Compare("IVFFLAT", tempIndex.IndexType) == 0 ||
			strings.Compare("IVFPQ", tempIndex.IndexType) == 0 ||
			strings.Compare("GPU", tempIndex.IndexType) == 0 {
			if indexParams.Ncentroids != 0 {
				if indexParams.Ncentroids < MinNcentroids || indexParams.Ncentroids > MaxNcentroids {
					return fmt.Errorf("IndexParams ncentroids:%d should in [%d, %d]", indexParams.Ncentroids, MinNcentroids, MaxNcentroids)
				}
			}

			if indexParams.TrainingThreshold != 0 {
				if indexParams.TrainingThreshold < indexParams.Ncentroids {
					return fmt.Errorf(tempIndex.IndexType+" training_threshold:[%d] should more than ncentroids:[%d]", indexParams.TrainingThreshold, indexParams.Ncentroids)
				}
				if indexParams.TrainingThreshold > DefaultMaxPointsPerCentroid*indexParams.Ncentroids {
					return fmt.Errorf(tempIndex.IndexType+" training_threshold:[%d] should less than DefaultMaxPointsPerCentroid(%d) * ncentroids(%d):[%d] so can not to index",
						indexParams.TrainingThreshold, DefaultMaxPointsPerCentroid, indexParams.Ncentroids, DefaultMaxPointsPerCentroid*indexParams.Ncentroids)
				}
			}
			if indexParams.Nprobe != 0 && indexParams.Nprobe > indexParams.Ncentroids {
				return fmt.Errorf(tempIndex.IndexType+" nprobe:[%d] should less than ncentroids:[%d]", indexParams.Nprobe, indexParams.Ncentroids)
			}
		}
	}

	*index = Index{
		Name:   tempIndex.IndexName,
		Type:   tempIndex.IndexType,
		Params: tempIndex.IndexParams,
	}

	return nil
}

// check params is ok
func (space *Space) Validate() error {
	rs := []rune(space.Name)

	if len(rs) == 0 {
		return fmt.Errorf("space name can not set empty string")
	}

	if unicode.IsNumber(rs[0]) {
		return fmt.Errorf("space name : %s can not start with num", space.Name)
	}

	if rs[0] == '_' {
		return fmt.Errorf("space name : %s can not start with _", space.Name)
	}

	for _, r := range rs {
		switch r {
		case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '\\', '+', '-', '!', '*', '/', '(', ')', ':', '^', '[', ']', '"', '{', '}', '~', '%', '&', '\'', '<', '>', '?':
			return fmt.Errorf("character '%c' can not in space name[%s]", r, space.Name)
		}
	}

	return nil
}

type Field struct {
	Name       string  `json:"name"`
	Type       string  `json:"type"`
	Array      *bool   `json:"array,omitempty"`
	Dimension  int     `json:"dimension,omitempty"`
	StoreType  *string `json:"store_type,omitempty"`
	Format     *string `json:"format,omitempty"`
	Index      *Index  `json:"index,omitempty"`
	StoreParam *struct {
		CacheSize int `json:"cache_size,omitempty"`
	} `json:"store_param,omitempty"`
}

func UnmarshalPropertyJSON(propertity []byte) (map[string]*SpaceProperties, error) {
	tmpPro := make(map[string]*SpaceProperties)
	tmp := make([]Field, 0)
	err := json.Unmarshal(propertity, &tmp)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	for _, data := range tmp {
		sp := &SpaceProperties{}
		isVector := false
		sp.Type = data.Type

		switch sp.Type {
		case "text", "keyword", "string":
			sp.FieldType = FieldType_STRING
		case "date":
			sp.FieldType = FieldType_DATE
		case "integer", "short", "byte":
			sp.FieldType = FieldType_INT
		case "long":
			sp.FieldType = FieldType_LONG
		case "float":
			sp.FieldType = FieldType_FLOAT
		case "double":
			sp.FieldType = FieldType_DOUBLE
		case "boolean", "bool":
			sp.FieldType = FieldType_BOOL
		case "vector":
			sp.FieldType = FieldType_VECTOR

			isVector = true
			if data.Dimension == 0 {
				return nil, fmt.Errorf("dimension can not be zero by field : [%s] ", data.Name)
			}
			sp.Dimension = data.Dimension

			if data.StoreType != nil && *data.StoreType != "" {
				if *data.StoreType != "RocksDB" && *data.StoreType != "MemoryOnly" {
					return nil, fmt.Errorf("vector field:[%s] not support this store type:[%s] it only RocksDB or MemoryOnly", data.Name, *sp.StoreType)
				}
			}
			sp.StoreType = data.StoreType
			sp.Format = data.Format
			format := data.Format
			if format != nil && *format != "" && !(strings.Compare(*format, "normalization") == 0 ||
				strings.Compare(*format, "normal") == 0 || strings.Compare(*format, "no") == 0) {
				return nil, fmt.Errorf("unknow vector process method:[%s]", *format)
			}

		default:
			return nil, fmt.Errorf("space property invalid field type")
		}
		sp.Index = data.Index
		sp.Array = false
		if data.Array != nil {
			sp.Array = *data.Array
		}

		if sp.Index != nil {
			sp.Option = FieldOption_Index
		} else {
			sp.Option = FieldOption_Null
		}

		if isVector {
			sp.Index = data.Index
		}

		// set date format
		if sp.Format != nil {
			if !(sp.FieldType == FieldType_DATE || sp.FieldType == FieldType_VECTOR) {
				return nil, fmt.Errorf("type:[%d] can not set format", sp.FieldType)
			}
		}

		tmpPro[data.Name] = sp
	}
	return tmpPro, nil
}
