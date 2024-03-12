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
	IndexName   string          `json:"index_name"`
	IndexParams json.RawMessage `json:"index_params,omitempty"`
	IndexType   string          `json:"index_type,omitempty"`
}

func NewDefaultIndex() *Index {
	return &Index{}
}

type IndexParams struct {
	Nlinks            int    `json:"nlinks"`
	EfSearch          int    `json:"efSearch,omitempty"`
	EfConstruction    int    `json:"efConstruction"`
	MetricType        string `json:"metric_type,omitempty"`
	Ncentroids        int    `json:"ncentroids"`
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
	Index           *Index                      `json:"index"`
	Models          json.RawMessage             `json:"models,omitempty"` //json model config for python plugin
	SpaceProperties map[string]*SpaceProperties `json:"space_properties"`
}

type SpaceSchema struct {
	Fields json.RawMessage `json:"fields"`
	Index  *Index          `json:"index"`
}

type SpaceInfo struct {
	SpaceName    string           `json:"space_name"`
	DbName       string           `json:"db_name"`
	PartitionNum int              `json:"partition_num"`
	ReplicaNum   uint8            `json:"replica_num"`
	Schema       *SpaceSchema     `json:"schema"`
	DocNum       uint64           `json:"doc_num"`
	Partitions   []*PartitionInfo `json:"partitions"`
	Status       string           `json:"status,omitempty"`
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
	Index      *bool                `json:"index,omitempty"`
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
		IndexName   string          `json:"index_name,omitempty"`
		IndexParams json.RawMessage `json:"index_params,omitempty"`
		IndexType   string          `json:"index_type,omitempty"`
	}{}
	if err := json.Unmarshal(bs, tempIndex); err != nil {
		return fmt.Errorf("space Index json.Unmarshal err:%v", err)
	}

	indexTypeMap := map[string]string{"IVFPQ": "IVFPQ", "IVFFLAT": "IVFFLAT", "BINARYIVF": "BINARYIVF", "FLAT": "FLAT",
		"HNSW": "HNSW", "GPU": "GPU", "SSG": "SSG", "IVFPQ_RELAYOUT": "IVFPQ_RELAYOUT", "SCANN": "SCANN"}
	if tempIndex.IndexType == "" {
		return fmt.Errorf("IndexType is null")
	}
	_, have := indexTypeMap[tempIndex.IndexType]
	if !have {
		return fmt.Errorf("IndexType not support :%s", tempIndex.IndexType)
	}

	var v IndexParams
	if tempIndex.IndexParams != nil && len(tempIndex.IndexParams) != 0 {
		if err := json.Unmarshal(tempIndex.IndexParams, &v); err != nil {
			return fmt.Errorf("IndexParams json.Unmarshal err :[%s]", err.Error())
		}

		if strings.Compare(tempIndex.IndexType, "HNSW") == 0 {
			if v.Nlinks == 0 || v.EfConstruction == 0 {
				return fmt.Errorf(tempIndex.IndexType + " index param has 0")
			}
		} else if strings.Compare(tempIndex.IndexType, "FLAT") == 0 {

		} else if strings.Compare("BINARYIVF", tempIndex.IndexType) == 0 ||
			strings.Compare("IVFFLAT", tempIndex.IndexType) == 0 {
			if v.Ncentroids == 0 {
				return fmt.Errorf(tempIndex.IndexType + " index param has 0")
			} else {
				if v.TrainingThreshold != 0 && int64(v.TrainingThreshold) < int64(v.Ncentroids) {
					return fmt.Errorf(tempIndex.IndexType+" training_threshold:[%d] less than ncentroids:[%d] so can not to index", int64(v.TrainingThreshold), v.Ncentroids)
				}
				if v.TrainingThreshold == 0 {
					v.TrainingThreshold = 39 * v.Ncentroids
				}
			}
		} else if strings.Compare("IVFPQ", tempIndex.IndexType) == 0 ||
			strings.Compare("GPU", tempIndex.IndexType) == 0 {
			if v.Nsubvector == 0 || v.Ncentroids == 0 {
				return fmt.Errorf(tempIndex.IndexType + " index param has 0")
			} else {
				if v.TrainingThreshold != 0 && int64(v.TrainingThreshold) < int64(v.Ncentroids) {
					return fmt.Errorf(tempIndex.IndexType+" training_threshold:[%d] less than ncentroids:[%d] so can not to index", int64(v.TrainingThreshold), v.Ncentroids)
				}
				if v.TrainingThreshold == 0 {
					v.TrainingThreshold = 39 * v.Ncentroids
				}
			}
		}
	} else {
		return fmt.Errorf("IndexParams is empty")
	}
	*index = Index{
		IndexName:   tempIndex.IndexName,
		IndexType:   tempIndex.IndexType,
		IndexParams: tempIndex.IndexParams,
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

func UnmarshalPropertyJSON(propertity []byte) (map[string]*SpaceProperties, error) {
	tmpPro := make(map[string]*SpaceProperties)
	tmp := make(map[string]json.RawMessage)
	err := json.Unmarshal([]byte(propertity), &tmp)
	if err != nil {
		fmt.Println(err)
	}

	for name, data := range tmp {
		sp := &SpaceProperties{}
		err = json.Unmarshal(data, sp)
		if err != nil {
			return nil, err
		}

		isVector := false

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
			if sp.Dimension == 0 {
				return nil, fmt.Errorf("dimension can not be zero by field : [%s] ", string(data))
			}

			if sp.StoreType != nil && *sp.StoreType != "" {
				if *sp.StoreType != "RocksDB" && *sp.StoreType != "MemoryOnly" {
					return nil, fmt.Errorf("vector field:[%s] not support this store type:[%s] it only RocksDB or MemoryOnly", name, *sp.StoreType)
				}
			}

			format := sp.Format
			if format != nil && *format != "" && !(strings.Compare(*format, "normalization") == 0 ||
				strings.Compare(*format, "normal") == 0 || strings.Compare(*format, "no") == 0) {
				return nil, fmt.Errorf("unknow vector process method:[%s]", *format)
			}

		default:
			return nil, fmt.Errorf("space property invalid field type")
		}

		if isVector {
			if sp.Index != nil {
				if *sp.Index {
					sp.Option = FieldOption_Index
				} else {
					sp.Option = FieldOption_Null
				}
			} else {
				sp.Option = FieldOption_Index
			}
		} else {
			if sp.Index != nil {
				if *sp.Index {
					sp.Option = FieldOption_Index
				} else {
					sp.Option = FieldOption_Null
				}
			} else {
				sp.Option = FieldOption_Null
			}
		}

		//set date format
		if sp.Format != nil {
			if !(sp.FieldType == FieldType_DATE || sp.FieldType == FieldType_VECTOR) {
				return nil, fmt.Errorf("type:[%d] can not set format", sp.FieldType)
			}
		}

		tmpPro[name] = sp
	}
	return tmpPro, nil
}
