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
	"time"
	"unicode"

	"github.com/vearch/vearch/v3/internal/pkg/cbbytes"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
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
	MaxNcentroids               = 262144
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
	PartitionRule   *PartitionRule              `json:"partition_rule,omitempty"`
	SpaceProperties map[string]*SpaceProperties `json:"space_properties"`
}

type SpaceSchema struct {
	Fields json.RawMessage `json:"fields"`
	Index  *Index          `json:"index,omitempty"`
}

type SpaceInfo struct {
	SpaceName     string           `json:"space_name"`
	DbName        string           `json:"db_name"`
	DocNum        uint64           `json:"doc_num"`
	PartitionNum  int              `json:"partition_num"`
	ReplicaNum    uint8            `json:"replica_num"`
	Schema        *SpaceSchema     `json:"schema"`
	PartitionRule *PartitionRule   `json:"partition_rule,omitempty"`
	Status        string           `json:"status,omitempty"`
	Partitions    []*PartitionInfo `json:"partitions"`
	Errors        *[]string        `json:"errors,omitempty"`
}

type SpacePartitionResource struct {
	SpaceName             string         `json:"space_name"`
	DbName                string         `json:"db_name"`
	PartitionNum          int            `json:"partition_num,omitempty"`
	ReplicaNum            uint8          `json:"replica_num,omitempty"`
	PartitionRule         *PartitionRule `json:"partition_rule,omitempty"`
	PartitionName         string         `json:"partition_name,omitempty"`
	PartitionOperatorType string         `json:"operator_type,omitempty"`
}

type SpaceDescribeRequest struct {
	SpaceName string `json:"space_name"`
	DbName    string `json:"db_name"`
	Detail    *bool  `json:"detail"`
}

type BackupSpace struct {
	Command    string `json:"command,omitempty"`
	WitchShema bool   `json:"with_schema,omitempty"`
	Part       int    `json:"part"`
	S3Param    struct {
		BucketName string `json:"bucket_name"`
		EndPoint   string `json:"endpoint"`
		AccessKey  string `json:"access_key"`
		SecretKey  string `json:"secret_key"`
		UseSSL     bool   `json:"use_ssl"`
	} `json:"s3_param,omitempty"`
}

type SpaceProperties struct {
	FieldType  vearchpb.FieldType   `json:"field_type"`
	Type       string               `json:"type"`
	Index      *Index               `json:"index,omitempty"`
	Format     *string              `json:"format,omitempty"`
	Dimension  int                  `json:"dimension,omitempty"`
	StoreType  *string              `json:"store_type,omitempty"`
	StoreParam json.RawMessage      `json:"store_param,omitempty"`
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

func (s *Space) PartitionIdsByRangeField(value []byte, field_type vearchpb.FieldType) ([]PartitionID, error) {
	pids := make([]PartitionID, 0)
	if len(s.Partitions) == 1 {
		pids = append(pids, s.Partitions[0].Id)
		return pids, nil
	}

	arr := s.Partitions
	partition_name := ""
	ts := cbbytes.Bytes2Int(value)
	found := false
	for _, r := range s.PartitionRule.Ranges {
		value, _ := ToTimestamp(r.Value)
		if ts < value {
			partition_name = r.Name
			found = true
			break
		}
	}
	if !found {
		u := cbbytes.Bytes2Int(value)
		timeStr := time.Unix(u/1e9, u%1e9)
		return pids, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("can't set partition for field value %s, space ranges %v", timeStr, s.PartitionRule.Ranges))
	}
	for i := 0; i < len(arr); i++ {
		if arr[i].Name == partition_name {
			pids = append(pids, arr[i].Id)
		}
	}
	return pids, nil
}

func (index *Index) UnmarshalJSON(bs []byte) error {
	if len(bs) == 0 {
		return fmt.Errorf("space Index json.Unmarshal err: empty json")
	}
	tempIndex := &struct {
		Name   string          `json:"name,omitempty"`
		Type   string          `json:"type,omitempty"`
		Params json.RawMessage `json:"params,omitempty"`
	}{}
	if err := json.Unmarshal(bs, tempIndex); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space Index json.Unmarshal err:%v", err))
	}

	indexTypeMap := map[string]string{"IVFPQ": "IVFPQ", "IVFFLAT": "IVFFLAT", "BINARYIVF": "BINARYIVF", "FLAT": "FLAT",
		"HNSW": "HNSW", "GPU": "GPU", "SSG": "SSG", "IVFPQ_RELAYOUT": "IVFPQ_RELAYOUT", "SCANN": "SCANN", "SCALAR": "SCALAR"}
	if tempIndex.Type == "" {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index type is null"))
	}
	_, have := indexTypeMap[tempIndex.Type]
	if !have {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index type not support: %s", tempIndex.Type))
	}

	if tempIndex.Params != nil && len(tempIndex.Params) != 0 {
		var indexParams IndexParams
		if err := json.Unmarshal(tempIndex.Params, &indexParams); err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index params:%s json.Unmarshal err :[%s]", tempIndex.Params, err.Error()))
		}

		if indexParams.MetricType != "" && indexParams.MetricType != "InnerProduct" && indexParams.MetricType != "L2" {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index params metric_type not support: %s, should be L2 or InnerProduct", indexParams.MetricType))
		}

		if tempIndex.Type == "HNSW" {
			if indexParams.Nlinks != 0 {
				if indexParams.Nlinks < MinNlinks || indexParams.Nlinks > MaxNlinks {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index params nlinks:%d should in [%d, %d]", indexParams.Nlinks, MinNlinks, MaxNlinks))
				}
			}
			if indexParams.EfConstruction != 0 {
				if indexParams.EfConstruction < MinEfConstruction || indexParams.EfConstruction > MaxEfConstruction {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index params efConstruction:%d should in [%d, %d]", indexParams.EfConstruction, MinEfConstruction, MaxEfConstruction))
				}
			}
		} else if tempIndex.Type == "FLAT" {

		} else if tempIndex.Type == "BINARYIVF" || tempIndex.Type == "IVFFLAT" || tempIndex.Type == "IVFPQ" || tempIndex.Type == "GPU" {
			if indexParams.Ncentroids != 0 {
				if indexParams.Ncentroids < MinNcentroids || indexParams.Ncentroids > MaxNcentroids {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index params ncentroids:%d should in [%d, %d]", indexParams.Ncentroids, MinNcentroids, MaxNcentroids))
				}
			}

			if indexParams.TrainingThreshold != 0 {
				if indexParams.TrainingThreshold < indexParams.Ncentroids {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(tempIndex.Type+" training_threshold:[%d] should more than ncentroids:[%d]", indexParams.TrainingThreshold, indexParams.Ncentroids))
				}
			}
			if indexParams.Nprobe != 0 && indexParams.Nprobe > indexParams.Ncentroids {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(tempIndex.Type+" nprobe:[%d] should less than ncentroids:[%d]", indexParams.Nprobe, indexParams.Ncentroids))
			}
		}
	}

	*index = Index{
		Name:   tempIndex.Name,
		Type:   tempIndex.Type,
		Params: tempIndex.Params,
	}

	return nil
}

// check params is ok
func (space *Space) Validate() error {
	rs := []rune(space.Name)

	if len(rs) == 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space name can not set empty string"))
	}

	if unicode.IsNumber(rs[0]) {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space name : %s can not start with num", space.Name))
	}

	if rs[0] == '_' {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space name : %s can not start with _", space.Name))
	}

	for _, r := range rs {
		switch r {
		case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '\\', '+', '-', '!', '*', '/', '(', ')', ':', '^', '[', ']', '"', '{', '}', '~', '%', '&', '\'', '<', '>', '?':
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("character '%c' can not in space name[%s]", r, space.Name))
		}
	}

	return nil
}

type Field struct {
	Name       string  `json:"name"`
	Type       string  `json:"type"`
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
			sp.FieldType = vearchpb.FieldType_STRING
		case "date":
			sp.FieldType = vearchpb.FieldType_DATE
		case "integer", "short", "byte":
			sp.FieldType = vearchpb.FieldType_INT
		case "long":
			sp.FieldType = vearchpb.FieldType_LONG
		case "float":
			sp.FieldType = vearchpb.FieldType_FLOAT
		case "double":
			sp.FieldType = vearchpb.FieldType_DOUBLE
		case "boolean", "bool":
			sp.FieldType = vearchpb.FieldType_BOOL
		case "stringArray", "StringArray":
			sp.FieldType = vearchpb.FieldType_STRINGARRAY
		case "vector":
			sp.FieldType = vearchpb.FieldType_VECTOR

			isVector = true
			if data.Dimension == 0 {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("dimension can not be zero by field : [%s] ", data.Name))
			}
			sp.Dimension = data.Dimension

			if data.StoreType != nil && *data.StoreType != "" {
				if *data.StoreType != "RocksDB" && *data.StoreType != "MemoryOnly" {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("vector field:[%s] not support this store type:[%s] it only RocksDB or MemoryOnly", data.Name, *sp.StoreType))
				}
			}
			sp.StoreType = data.StoreType
			sp.Format = data.Format
			format := data.Format
			if format != nil && !(*format == "normalization" || *format == "normal" || *format == "no") {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unknow vector process method:[%s]", *format))
			}

		default:
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space invalid field type: %s", sp.Type))
		}
		sp.Index = data.Index

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
			if !(sp.FieldType == vearchpb.FieldType_DATE || sp.FieldType == vearchpb.FieldType_VECTOR) {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("type:[%d] can not set format", sp.FieldType))
			}
		}

		tmpPro[data.Name] = sp
	}
	return tmpPro, nil
}
