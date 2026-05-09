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
	"slices"
	"time"
	"unicode"

	"github.com/vearch/vearch/v3/internal/pkg/cbbytes"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

const (
	IdField    = "_id"
	ScoreField = "_score"
)

type Index struct {
	Name       string          `json:"name"`
	Type       string          `json:"type,omitempty"`
	FieldName  string          `json:"field_name,omitempty"`  // for single-field indexes
	FieldNames []string        `json:"field_names,omitempty"` // for multi-field indexes
	Params     json.RawMessage `json:"params,omitempty"`
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
	MinTrainingThreshold        = 256
	DefaultMaxPointsPerCentroid = 256
	DefaultMinPointsPerCentroid = 39
	DefaultRefreshInterval      = 1000 // 1s
	DefaultEnableIdCache        = false
	DefalutEnableRealtime       = false
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
	Id                    SpaceID                     `json:"id,omitempty"`
	Desc                  string                      `json:"desc,omitempty"` // user setting
	Name                  string                      `json:"name,omitempty"` // user setting
	ResourceName          string                      `toml:"resource_name,omitempty" json:"resource_name"`
	Version               Version                     `json:"version,omitempty"`
	DBId                  DBID                        `json:"db_id,omitempty"`
	Enabled               *bool                       `json:"enabled"`    // Enabled flag whether the space can work
	Partitions            []*Partition                `json:"partitions"` // partitionids not sorted
	PartitionNum          int                         `json:"partition_num"`
	ReplicaNum            uint8                       `json:"replica_num"`
	Fields                json.RawMessage             `json:"fields"`
	Indexes               []*Index                    `json:"indexes,omitempty"`
	PartitionRule         *PartitionRule              `json:"partition_rule,omitempty"`
	SpaceProperties       map[string]*SpaceProperties `json:"space_properties,omitempty"`
	RefreshInterval       *int32                      `json:"refresh_interval,omitempty"`
	PartitionName         *string                     `json:"partition_name,omitempty"`  // partition name for partition rule
	PartitionOperatorType *string                     `json:"operator_type,omitempty"`   // partition rule operator type
	EnableIdCache         *bool                       `json:"enable_id_cache,omitempty"` // whether enable map docid to _id value in cache
	EnableRealtime        *bool                       `json:"enable_realtime,omitempty"` // whether enable realtime search
}

// TODO separete space config and mapping
// space_config/[dbId]/[spaceId]:[spaceConfigBody]
type SpaceConfig struct {
	Id              SpaceID `json:"id,omitempty"`
	DBId            DBID    `json:"db_id,omitempty"`
	EngineCacheSize *int64  `json:"engine_cache_size,omitempty"`
	Path            *string `json:"path,omitempty"`
	SlowSearchTime  *int64  `json:"slow_search_time,omitempty"` //previous name "long_search_time"
	RefreshInterval *int32  `json:"refresh_interval,omitempty"`
	EnableIdCache   *bool   `json:"enable_id_cache,omitempty"`
}

type SpaceSchema struct {
	Fields  json.RawMessage `json:"fields"`
	Indexes []*Index        `json:"indexes,omitempty"`
}

type SpaceInfo struct {
	SpaceName     string           `json:"space_name,omitempty"`
	Name          string           `json:"name,omitempty"` // for compitable with old version before v3.5.5, cluster health api use it
	DbName        string           `json:"db_name"`
	DocNum        uint64           `json:"doc_num"`
	PartitionNum  int              `json:"partition_num"`
	ReplicaNum    uint8            `json:"replica_num"`
	Schema        *SpaceSchema     `json:"schema"`
	PartitionRule *PartitionRule   `json:"partition_rule,omitempty"`
	Status        string           `json:"status,omitempty"`
	Partitions    []*PartitionInfo `json:"partitions"`
	Errors        []string         `json:"errors,omitempty"`
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

func (s *Space) GetFieldIndexType(fieldName string) string {
	for _, idx := range s.Indexes {
		if idx.FieldName == fieldName {
			return idx.Type
		}
	}
	for field, pro := range s.SpaceProperties {
		if field == fieldName {
			if pro.Index != nil {
				return pro.Index.Type
			}
			return ""
		}
	}
	return ""
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
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space vector field index should not be empty"))
	}
	tempIndex := &struct {
		Name       string          `json:"name,omitempty"`
		Type       string          `json:"type,omitempty"`
		FieldName  string          `json:"field_name,omitempty"`
		FieldNames []string        `json:"field_names,omitempty"`
		Params     json.RawMessage `json:"params,omitempty"`
	}{}
	if err := json.Unmarshal(bs, tempIndex); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space Index json.Unmarshal err:%v", err))
	}

	indexTypeMap := map[string]string{
		"IVFPQ":          "IVFPQ",
		"IVFFLAT":        "IVFFLAT",
		"BINARYIVF":      "BINARYIVF",
		"FLAT":           "FLAT",
		"HNSW":           "HNSW",
		"GPU_IVFPQ":      "GPU_IVFPQ",
		"GPU_IVFFLAT":    "GPU_IVFFLAT",
		"SSG":            "SSG",
		"IVFPQ_RELAYOUT": "IVFPQ_RELAYOUT",
		"SCANN":          "SCANN",
		"SCALAR":         "SCALAR",
		"IVFRABITQ":      "IVFRABITQ",
		"INVERTED":       "INVERTED",
		"BITMAP":         "BITMAP",
		"COMPOSITE":      "COMPOSITE",
	}

	if tempIndex.Type == "" {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index type is null"))
	}
	_, have := indexTypeMap[tempIndex.Type]
	if !have {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index type not support: %s", tempIndex.Type))
	}

	if len(tempIndex.Params) != 0 {
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

		} else if slices.Contains([]string{"BINARYIVF", "IVFFLAT", "IVFPQ", "GPU_IVFPQ", "GPU_IVFFLAT", "IVFRABITQ"}, tempIndex.Type) {
			if indexParams.Ncentroids != 0 {
				if indexParams.Ncentroids < MinNcentroids || indexParams.Ncentroids > MaxNcentroids {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index params ncentroids:%d should in [%d, %d]", indexParams.Ncentroids, MinNcentroids, MaxNcentroids))
				}
			}

			if indexParams.TrainingThreshold != 0 {
				if indexParams.TrainingThreshold < indexParams.Ncentroids {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(tempIndex.Type+" training_threshold:[%d] should more than ncentroids:[%d]", indexParams.TrainingThreshold, indexParams.Ncentroids))
				}
				if indexParams.TrainingThreshold < MinTrainingThreshold {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(tempIndex.Type+" training_threshold:[%d] should more than [%d]", indexParams.TrainingThreshold, MinTrainingThreshold))
				}
			} else {
				if indexParams.Ncentroids != 0 && indexParams.Ncentroids*DefaultMinPointsPerCentroid < MinTrainingThreshold {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(tempIndex.Type+" training_threshold:[%d] should more than [%d]", indexParams.TrainingThreshold, MinTrainingThreshold))
				}
			}
			if indexParams.Nprobe != 0 && indexParams.Nprobe > indexParams.Ncentroids {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(tempIndex.Type+" nprobe:[%d] should less than ncentroids:[%d]", indexParams.Nprobe, indexParams.Ncentroids))
			}
		}
	}

	*index = Index{
		Name:       tempIndex.Name,
		Type:       tempIndex.Type,
		FieldName:  tempIndex.FieldName,
		FieldNames: tempIndex.FieldNames,
		Params:     tempIndex.Params,
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
	fields := make([]Field, 0)
	err := json.Unmarshal(propertity, &fields)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	names := make(map[string]bool)
	indexNames := make(map[string]bool)

	for _, field := range fields {
		if field.Name == "" {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field name can not be empty"))
		}
		if field.Name == IdField {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field name can not be %s", IdField))
		}
		if field.Name == ScoreField {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field name can not be %s", ScoreField))
		}
		if _, ok := names[field.Name]; ok {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field name can not be duplicate"))
		} else {
			names[field.Name] = true
		}
		if field.Index != nil {
			if _, ok := indexNames[field.Index.Name]; ok {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index name can not be duplicate"))
			} else {
				indexNames[field.Index.Name] = true
			}
		}

		sp := &SpaceProperties{}
		isVector := false
		sp.Type = field.Type

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
		case "stringArray", "StringArray", "string_array":
			sp.FieldType = vearchpb.FieldType_STRINGARRAY
		case "vector":
			sp.FieldType = vearchpb.FieldType_VECTOR

			isVector = true
			if field.Dimension == 0 {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("dimension can not be zero by field : [%s] ", field.Name))
			}
			sp.Dimension = field.Dimension

			if field.StoreType != nil && *field.StoreType != "" {
				if *field.StoreType != "RocksDB" && *field.StoreType != "MemoryOnly" {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("vector field:[%s] not support this store type:[%s] it only RocksDB or MemoryOnly", field.Name, *sp.StoreType))
				}
			}
			sp.StoreType = field.StoreType
			sp.Format = field.Format
			format := field.Format
			if format != nil && !(*format == "normalization" || *format == "normal" || *format == "no") {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unknow vector process method:[%s]", *format))
			}

		default:
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space invalid field type: %s", sp.Type))
		}
		sp.Index = field.Index

		// Scalar index types are only allowed for non-Vector (scalar) fields.
		// Vector fields must use vector index types (IVFPQ, HNSW, FLAT, etc.).
		if sp.Index != nil {
			if isVector {
				// Vector fields: reject scalar index types
				if IsScalarIndexType(sp.Index.Type) {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
						fmt.Errorf("field [%s] is a vector field, scalar index type [%s] is not allowed",
							field.Name, sp.Index.Type))
				}
				// Vector fields: set Option to Index (vector index type is handled elsewhere)
				sp.Option = vearchpb.FieldOption_Index
			} else {
				// Scalar fields: reject vector index types
				if !IsScalarIndexType(sp.Index.Type) {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
						fmt.Errorf("field [%s] is a scalar field, index type [%s] is not allowed",
							field.Name, sp.Index.Type))
				}
				switch sp.Index.Type {
				case ScalarIndexType:
					sp.Option = vearchpb.FieldOption_Scalar
				case InvertedIndexType:
					sp.Option = vearchpb.FieldOption_Inverted
				case InvertedListIndexType:
					sp.Option = vearchpb.FieldOption_InvertedList
				case BitmapIndexType:
					sp.Option = vearchpb.FieldOption_Bitmap
				case CompositeIndexType:
					sp.Option = vearchpb.FieldOption_Composite
				default:
					sp.Option = vearchpb.FieldOption_Scalar
				}
			}
		} else {
			sp.Option = vearchpb.FieldOption_Null
		}

		if isVector {
			sp.Index = field.Index
		}

		// set date format
		if sp.Format != nil {
			if !(sp.FieldType == vearchpb.FieldType_DATE || sp.FieldType == vearchpb.FieldType_VECTOR) {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("type:[%d] can not set format", sp.FieldType))
			}
		}

		tmpPro[field.Name] = sp
	}
	return tmpPro, nil
}

// IsScalarIndexType returns true if the given index type is a scalar (non-vector) index type.
func IsScalarIndexType(indexType string) bool {
	switch indexType {
	case ScalarIndexType, InvertedIndexType, BitmapIndexType, CompositeIndexType:
		return true
	default:
		return false
	}
}

// validateIndexes checks structural constraints for each index definition.
// Rules:
//   - COMPOSITE: must have field_names with at least 2 fields, field_name must be empty
//   - Single-field types (SCALAR/INVERTED/BITMAP/INVERTED_LIST): must have exactly one field_name, field_names must be empty
//   - field_name and field_names cannot both be set
//   - name cannot be empty
func validateIndexes(indexes []*Index, props map[string]*SpaceProperties) error {
	indexNames := make(map[string]bool)
	for i, idx := range indexes {
		if idx.Name == "" {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("indexes[%d]: name cannot be empty", i))
		}
		if _, ok := indexNames[idx.Name]; ok {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("index name can not be duplicate"))
		} else {
			indexNames[idx.Name] = true
		}
		// field_name and field_names cannot coexist
		if idx.FieldName != "" && len(idx.FieldNames) > 0 {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("indexes[%d] name[%s]: field_name and field_names cannot both be set", i, idx.Name))
		}

		if idx.Type == CompositeIndexType {
			if len(idx.FieldNames) < 2 {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
					fmt.Errorf("indexes[%d] name[%s] type[COMPOSITE]: requires at least 2 fields, got %d",
						i, idx.Name, len(idx.FieldNames)))
			}
			seen := make(map[string]struct{})
			for _, name := range idx.FieldNames {
				if _, ok := props[name]; !ok {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
						fmt.Errorf("indexes[%d] name[%s] type[COMPOSITE]: field[%s] not found in fields", i, idx.Name, name))
				}
				// COMPOSITE index: all referenced fields must be scalar fields
				if props[name].FieldType == vearchpb.FieldType_VECTOR {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
						fmt.Errorf("indexes[%d] name[%s] type[COMPOSITE]: field[%s] is a vector field, composite index only supports scalar fields",
							i, idx.Name, name))
				}
				if _, ok := seen[name]; ok {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
						fmt.Errorf("indexes[%d] name[%s] type[COMPOSITE]: duplicate field[%s] in field_names", i, idx.Name, name))
				}
				seen[name] = struct{}{}
			}
		} else {
			if idx.FieldName == "" {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
					fmt.Errorf("indexes[%d] name[%s] type[%s]: requires field_name to be set",
						i, idx.Name, idx.Type))
			}
			prop, ok := props[idx.FieldName]
			if !ok {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
					fmt.Errorf("indexes[%d] name[%s] type[%s]: field[%s] not found in fields", i, idx.Name, idx.Type, idx.FieldName))
			}
			// Check field-type/index-type compatibility for single-field indexes
			if IsScalarIndexType(idx.Type) {
				if prop.FieldType == vearchpb.FieldType_VECTOR {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
						fmt.Errorf("indexes[%d] name[%s] type[%s]: field[%s] is a vector field, scalar index types are not allowed",
							i, idx.Name, idx.Type, idx.FieldName))
				}
			} else {
				if prop.FieldType != vearchpb.FieldType_VECTOR {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
						fmt.Errorf("indexes[%d] name[%s] type[%s]: field[%s] is a scalar field, use scalar index types instead",
							i, idx.Name, idx.Type, idx.FieldName))
				}
			}
		}
	}
	return nil
}

// This means a field can participate in both a single-field index and a composite index simultaneously.
func MergeFieldIndexes(props map[string]*SpaceProperties, indexes *[]*Index) error {
	for fieldName, field := range props {
		if field.Index != nil {
			if field.Index.Type == CompositeIndexType {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
					fmt.Errorf("field[%s] index type[COMPOSITE] can not set in fields", fieldName))
			}
			for _, idx := range *indexes {
				if idx == nil {
					continue
				}
				if fieldName == idx.FieldName && idx.Type != CompositeIndexType {
					return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
						fmt.Errorf("field[%s] index duplicated", fieldName))
				}
			}
			index := &Index{
				Name:      field.Index.Name,
				Type:      field.Index.Type,
				FieldName: fieldName,
				Params:    field.Index.Params,
			}
			*indexes = append(*indexes, index)
		}
	}
	if err := validateIndexes(*indexes, props); err != nil {
		return err
	}
	return nil
}
