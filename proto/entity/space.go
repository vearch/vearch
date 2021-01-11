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

	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/vearchlog"
)

const (
	Gamma = "gamma"
)

type FieldType int32

const (
	FieldType_INT FieldType = iota
	FieldType_LONG
	FieldType_FLOAT
	FieldType_DOUBLE
	FieldType_STRING
	FieldType_VECTOR
	FieldType_BOOL
	FieldType_GEOPOINT
	FieldType_DATE
)

type FieldOption int32

const (
	FieldOption_Null        FieldOption = 0
	FieldOption_Index       FieldOption = 1
	FieldOption_Index_False FieldOption = 2
)

type Engine struct {
	Name            string          `json:"name"`
	IndexSize       int64           `json:"index_size"`
	MetricType      string          `json:"metric_type,omitempty"`
	RetrievalType   string          `json:"retrieval_type,omitempty"`
	RetrievalTypes  []string        `json:"retrieval_types,omitempty"`
	RetrievalParam  json.RawMessage `json:"retrieval_param,omitempty"`
	RetrievalParams json.RawMessage `json:"retrieval_params,omitempty"`
	IdType          string          `json:"id_type,omitempty"`
}

func NewDefaultEngine() *Engine {
	return &Engine{
		Name: Gamma,
	}
}

type RetrievalParams struct {
	RetrievalParamArr []RetrievalParam `json:"retrieval_params,omitempty"`
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
	Id              SpaceID                     `json:"id,omitempty"`
	Name            string                      `json:"name,omitempty"` //user setting
	Version         Version                     `json:"version,omitempty"`
	DBId            DBID                        `json:"db_id,omitempty"`
	Enabled         *bool                       `json:"enabled"`    //Enabled flag whether the space can work
	Partitions      []*Partition                `json:"partitions"` // partitionids not sorted
	PartitionNum    int                         `json:"partition_num"`
	ReplicaNum      uint8                       `json:"replica_num"`
	Properties      json.RawMessage             `json:"properties"`
	Engine          *Engine                     `json:"engine"`
	Models          json.RawMessage             `json:"models,omitempty"` //json model config for python plugin
	SpaceProperties map[string]*SpaceProperties `json:"space_properties"`
}

type SpaceProperties struct {
	FieldType  FieldType       `json:"field_type"`
	Type       string          `json:"type"`
	Index      *bool           `json:"index,omitempty"`
	Format     *string         `json:"format,omitempty"`
	Dimension  int             `json:"dimension,omitempty"`
	ModelId    string          `json:"model_id,omitempty"`
	StoreType  *string         `json:"store_type,omitempty"`
	StoreParam json.RawMessage `json:"store_param,omitempty"`
	Array      bool            `json:"array,omitempty"`
	HasSource  bool            `json:"has_source,omitempty"`
	Option     FieldOption     `json:"option,omitempty"`
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
		Name            string          `json:"name"`
		IndexSize       *int64          `json:"index_size"`
		MetricType      string          `json:"metric_type,omitempty"`
		RetrievalParam  json.RawMessage `json:"retrieval_param,omitempty"`
		RetrievalParams json.RawMessage `json:"retrieval_params,omitempty"`
		RetrievalType   string          `json:"retrieval_type,omitempty"`
		RetrievalTypes  []string        `json:"retrieval_types,omitempty"`
		IdType          string          `json:"id_type,omitempty"`
	}{}

	if err := json.Unmarshal(bs, tempEngine); err != nil {
		return fmt.Errorf("parameter analysis err ,the details err:%v", err)

	}

	retrievalTypeMap := map[string]string{"IVFPQ": "IVFPQ", "IVFFLAT": "IVFFLAT", "BINARYIVF": "BINARYIVF", "FLAT": "FLAT", "HNSW": "HNSW", "GPU": "GPU"}
	var retrievalParamsArr []string
	switch tempEngine.Name {
	case Gamma:
		if tempEngine.RetrievalType == "" {
			if tempEngine.RetrievalTypes == nil || len(tempEngine.RetrievalTypes) == 0 {
				return fmt.Errorf("retrieval_type or retrieval_types is not null")
			}
		} else {
			_, have := retrievalTypeMap[tempEngine.RetrievalType]
			if !have {
				return fmt.Errorf("retrieval_type not support :%s", tempEngine.RetrievalType)
			}
		}

		retrievalParams := &RetrievalParams{}
		if tempEngine.RetrievalTypes != nil && len(tempEngine.RetrievalTypes) > 0 {
			for _, retrievalType := range tempEngine.RetrievalTypes {
				_, have := retrievalTypeMap[retrievalType]
				if !have {
					return fmt.Errorf("retrieval_types not support:%s", retrievalType)
				}
			}
			if tempEngine.RetrievalParams == nil {
				return fmt.Errorf("retrieval_types not null and need retrieval_params param")
			}

			err := cbjson.Unmarshal(tempEngine.RetrievalParams, &retrievalParams.RetrievalParamArr)
			if err != nil {
				return fmt.Errorf("retrieval_params Unmarshal error")
			}

			if len(tempEngine.RetrievalTypes) != len(retrievalParams.RetrievalParamArr) {
				return fmt.Errorf("retrieval_types lenth not equal retrieval_params lenth")
			}
		}

		if tempEngine.RetrievalParams != nil {
			if tempEngine.RetrievalTypes == nil {
				return fmt.Errorf("retrieval_params is not null need retrieval_types is not null")
			}
			err := cbjson.Unmarshal(tempEngine.RetrievalParams, &retrievalParams.RetrievalParamArr)
			if err != nil {
				return fmt.Errorf("retrieval_params Unmarshal error")
			}
			retrievalParamsArr = make([]string, len(retrievalParams.RetrievalParamArr))
			for i := 0; i < len(retrievalParams.RetrievalParamArr); i++ {
				v := retrievalParams.RetrievalParamArr[i]
				retrievalType := tempEngine.RetrievalTypes[i]
				if strings.Compare(retrievalType, "HNSW") == 0 {
					if v.Nlinks == 0 || v.EfConstruction == 0 {
						return fmt.Errorf("HNSW model param is 0")
					}
					if tempEngine.IndexSize == nil || *tempEngine.IndexSize <= 0 {
						tempEngine.IndexSize = util.PInt64(1)
					}
				} else if strings.Compare(retrievalType, "FLAT") == 0 {

				} else if strings.Compare("BINARYIVF", retrievalType) == 0 ||
					strings.Compare("IVFFLAT", retrievalType) == 0 {
					if v.Ncentroids == 0 {
						return fmt.Errorf(retrievalType + " model param is 0")
					} else {
						if tempEngine.IndexSize == nil || *tempEngine.IndexSize <= 0 {
							tempEngine.IndexSize = util.PInt64(100000)
						}
					}
					if *tempEngine.IndexSize < 8192 {
						return fmt.Errorf(retrievalType+" model doc size:[%d] less than 8192 so can not to index", int64(*tempEngine.IndexSize))
					}
				} else {
					if v.Nsubvector == 0 || v.Ncentroids == 0 {
						return fmt.Errorf(retrievalType + " model param is 0")
					} else {
						if tempEngine.IndexSize == nil || *tempEngine.IndexSize <= 0 {
							tempEngine.IndexSize = util.PInt64(100000)
						}
					}
					if *tempEngine.IndexSize < 8192 {
						return fmt.Errorf(retrievalType+" model doc size:[%d] less than 8192 so can not to index", int64(*tempEngine.IndexSize))
					}
				}
				if v.MetricType == "" {
					return fmt.Errorf("metric_type is null")
				}

				tempEngine.MetricType = v.MetricType
				retrievalParamsByte, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("retrievalParams to json err")
				}
				retrievalParamsArr[i] = string(retrievalParamsByte)
			}
		} else {
			if tempEngine.IndexSize == nil {
				tempEngine.IndexSize = util.PInt64(100000)
			}
		}

		if tempEngine.RetrievalParam != nil {
			var v RetrievalParam
			if err := json.Unmarshal(tempEngine.RetrievalParam, &v); err != nil {
				return fmt.Errorf("engine UnmarshalJSON RetrievalParam json.Unmarshal err :[%s]", err.Error())
			}

			if strings.Compare(tempEngine.RetrievalType, "HNSW") == 0 {
				if v.Nlinks == 0 || v.EfConstruction == 0 {
					return fmt.Errorf("HNSW model param is 0")
				}
				if tempEngine.IndexSize == nil || *tempEngine.IndexSize <= 0 {
					tempEngine.IndexSize = util.PInt64(1)
				}
			} else if strings.Compare(tempEngine.RetrievalType, "FLAT") == 0 {

			} else if strings.Compare("BINARYIVF", tempEngine.RetrievalType) == 0 ||
				strings.Compare("IVFFLAT", tempEngine.RetrievalType) == 0 {
				if v.Ncentroids == 0 {
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
		return vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITON_ENGINENAME_INVALID, nil))
	}

	*engine = Engine{
		Name:            tempEngine.Name,
		IndexSize:       *tempEngine.IndexSize,
		RetrievalParam:  tempEngine.RetrievalParam,
		RetrievalParams: tempEngine.RetrievalParams,
		MetricType:      tempEngine.MetricType,
		RetrievalType:   tempEngine.RetrievalType,
		RetrievalTypes:  tempEngine.RetrievalTypes,
		IdType:          tempEngine.IdType,
	}

	return nil
}

//check params is ok
func (space *Space) Validate() error {

	switch space.Engine.Name {
	case Gamma:
	default:
		return vearchpb.NewError(vearchpb.ErrorEnum_INVALID_ENGINE, nil)
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
		case "geo_point":
			sp.FieldType = FieldType_GEOPOINT
		case "vector":
			sp.FieldType = FieldType_VECTOR

			isVector = true
			if sp.Dimension == 0 {
				return nil, fmt.Errorf("dimension can not zero by field : [%s] ", string(data))
			}

			if sp.StoreType != nil && *sp.StoreType != "" {
				if *sp.StoreType != "Mmap" && *sp.StoreType != "RocksDB" && *sp.StoreType != "MemoryOnly" {
					return nil, fmt.Errorf("vector field:[%s] not support this store type:[%s] it only Mmap or RocksDB or MemoryOnly", name, *sp.StoreType)
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
				return nil, fmt.Errorf("type:[%s] can not set format", sp.FieldType)
			}
		}

		tmpPro[name] = sp
	}
	return tmpPro, nil
}
