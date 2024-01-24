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

package mapping

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbbytes"
)

const (
	IndexField      = "_index"
	UidField        = "_uid"
	TypeField       = "_type"
	IdField         = "_id"
	SourceField     = "_source"
	SizeField       = "_size"
	AllField        = "_all"
	FieldNamesField = "_field_names"
	IgnoredField    = "_ignored"
	RoutingField    = "_routing"
	MetaField       = "_meta"

	//	VersionField    = "_version"
	//	SlotField       = "_slot"
)

var FieldsIndex = map[string]int{
	IndexField:      1,
	UidField:        2,
	TypeField:       3,
	IdField:         4,
	SourceField:     5,
	SizeField:       6,
	AllField:        7,
	FieldNamesField: 8,
	IgnoredField:    9,
	RoutingField:    10,
	MetaField:       11,
	//	VersionField:    12,
	//	SlotField:       13,
}

// control the default behavior for dynamic fields (those not explicitly mapped)
var (
	withOutIndex = vearchpb.FieldOption_Null
	withIndex    = vearchpb.FieldOption_Index
)

type FieldMapping struct {
	Name string
	FieldMappingI
}

func NewFieldMapping(name string, i FieldMappingI) *FieldMapping {
	i.Base().Name = name
	return &FieldMapping{Name: name, FieldMappingI: i}
}

func (f *FieldMapping) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Type      string  `json:"type"`
		Index     *bool   `json:"index,omitempty"`
		Format    *string `json:"format,omitempty"`
		Dimension int     `json:"dimension,omitempty"`
		ModelId   string  `json:"model_id,omitempty"`
		//		RetrievalType *string         `json:"retrieval_type,omitempty"`
		StoreType  *string         `json:"store_type,omitempty"`
		StoreParam json.RawMessage `json:"store_param,omitempty"`
		Array      bool            `json:"array,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	var fieldMapping FieldMappingI
	switch tmp.Type {
	case "text", "keyword", "string":
		fieldMapping = NewStringFieldMapping("")
	case "date":
		fieldMapping = NewDateFieldMapping("")
	case "integer", "short", "byte":
		fieldMapping = NewIntegerFieldMapping("")
	case "long":
		fieldMapping = NewLongFieldMapping("")
	case "float":
		fieldMapping = NewFloatFieldMapping("")
	case "double":
		fieldMapping = NewDoubleFieldMapping("")
	case "boolean", "bool":
		fieldMapping = NewBooleanFieldMapping("")
	case "vector":
		fieldMapping = NewVectorFieldMapping("")
		if tmp.Dimension == 0 {
			return fmt.Errorf("dimension can not zero by field : [%s] ", string(data))
		}
		/*if tmp.RetrievalType != nil && *tmp.RetrievalType != "" {
			fieldMapping.(*VectortFieldMapping).RetrievalType = *tmp.RetrievalType
		} else {
			return fmt.Errorf("retrieval_type can not null by field : [%s] ", string(data))
		}*/
		if tmp.StoreType != nil && *tmp.StoreType != "" {
			if *tmp.StoreType != "RocksDB" && *tmp.StoreType != "MemoryOnly" {
				return fmt.Errorf("vector field:[%s] not support this store type:[%s] it only RocksDB or MemoryOnly", fieldMapping.FieldName(), *tmp.StoreType)
			}
			fieldMapping.(*VectortFieldMapping).StoreType = *tmp.StoreType
		}
		if tmp.StoreParam != nil && len(tmp.StoreParam) > 0 {
			fieldMapping.(*VectortFieldMapping).StoreParam = tmp.StoreParam
		}

	default:
		return errors.New("invalid field type")
	}

	fieldMapping.Base().Array = tmp.Array

	//set index
	if tmp.Index != nil {
		if *tmp.Index {
			fieldMapping.Base().Option |= vearchpb.FieldOption_Index
		} else {
			fieldMapping.Base().Option = fieldMapping.Base().Option & withOutIndex
		}
	}

	//set dimension
	if tmp.Dimension != 0 {
		if mapping, ok := fieldMapping.(*VectortFieldMapping); ok {
			mapping.Dimension = tmp.Dimension
		} else {
			return fmt.Errorf("type:[%s] can not set dimension", fieldMapping.FieldType().String())
		}
	}

	//set model id
	if tmp.ModelId != "" {
		if mapping, ok := fieldMapping.(*VectortFieldMapping); ok {
			mapping.ModelId = tmp.ModelId
		} else {
			return fmt.Errorf("type:[%s] can not set model_id", fieldMapping.FieldType().String())
		}
	}

	//set date format
	if tmp.Format != nil {
		if mapping, ok := fieldMapping.(*DateFieldMapping); ok {
			mapping.Format = *tmp.Format
		} else if mapping, ok := fieldMapping.(*VectortFieldMapping); ok {
			mapping.Format = tmp.Format
		} else {
			return fmt.Errorf("type:[%s] can not set format", fieldMapping.FieldType().String())
		}
	}

	fieldMapping.Base().Name = f.Name
	f.FieldMappingI = fieldMapping
	return nil
}

type FieldMappingI interface {
	FieldName() string
	FieldType() vearchpb.FieldType
	Options() vearchpb.FieldOption
	Base() *BaseFieldMapping
	IsArray() bool
}

func NewBaseFieldMapping(name string, fieldType vearchpb.FieldType, boost float64, option vearchpb.FieldOption) *BaseFieldMapping {
	return &BaseFieldMapping{
		Type:   fieldType,
		Name:   name,
		Boost:  boost,
		Option: option,
	}
}

type BaseFieldMapping struct {
	Type   vearchpb.FieldType   `json:"type"`
	Name   string               `json:"_"`
	Boost  float64              `json:"boost,omitempty"`
	Option vearchpb.FieldOption `json:"option,omitempty"`
	Array  bool                 `json:"array,omitempty"`
}

func (f *BaseFieldMapping) Base() *BaseFieldMapping {
	return f
}

func (f *BaseFieldMapping) FieldName() string {
	return f.Name
}

func (f *BaseFieldMapping) FieldType() vearchpb.FieldType {
	return f.Type
}

func (f *BaseFieldMapping) Options() vearchpb.FieldOption {
	return f.Option
}

func (f *BaseFieldMapping) IsArray() bool {
	return f.Array
}

type StringFieldMapping struct {
	*BaseFieldMapping
	NullValue string `json:"null_value,omitempty"`
}

func NewStringFieldMapping(name string) *StringFieldMapping {
	return &StringFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_STRING, 1, vearchpb.FieldOption_Null),
	}
}

type NumericFieldMapping struct {
	*BaseFieldMapping
	NullValue       string `json:"null_value,omitempty"`
	Coerce          bool   `json:"coerce"`
	IgnoreMalformed bool   `json:"ignore_malformed"`
}

func NewIntegerFieldMapping(name string) *NumericFieldMapping {
	return &NumericFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_INT, 1, vearchpb.FieldOption_Null),
		Coerce:           true,
	}
}

func NewLongFieldMapping(name string) *NumericFieldMapping {
	return &NumericFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_LONG, 1, vearchpb.FieldOption_Null),
		Coerce:           true,
	}
}

func NewFloatFieldMapping(name string) *NumericFieldMapping {
	return &NumericFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_FLOAT, 1, vearchpb.FieldOption_Null),
		Coerce:           true,
	}
}

func NewDoubleFieldMapping(name string) *NumericFieldMapping {
	return &NumericFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_DOUBLE, 1, vearchpb.FieldOption_Null),
		Coerce:           true,
	}
}

type DateFieldMapping struct {
	*BaseFieldMapping
	Format          string `json:"format,omitempty"`
	Locale          string `json:"locale,omitempty"`
	NullValue       string `json:"null_value,omitempty"`
	IgnoreMalformed bool   `json:"ignore_malformed"`
}

func NewDateFieldMapping(name string) *DateFieldMapping {
	return &DateFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_DATE, 1, vearchpb.FieldOption_Null),
	}
}

type BooleanFieldMapping struct {
	*BaseFieldMapping
	NullValue string `json:"null_value,omitempty"`
}

func NewBooleanFieldMapping(name string) *BooleanFieldMapping {
	return &BooleanFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_BOOL, 1, vearchpb.FieldOption_Null),
	}
}

type VectortFieldMapping struct {
	*BaseFieldMapping
	Dimension int     `json:"dimension"`
	ModelId   string  `json:"model_id"`
	Format    *string `json:"format,omitempty"` //default is "normalization", "normal" , if set "no" others it will not format
	//	RetrievalType string  `json:"retrieval_type,omitempty"` // "IVFPQ", "PACINS","GPU" ...
	StoreType  string `json:"store_type,omitempty"` // "Mmap", "RocksDB", "MemoryOnly"
	StoreParam []byte `json:"store_param,omitempty"`
}

func NewVectorFieldMapping(name string) *VectortFieldMapping {
	return &VectortFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_VECTOR, 1, vearchpb.FieldOption_Index),
		//		RetrievalType:    "IVFPQ",
		StoreType: "",
	}
}

func processString(ctx *walkContext, fm *FieldMapping, fieldName, val string) (*vearchpb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}

	switch fm.FieldType() {
	case vearchpb.FieldType_STRING:
		field := &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_STRING,
			Value:  []byte(val),
			Option: fm.Options(),
		}
		return field, nil
	case vearchpb.FieldType_DATE:
		// UTC time
		parsedDateTime, err := cast.ToTimeE(val)
		if err != nil {
			return nil, fmt.Errorf("parse date %s faield, err %v", val, err)
		}
		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_DATE,
			Value:  cbbytes.Int64ToByte(parsedDateTime.UnixNano()),
			Option: fm.Options(),
		}, nil

	case vearchpb.FieldType_INT:
		numericFM := fm.FieldMappingI.(*NumericFieldMapping)
		if numericFM.Coerce {
			i, err := cast.ToInt32E(val)
			if err != nil {
				if numericFM.IgnoreMalformed {
					return nil, nil
				} else {
					return nil, fmt.Errorf("parse string %s to integer failed, err %v", val, err)
				}
			}
			return &vearchpb.Field{
				Name:   fieldName,
				Type:   vearchpb.FieldType_INT,
				Value:  cbbytes.Int32ToByte(i),
				Option: fm.Options(),
			}, nil
		} else {
			return nil, fmt.Errorf("string mismatch field:[%s] type:[%s] ", fieldName, fm.FieldType())
		}
	case vearchpb.FieldType_LONG:
		numericFM := fm.FieldMappingI.(*NumericFieldMapping)
		if numericFM.Coerce {
			i, err := cast.ToInt64E(val)
			if err != nil {
				if numericFM.IgnoreMalformed {
					return nil, nil
				} else {
					return nil, fmt.Errorf("parse string %s to long failed, err %v", val, err)
				}
			}
			return &vearchpb.Field{
				Name:   fieldName,
				Type:   vearchpb.FieldType_LONG,
				Value:  cbbytes.Int64ToByte(i),
				Option: fm.Options(),
			}, nil
		} else {
			return nil, fmt.Errorf("string mismatch field:[%s] type:[%s] ", fieldName, fm.FieldType())
		}
	case vearchpb.FieldType_FLOAT:
		numericFM := fm.FieldMappingI.(*NumericFieldMapping)
		if numericFM.Coerce {
			f, err := cast.ToFloat32E(val)
			if err != nil {
				if numericFM.IgnoreMalformed {
					return nil, nil
				} else {
					return nil, fmt.Errorf("parse string %s to float failed, err %v", val, err)
				}
			}
			return &vearchpb.Field{
				Name:   fieldName,
				Type:   vearchpb.FieldType_FLOAT,
				Value:  cbbytes.Float32ToByte(f),
				Option: fm.Options(),
			}, nil
		} else {
			return nil, fmt.Errorf("string mismatch field type %s", fm.FieldType())
		}
	case vearchpb.FieldType_DOUBLE:
		numericFM := fm.FieldMappingI.(*NumericFieldMapping)
		if numericFM.Coerce {
			f, err := cast.ToFloat64E(val)
			if err != nil {
				if numericFM.IgnoreMalformed {
					return nil, nil
				} else {
					return nil, fmt.Errorf("parse string %s to float64 failed, err %v", val, err)
				}
			}
			return &vearchpb.Field{
				Name:   fieldName,
				Type:   vearchpb.FieldType_DOUBLE,
				Value:  cbbytes.Float64ToByte(f),
				Option: fm.Options(),
			}, nil
		} else {
			return nil, fmt.Errorf("string mismatch field type %s", fm.FieldType())
		}
	}
	return nil, nil
}

func processNumber(ctx *walkContext, fm *FieldMapping, fieldName string, val float64) (*vearchpb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}

	switch fm.FieldType() {
	case vearchpb.FieldType_INT:
		i := int32(val)
		e := float32(val) - float32(i)
		if e > 0 || e < 0 {
			return nil, fmt.Errorf("string mismatch field:[%s] type:[%s] ", fieldName, fm.FieldType())
		}
		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_INT,
			Value:  cbbytes.Int32ToByte(i),
			Option: fm.Options(),
		}, nil
	case vearchpb.FieldType_LONG:
		i := int64(val)
		e := val - float64(i)
		if e > 0 || e < 0 {
			return nil, fmt.Errorf("string mismatch field:[%s] type:[%s] ", fieldName, fm.FieldType())
		}
		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_LONG,
			Value:  cbbytes.Int64ToByte(i),
			Option: fm.Options(),
		}, nil
	case vearchpb.FieldType_FLOAT:
		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_FLOAT,
			Value:  cbbytes.Float64ToByte(val),
			Option: fm.Options(),
		}, nil
	case vearchpb.FieldType_DOUBLE:
		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_DOUBLE,
			Value:  cbbytes.Float64ToByte(val),
			Option: fm.Options(),
		}, nil
	case vearchpb.FieldType_DATE:
		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_DATE,
			Value:  cbbytes.Int64ToByte(int64(val) * 1e6),
			Option: fm.Options(),
		}, nil
	default:
		return nil, fmt.Errorf("string mismatch field:[%s] value:[%f] type:[%s] ", fieldName, val, fm.FieldType())
	}
}

func processBool(ctx *walkContext, fm *FieldMapping, fieldName string, val bool) (*vearchpb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}
	switch fm.FieldType() {
	case vearchpb.FieldType_BOOL:
		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_BOOL,
			Value:  cbbytes.BoolToByte(val),
			Option: fm.Options(),
		}, nil
	default:
		return nil, fmt.Errorf("string mismatch field:[%s] type:[%s] ", fieldName, fm.FieldType())
	}
}

func processVectorBinary(ctx *walkContext, fm *FieldMapping, fieldName string, val []uint8) (*vearchpb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}

	switch fm.FieldType() {
	case vearchpb.FieldType_VECTOR:
		if fm.FieldMappingI.(*VectortFieldMapping).Dimension > 0 && (fm.FieldMappingI.(*VectortFieldMapping).Dimension)/8 != len(val) {
			return nil, fmt.Errorf("processVectorBinary field:[%s] vector_length err ,schema is:[%d] but input :[%d]", fieldName, fm.FieldMappingI.(*VectortFieldMapping).Dimension, len(val))
		}

		bs, err := cbbytes.VectorBinaryToByte(val)
		if err != nil {
			return nil, err
		}

		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_VECTOR,
			Value:  bs,
			Option: fm.Options(),
		}, nil
	default:
		return nil, fmt.Errorf("processVectorBinary field:[%s] value %v mismatch field type %s", fieldName, val, fm.FieldType())
	}
}

func processVector(ctx *walkContext, fm *FieldMapping, fieldName string, val []float32) (*vearchpb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}

	switch fm.FieldType() {
	case vearchpb.FieldType_VECTOR:
		if fm.FieldMappingI.(*VectortFieldMapping).Dimension > 0 && fm.FieldMappingI.(*VectortFieldMapping).Dimension != len(val) {
			return nil, fmt.Errorf("field:[%s] vector_length err ,schema is:[%d] but input :[%d]", fieldName, fm.FieldMappingI.(*VectortFieldMapping).Dimension, len(val))
		}

		if fm.FieldMappingI.(*VectortFieldMapping).Format != nil {
			switch *fm.FieldMappingI.(*VectortFieldMapping).Format {
			case "normalization", "normal":
				if err := util.Normalization(val); err != nil {
					return nil, err
				}
			case "no":
			default:
				return nil, fmt.Errorf("unknow vector process method:[%s]", *fm.FieldMappingI.(*VectortFieldMapping).Format)
			}
		}

		bs, err := cbbytes.VectorToByte(val)
		if err != nil {
			return nil, err
		}

		return &vearchpb.Field{
			Name:   fieldName,
			Type:   vearchpb.FieldType_VECTOR,
			Value:  bs,
			Option: fm.Options(),
		}, nil
	default:
		return nil, fmt.Errorf("field:[%s] value %v mismatch field type %s", fieldName, val, fm.FieldType())
	}
}
