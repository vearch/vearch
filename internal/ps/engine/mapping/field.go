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

	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/proto/vearchpb"
)

const (
	IdField = "_id"
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
		Name       *string         `json:"name,omitempty"`
		Type       string          `json:"type"`
		Index      *entity.Index   `json:"index,omitempty"`
		Format     *string         `json:"format,omitempty"`
		Dimension  int             `json:"dimension,omitempty"`
		StoreType  *string         `json:"store_type,omitempty"`
		StoreParam json.RawMessage `json:"store_param,omitempty"`
		Array      bool            `json:"array,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	if tmp.Name != nil {
		f.Name = *tmp.Name
	}

	var fieldMapping FieldMappingI
	switch tmp.Type {
	case "text", "keyword", "string":
		fieldMapping = NewStringFieldMapping("")
	case "date":
		fieldMapping = NewDateFieldMapping("")
	case "integer", "int":
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
		fieldMapping.Base().Option |= vearchpb.FieldOption_Index
	}

	//set dimension
	if tmp.Dimension != 0 {
		if mapping, ok := fieldMapping.(*VectortFieldMapping); ok {
			mapping.Dimension = tmp.Dimension
		} else {
			return fmt.Errorf("type:[%s] can not set dimension", fieldMapping.FieldType().String())
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
	Dimension  int     `json:"dimension"`
	Format     *string `json:"format,omitempty"`     // default is "normalization", "normal" , if set "no" others it will not format
	StoreType  string  `json:"store_type,omitempty"` // "RocksDB", "MemoryOnly"
	StoreParam []byte  `json:"store_param,omitempty"`
}

func NewVectorFieldMapping(name string) *VectortFieldMapping {
	return &VectortFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, vearchpb.FieldType_VECTOR, 1, vearchpb.FieldOption_Index),
		StoreType:        "",
	}
}
