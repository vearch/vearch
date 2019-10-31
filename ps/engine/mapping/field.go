//  Copyright (c) 2014 Couchbase, Inc.
// Modified work copyright (C) 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this Field except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mapping

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbbytes"
	"strings"

	"github.com/mmcloughlin/geohash"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/proto/pspb"
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
	VersionField    = "_version"
	SlotField       = "_slot"
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
	VersionField:    12,
	SlotField:       13,
}

// control the default behavior for dynamic fields (those not explicitly mapped)
var (
	withOutIndex = pspb.FieldOption_Null
	withIndex    = pspb.FieldOption_Index
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
		Type          string          `json:"type"`
		Index         *string         `json:"index,omitempty"`
		Format        *string         `json:"format,omitempty"`
		Dimension     int             `json:"dimension,omitempty"`
		ModelId       string          `json:"model_id,omitempty"`
		RetrievalType *string         `json:"retrieval_type,omitempty"`
		StoreType     *string         `json:"store_type,omitempty"`
		StoreParam    json.RawMessage `json:"store_param,omitempty"`
		Array         bool            `json:"array,omitempty"`
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
	case "long", "integer", "short", "byte":
		fieldMapping = NewIntegerFieldMapping("")
	case "double", "float":
		fieldMapping = NewFloatFieldMapping("")
	case "boolean", "bool":
		fieldMapping = NewBooleanFieldMapping("")
	case "geo_point":
		fieldMapping = NewGeoPointFieldMapping("")
	case "vector":
		fieldMapping = NewVectorFieldMapping("")
		if tmp.Dimension == 0 {
			return fmt.Errorf("dimension can not zero by field : [%s] ", string(data))
		}
		if tmp.RetrievalType != nil && *tmp.RetrievalType != "" {
			fieldMapping.(*VectortFieldMapping).RetrievalType = *tmp.RetrievalType
		}
		if tmp.StoreType != nil && *tmp.StoreType != "" {
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
		switch *tmp.Index {
		case "yes", "true":
			fieldMapping.Base().Option |= pspb.FieldOption_Index
		case "no", "false":
			fieldMapping.Base().Option = fieldMapping.Base().Option & withOutIndex
		default:
			return fmt.Errorf("tmp index param has err only support [yes , no true false] but got:[]%s", *tmp.Index)
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
	if tmp.Dimension != 0 {
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
	FieldType() pspb.FieldType
	Options() pspb.FieldOption
	Base() *BaseFieldMapping
	IsArray() bool
}

func NewBaseFieldMapping(name string, fieldType pspb.FieldType, boost float64, option pspb.FieldOption) *BaseFieldMapping {
	return &BaseFieldMapping{
		Type:   fieldType,
		Name:   name,
		Boost:  boost,
		Option: option,
	}
}

type BaseFieldMapping struct {
	Type   pspb.FieldType   `json:"type"`
	Name   string           `json:"_"`
	Boost  float64          `json:"boost,omitempty"`
	Option pspb.FieldOption `json:"option,omitempty"`
	Array  bool             `json:"array,omitempty"`
}

func (f *BaseFieldMapping) Base() *BaseFieldMapping {
	return f
}

func (f *BaseFieldMapping) FieldName() string {
	return f.Name
}

func (f *BaseFieldMapping) FieldType() pspb.FieldType {
	return f.Type
}

func (f *BaseFieldMapping) Options() pspb.FieldOption {
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
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_STRING, 1, pspb.FieldOption_Null),
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
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_INT, 1, pspb.FieldOption_Null),
		Coerce:           true,
	}
}

func NewFloatFieldMapping(name string) *NumericFieldMapping {
	return &NumericFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_FLOAT, 1, pspb.FieldOption_Null),
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
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_DATE, 1, pspb.FieldOption_Null),
	}
}

type BooleanFieldMapping struct {
	*BaseFieldMapping
	NullValue string `json:"null_value,omitempty"`
}

func NewBooleanFieldMapping(name string) *BooleanFieldMapping {
	return &BooleanFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_BOOL, 1, pspb.FieldOption_Null),
	}
}

type GeoPointFieldMapping struct {
	*BaseFieldMapping
	IgnoreZValue bool   `json:"ignore_z_value,omitempty"`
	NullValue    string `json:"null_value,omitempty"`
}

func NewGeoPointFieldMapping(name string) *GeoPointFieldMapping {
	return &GeoPointFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_GEOPOINT, 1, pspb.FieldOption_Null),
		IgnoreZValue:     true,
	}
}

type VectortFieldMapping struct {
	*BaseFieldMapping
	Dimension     int     `json:"dimension"`
	ModelId       string  `json:"model_id"`
	Format        *string `json:"format,omitempty"`         //"normalization", "normal"
	RetrievalType string  `json:"retrieval_type,omitempty"` // "IVFPQ", "PACINS", ...
	StoreType     string  `json:"store_type,omitempty"`     // "MemoryOnly", "MemoryWithDisk"
	StoreParam    []byte  `json:"store_param,omitempty"`
}

func NewVectorFieldMapping(name string) *VectortFieldMapping {
	return &VectortFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_VECTOR, 1, pspb.FieldOption_Index),
		RetrievalType:    "IVFPQ",
		StoreType:        "MemoryOnly",
	}
}

func processString(ctx *walkContext, fm *FieldMapping, fieldName, val string) (*pspb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}

	switch fm.FieldType() {
	case pspb.FieldType_STRING:
		field := &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_STRING,
			Value:  []byte(val),
			Option: fm.Options(),
		}
		return field, nil
	case pspb.FieldType_DATE:
		// UTC time
		parsedDateTime, err := cast.ToTimeE(val)
		if err != nil {
			return nil, fmt.Errorf("parse date %s faield, err %v", val, err)
		}
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_DATE,
			Value:  cbbytes.Int64ToByte(parsedDateTime.UnixNano()),
			Option: fm.Options(),
		}, nil

	case pspb.FieldType_INT:
		numericFM := fm.FieldMappingI.(*NumericFieldMapping)
		if numericFM.Coerce {
			i, err := cast.ToInt64E(val)
			if err != nil {
				if numericFM.IgnoreMalformed {
					return nil, nil
				} else {
					return nil, fmt.Errorf("parse string %s to integer failed, err %v", val, err)
				}
			}
			return &pspb.Field{
				Name:   fieldName,
				Type:   pspb.FieldType_INT,
				Value:  cbbytes.Int64ToByte(i),
				Option: fm.Options(),
			}, nil
		} else {
			return nil, fmt.Errorf("string mismatch field type %s", fm.FieldType())
		}
	case pspb.FieldType_FLOAT:
		numericFM := fm.FieldMappingI.(*NumericFieldMapping)
		if numericFM.Coerce {
			f, err := cast.ToFloat64E(val)
			if err != nil {
				if numericFM.IgnoreMalformed {
					return nil, nil
				} else {
					return nil, fmt.Errorf("parse string %s to integer failed, err %v", val, err)
				}
			}
			return &pspb.Field{
				Name:   fieldName,
				Type:   pspb.FieldType_FLOAT,
				Value:  cbbytes.Float64ToByte(f),
				Option: fm.Options(),
			}, nil
		} else {
			return nil, fmt.Errorf("string mismatch field type %s", fm.FieldType())
		}
	case pspb.FieldType_GEOPOINT:
		lat, lon, err := parseStringToGeoPoint(val)
		if err != nil {
			return nil, err
		}

		code, err := cbbytes.FloatArrayByte([]float32{float32(lon), float32(lat)})
		if err != nil {
			return nil, err
		}

		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_GEOPOINT,
			Value:  code,
			Option: fm.Options(),
		}, nil
	}
	return nil, nil
}

func processNumber(ctx *walkContext, fm *FieldMapping, fieldName string, val float64) (*pspb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}

	switch fm.FieldType() {
	case pspb.FieldType_INT:
		i := int64(val)
		e := val - float64(i)
		if e > 0 || e < 0 {
			return nil, fmt.Errorf("field value %f mismatch field type interger", val)
		}
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_INT,
			Value:  cbbytes.Int64ToByte(i),
			Option: fm.Options(),
		}, nil
	case pspb.FieldType_FLOAT:
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_FLOAT,
			Value:  cbbytes.Float64ToByte(val),
			Option: fm.Options(),
		}, nil
	case pspb.FieldType_DATE:
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_DATE,
			Value:  cbbytes.Int64ToByte(int64(val) * 1e6),
			Option: fm.Options(),
		}, nil
	default:
		return nil, fmt.Errorf("field value %f mismatch field type %s", val, fm.FieldType())
	}
}

func processGeoPoint(ctx *walkContext, fm *FieldMapping, fieldName string, lon, lat float64) (*pspb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}

	switch fm.FieldType() {
	case pspb.FieldType_GEOPOINT:
		code, err := cbbytes.FloatArrayByte([]float32{float32(lon), float32(lat)})
		if err != nil {
			return nil, err
		}
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_GEOPOINT,
			Value:  code,
			Option: fm.Options(),
		}, nil
	default:
		return nil, fmt.Errorf("field value [%f,%f] mismatch field type %s", lon, lat, fm.FieldType())
	}
}

func processBool(ctx *walkContext, fm *FieldMapping, fieldName string, val bool) (*pspb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}
	switch fm.FieldType() {
	case pspb.FieldType_BOOL:
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_BOOL,
			Value:  cbbytes.BoolToByte(val),
			Option: fm.Options(),
		}, nil
	default:
		return nil, fmt.Errorf("field value %v mismatch field type %s", val, fm.FieldType())
	}
}

func processVector(ctx *walkContext, fm *FieldMapping, fieldName string, val []float32, source string) (*pspb.Field, error) {
	if ctx.Err != nil {
		return nil, ctx.Err
	}

	switch fm.FieldType() {
	case pspb.FieldType_VECTOR:
		if fm.FieldMappingI.(*VectortFieldMapping).Dimension > 0 && fm.FieldMappingI.(*VectortFieldMapping).Dimension != len(val) {
			return nil, fmt.Errorf("field:[%s] vector_length err ,schema is:[%d] but input :[%d]", fieldName, fm.FieldMappingI.(*VectortFieldMapping).Dimension, len(val))
		}

		if fm.FieldMappingI.(*VectortFieldMapping).Format != nil && len(*fm.FieldMappingI.(*VectortFieldMapping).Format) > 0 {
			switch *fm.FieldMappingI.(*VectortFieldMapping).Format {
			case "normalization", "normal":
				if err := util.Normalization(val); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unknow vector process method:[%s]", *fm.FieldMappingI.(*VectortFieldMapping).Format)
			}
		}

		bs, err := cbbytes.VectorToByte(val, source)
		if err != nil {
			return nil, err
		}

		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_VECTOR,
			Value:  bs,
			Option: fm.Options(),
		}, nil
	default:
		return nil, fmt.Errorf("field:[%s] value %v mismatch field type %s", fieldName, val, fm.FieldType())
	}
}

func parseStringToGeoPoint(val string) (lat float64, lon float64, err error) {
	// Geo-point expressed as a string with the format: "lat,lon".
	ls := strings.Split(val, ",")
	if len(ls) == 2 {
		lat, err = cast.ToFloat64E(ls[0])
		if err != nil {
			return
		}
		lon, err = cast.ToFloat64E(ls[1])
		if err != nil {
			return
		}
		return
	}

	// Geo-point expressed as a geohash.
	if len(val) != 12 {
		err = fmt.Errorf("invalid geohash %s", val)
		return
	}
	lat, lon = geohash.Decode(val)
	return
}
