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
	withOutDocValue = pspb.FieldOption_Index | pspb.FieldOption_Store | pspb.FieldOption_IncludeTermVectors
	withOutIndex    = pspb.FieldOption_Store | pspb.FieldOption_IncludeTermVectors | pspb.FieldOption_DocValues
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
		DocValues     *bool           `json:"doc_values,omitempty"`
		Index         *string         `json:"index,omitempty"`
		Store         *bool           `json:"store,omitempty"`
		TermVector    *string         `json:"term_vector,omitempty"`
		Format        *string         `json:"format,omitempty"`
		CopyTo        json.RawMessage `json:"copy_to,omitempty"`
		IgnoreAbove   int             `json:"ignore_above,omitempty"`
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
	case "text":
		fieldMapping = NewTextFieldMapping("")
		fieldMapping.(*TextFieldMapping).Analyzer = DefaultAnalyzer
	case "keyword", "string":
		fieldMapping = NewKeywordFieldMapping("")
		if tmp.Array { //for gamma
			fieldMapping.(*KeywordFieldMapping).Array = true
		}
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

	if tmp.Store != nil {
		if *tmp.Store {
			fieldMapping.Base().Option |= pspb.FieldOption_Store
		}
	}

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

	//set docvalues
	if tmp.DocValues != nil {
		if !*tmp.DocValues {
			fieldMapping.Base().Option &= withOutDocValue
		} else {
			fieldMapping.Base().Option |= pspb.FieldOption_DocValues
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

	//set term vector
	if tmp.Type == "text" { //if text has other type names please fixme
		if tmp.TermVector == nil || *tmp.TermVector != "no" { //not all es types support TODO FIXME
			fieldMapping.Base().Option |= pspb.FieldOption_IncludeTermVectors
		}
	}

	//set ignore above
	if tmp.IgnoreAbove != 0 {
		if mapping, ok := fieldMapping.(*TextFieldMapping); !ok {
			return fmt.Errorf("type:[%s] can not set ignore_above", fieldMapping.FieldType().String())
		} else {
			mapping.IgnoreAbove = tmp.IgnoreAbove
		}
	}

	if tmp.CopyTo != nil {
		var copyTo string
		var copyTos []string
		err = json.Unmarshal(tmp.CopyTo, &copyTo)
		if err != nil {
			copyTos = make([]string, 0)
			err = json.Unmarshal(tmp.CopyTo, &copyTos)
			if err != nil {
				return err
			}
		} else {
			copyTos = append(copyTos, copyTo)
		}
		fieldMapping.Base().CopyTo = copyTos
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
	CopyTo []string         `json:"copy_to,omitempty"`
	Option pspb.FieldOption `json:"option,omitempty"`
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

type TextFieldMapping struct {
	*BaseFieldMapping
	Analyzer       string `json:"analyzer,omitempty"`
	SearchAnalyzer string `json:"search_analyzer,omitempty"`
	IgnoreAbove    int    `json:"ignore_above,omitempty"`
}

func NewTextFieldMapping(name string) *TextFieldMapping {
	return &TextFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_TEXT, 1, pspb.FieldOption_IncludeTermVectors),
		IgnoreAbove:      327660,
	}
}

type KeywordFieldMapping struct {
	*BaseFieldMapping
	IgnoreAbove int    `json:"ignore_above,omitempty"`
	NullValue   string `json:"null_value,omitempty"`
	Array       bool   `json:"array,omitempty"`
}

func NewKeywordFieldMapping(name string) *KeywordFieldMapping {
	return &KeywordFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_KEYWORD, 1, pspb.FieldOption_DocValues),
		IgnoreAbove:      1024,
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
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_INT, 1, pspb.FieldOption_DocValues),
		Coerce:           true,
	}
}

func NewFloatFieldMapping(name string) *NumericFieldMapping {
	return &NumericFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_FLOAT, 1, pspb.FieldOption_DocValues),
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
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_DATE, 1, pspb.FieldOption_DocValues),
	}
}

type BooleanFieldMapping struct {
	*BaseFieldMapping
	NullValue string `json:"null_value,omitempty"`
}

func NewBooleanFieldMapping(name string) *BooleanFieldMapping {
	return &BooleanFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_BOOL, 1, pspb.FieldOption_DocValues),
	}
}

type GeoPointFieldMapping struct {
	*BaseFieldMapping
	IgnoreZValue bool   `json:"ignore_z_value,omitempty"`
	NullValue    string `json:"null_value,omitempty"`
}

func NewGeoPointFieldMapping(name string) *GeoPointFieldMapping {
	return &GeoPointFieldMapping{
		BaseFieldMapping: NewBaseFieldMapping(name, pspb.FieldType_GEOPOINT, 1, pspb.FieldOption_DocValues),
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
	case pspb.FieldType_TEXT:
		field := &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_TEXT,
			Value:  &pspb.FieldValue{Text: val},
			Option: fm.Options(),
		}
		if len(val) > fm.FieldMappingI.(*TextFieldMapping).IgnoreAbove {
			field.Option = field.Option & withOutDocValue
		}
		return field, nil
	case pspb.FieldType_KEYWORD:
		field := &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_KEYWORD,
			Value:  &pspb.FieldValue{Text: val},
			Option: fm.Options(),
		}
		if len(val) > fm.FieldMappingI.(*KeywordFieldMapping).IgnoreAbove {
			field.Option = field.Option & withOutDocValue
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
			Value:  &pspb.FieldValue{Time: &pspb.TimeStamp{Usec: parsedDateTime.UnixNano()}},
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
				Value:  &pspb.FieldValue{Int: i},
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
				Value:  &pspb.FieldValue{Float: f},
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
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_GEOPOINT,
			Value:  &pspb.FieldValue{Geo: &pspb.Geo{Lon: lon, Lat: lat}},
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
			Value:  &pspb.FieldValue{Int: i},
			Option: fm.Options(),
		}, nil
	case pspb.FieldType_FLOAT:
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_FLOAT,
			Value:  &pspb.FieldValue{Float: val},
			Option: fm.Options(),
		}, nil
	case pspb.FieldType_DATE:
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_DATE,
			Value:  &pspb.FieldValue{Time: &pspb.TimeStamp{Usec: int64(val) * 1e6}},
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
		return &pspb.Field{
			Name:   fieldName,
			Type:   pspb.FieldType_GEOPOINT,
			Value:  &pspb.FieldValue{Geo: &pspb.Geo{Lon: lon, Lat: lat}},
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
			Value:  &pspb.FieldValue{Bool: val},
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

		return &pspb.Field{
			Name: fieldName,
			Type: pspb.FieldType_VECTOR,
			Value: &pspb.FieldValue{Vector: &pspb.Vector{
				Feature: val,
				Source:  source,
			}},
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
