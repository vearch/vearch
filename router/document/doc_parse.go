// Copyright 2018 The Couchbase Authors.
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

package document

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/valyala/fastjson"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/router/document/rutil"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/netutil"
)

const (
	// key index field
	IndexField      = "_index"
	UIDField        = "_uid"
	TypeField       = "_type"
	IDField         = "_id"
	SourceField     = "_source"
	SizeField       = "_size"
	AllField        = "_all"
	FieldNamesField = "_field_names"
	IgnoredField    = "_ignored"
	RoutingField    = "_routing"
	MetaField       = "_meta"
)

// fields index map
var FieldsIndex = map[string]int{
	// value index
	IndexField:      1,
	UIDField:        2,
	TypeField:       3,
	IDField:         4,
	SourceField:     5,
	SizeField:       6,
	AllField:        7,
	FieldNamesField: 8,
	IgnoredField:    9,
	RoutingField:    10,
	MetaField:       11,
}

// parse doc
func MapDocument(source []byte, space *entity.Space, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.Field, error) {
	var fast fastjson.Parser
	v, err := fast.ParseBytes(source)
	if err != nil {
		log.Warnf("bytes transform to json failed when inserting, err: %s ,data:%s", err.Error(), string(source))
		return nil, errors.Wrap(err, "data format error, please check your input!")
	}
	var path []string
	return parseJSON(path, v, space, proMap)
}

func parseJSON(path []string, v *fastjson.Value, space *entity.Space, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.Field, error) {
	fields := make([]*vearchpb.Field, 0)
	obj, err := v.Object()
	if err != nil {
		log.Warnf("data format error, object is required but received %s", v.Type().String())
		return nil, fmt.Errorf("data format error, object is required but received %s", v.Type().String())
	}

	haveNoField := false
	errorField := ""
	haveVector := false
	parseErr := fmt.Errorf("")
	obj.Visit(func(key []byte, val *fastjson.Value) {
		fieldName := string(key)
		pro, ok := proMap[fieldName]
		if !ok {
			haveNoField = true
			errorField = fieldName
			log.Warnf("unrecognizable field, %s is not found in space fields", fieldName)
			return
		}
		if _, ok := FieldsIndex[fieldName]; ok {
			log.Warnf("filed name [%s]  is an internal field that cannot be used", fieldName)
			return
		}
		docV := rutil.GetDocVal()
		if docV == nil {
			docV = &rutil.DocVal{FieldName: fieldName, Path: path}
		} else {
			docV.FieldName = fieldName
			docV.Path = path
		}

		defer func() {
			rutil.PutDocVal(docV)
		}()
		field, err := processProperty(docV, val, space.Engine.RetrievalType, pro)
		if err != nil {
			log.Error("processProperty unrecognizable field:[%s] value %v", fieldName, err)
			parseErr = err
			return
		}
		if field != nil && field.Type == vearchpb.FieldType_VECTOR && field.Value != nil {
			haveVector = true
		}
		fields = append(fields, field)
	})

	if parseErr.Error() != "" {
		return nil, fmt.Errorf("param parse error msg:[%s]", parseErr.Error())
	}

	if haveNoField {
		return nil, fmt.Errorf("param have error field [%s]", errorField)
	}

	if !strings.EqualFold("scalar", space.Engine.DataType) {
		if !haveVector {
			return nil, fmt.Errorf("param have not vector value")
		}
	}

	return fields, nil
}

func processPropertyString(v *fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	propertyValueByte, err := v.StringBytes()
	if err != nil {
		return nil, err
	}

	propertyValueString := string(propertyValueByte)
	if pro != nil {
		field, err := processString(pro, pathString, propertyValueString)
		if err != nil {
			return nil, err
		}
		return field, nil
	} else {
		return nil, fmt.Errorf("unrecognizable field %s %v", pathString, pro)
	}
}

func processPropertyNumber(v *fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	if pro != nil {
		field, err := processNumber(pro, pathString, v)
		if err != nil {
			return nil, err
		}
		return field, nil
	} else {
		return nil, fmt.Errorf("unrecognizable field %s %v", pathString, pro)
	}
}

func processPropertyBool(v *fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	propertyValBool, err := v.Bool()
	if err != nil {
		return nil, err
	}
	if pro != nil {
		field, err := processBool(pro, pathString, propertyValBool)
		if err != nil {
			return nil, err
		}
		return field, nil
	} else {
		return nil, fmt.Errorf("unrecognizable field %s %v", pathString, pro)
	}
}

func processPropertyObjectGEOPOINT(v *fastjson.Value, pathString string, pro *entity.SpaceProperties, latlonV map[string]*fastjson.Value) (*vearchpb.Field, error) {
	lonV := latlonV["lonV"]
	latV := latlonV["latV"]
	lon, err := lonV.Float64()
	if err != nil {
		log.Error("field value %s mismatch geo point, err %v", v.String(), err)
		return nil, fmt.Errorf("field value %s mismatch geo point, err %v", v.String(), err)
	}
	lat, err := latV.Float64()
	if err != nil {
		log.Error("field value %s mismatch geo point, err %v", v.String(), err)
		return nil, fmt.Errorf("field value %s mismatch geo point, err %v", v.String(), err)
	}
	field, err := processGeoPoint(pro, pathString, lon, lat)
	if err != nil {
		return nil, err
	} else {
		return field, err
	}
}

func processPropertyObjectVectorBinary(feature []*fastjson.Value, source []byte, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	vector := make([]uint8, len(feature))
	for i := 0; i < len(feature); i++ {
		if uint8Value, err := feature[i].Int(); err != nil {
			log.Error("vector can not to uint8 %v", feature[i])
			return nil, fmt.Errorf("vector can not to uint8 %v", feature[i])
		} else {
			if uint8Value < 0 || uint8Value > 255 {
				return nil, fmt.Errorf("vector value overflows constant: %v", uint8Value)
			}
			vector[i] = uint8(uint8Value)
		}
	}
	field, err := processVectorBinary(pro, pathString, vector, string(source))
	field.Source = string(source)
	if err != nil {
		log.Error("process vectory binary err:[%s] m value:[%v]", err.Error(), vector)
		return nil, fmt.Errorf("process vectory binary err:[%s] m value:[%v]", err.Error(), vector)
	}
	return field, nil
}

func processPropertyObjectVectorOther(feature []*fastjson.Value, source []byte, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	vector := make([]float32, len(feature))
	for i := 0; i < len(feature); i++ {
		if f64, err := feature[i].Float64(); err != nil {
			log.Error("vector can not to float 64 %v", feature[i])
			return nil, fmt.Errorf("vector can not to float 64 %v", feature[i])
		} else if math.IsNaN(f64) || math.IsInf(f64, 0) {
			log.Error("vector value is index:[%d], err:[ %v]", i, feature[i])
			return nil, fmt.Errorf("vector value is index:[%d], err:[ %v]", i, feature[i])
		} else {
			vector[i] = float32(f64)
		}
	}
	field, err := processVector(pro, pathString, vector, string(source))
	if err != nil {
		log.Error("process vectory err:[%s] m value:[%v]", err.Error(), vector)
		return nil, fmt.Errorf("process vectory err:[%s] m value:[%v]", err.Error(), vector)
	}
	return field, nil
}

func processPropertyArrayVectorGeoPoint(vs []*fastjson.Value, v *fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	field := &vearchpb.Field{Name: ""}
	err := fmt.Errorf("parse param ArrayVectorGeoPoint err field:%s", v.String())
	if len(vs) != 2 {
		log.Error("field value %s mismatch geo point, %v", v.String(), vs)
		field, err = nil, fmt.Errorf("field value %s mismatch geo point, %v", v.String(), vs)
	}
	// Geo-point expressed as an array with the format: [ lon, lat]
	if vs[0].Type() == fastjson.TypeNumber && vs[1].Type() == fastjson.TypeNumber {
		lon, err := vs[0].Float64()
		if err != nil {
			log.Error("field value %s mismatch geo point, lon err %v", v.String(), err)
			field, err = nil, fmt.Errorf("field value %s mismatch geo point, lon err %v", v.String(), err)
		}
		lat, err := vs[1].Float64()
		if err != nil {
			log.Error("field value %s mismatch geo point, lat err %v", v.String(), err)
			field, err = nil, fmt.Errorf("field value %s mismatch geo point, lat err %v", v.String(), err)
		}
		field, err = processGeoPoint(pro, pathString, lon, lat)
	} else {
		log.Error("field value %s mismatch geo point, type is not number err %v", v.String)
		field, err = nil, fmt.Errorf("field value %s mismatch geo point, type is not number err %v", v.String(), err)
	}

	return field, err
}

func processPropertyArrayVectorString(vs []*fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	buffer := bytes.Buffer{}
	for i, vv := range vs {
		if stringBytes, err := vv.StringBytes(); err != nil {
			return nil, err
		} else {
			buffer.Write(stringBytes)
			if i < len(vs)-1 {
				buffer.WriteRune('\001')
			}
		}
	}
	field, err := processString(pro, pathString, buffer.String())
	if err != nil {
		return nil, err
	}
	return field, nil
}

func processPropertyArrayVectorInt(vs []*fastjson.Value, fieldName string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	buffer, opt := processPropertyVectorIntLong(vs, pro)
	field := &vearchpb.Field{
		Name:   fieldName,
		Type:   vearchpb.FieldType_INT,
		Value:  buffer,
		Option: opt,
	}
	return field, nil
}

func processPropertyObject(v *fastjson.Value, pathString string, pro *entity.SpaceProperties, retrievalType string) (*vearchpb.Field, error) {
	field := &vearchpb.Field{Name: ""}
	err := fmt.Errorf("parse param processPropertyObject err retrievalType:%s", retrievalType)
	if pro.FieldType == entity.FieldType_GEOPOINT {
		// Geo-point expressed as an object, with lat and lon keys.
		latV := v.Get("lat")
		lonV := v.Get("lon")
		if latV != nil && lonV != nil {
			if latV.Type() == fastjson.TypeNumber && lonV.Type() == fastjson.TypeNumber {
				latlonV := make(map[string]*fastjson.Value)
				latlonV["latV"] = latV
				latlonV["lonV"] = lonV
				field, err = processPropertyObjectGEOPOINT(v, pathString, pro, latlonV)
			} else {
				log.Error("field value %s mismatch geo point,type is not number", v.String())
				field, err = nil, fmt.Errorf("field value %s mismatch geo point,type is not number", v.String())
			}
		} else {
			log.Error("field value %s mismatch geo point,lat or lon is nil", v.String())
			field, err = nil, fmt.Errorf("field value %s mismatch geo point,lat or lon is nil", v.String())
		}
	} else if pro.FieldType == entity.FieldType_VECTOR {
		source := v.GetStringBytes("source")
		feature := v.GetArray("feature")
		if strings.Compare(retrievalType, "BINARYIVF") == 0 {
			field, err = processPropertyObjectVectorBinary(feature, source, pathString, pro)
		} else {
			field, err = processPropertyObjectVectorOther(feature, source, pathString, pro)
		}
	}
	return field, err
}

func processPropertyArray(v *fastjson.Value, pathString string, pro *entity.SpaceProperties, fieldName string) (*vearchpb.Field, error) {
	field := &vearchpb.Field{Name: fieldName}
	err := fmt.Errorf("parse param processPropertyArray err :%s", fieldName)
	vs, err := v.Array()
	if err != nil {
		field = nil
	}
	if pro.FieldType == entity.FieldType_GEOPOINT {
		field, err = processPropertyArrayVectorGeoPoint(vs, v, pathString, pro)
	} else if pro.FieldType == entity.FieldType_STRING && pro.Array {
		field, err = processPropertyArrayVectorString(vs, pathString, pro)
	} else if pro.FieldType == entity.FieldType_INT && pro.Array {
		field, err = processPropertyArrayVectorInt(vs, fieldName, pro)
	} else if pro.FieldType == entity.FieldType_LONG && pro.Array {
		field, err = processPropertyArrayVectorLong(vs, fieldName, pro)
	} else if pro.FieldType == entity.FieldType_FLOAT && pro.Array {
		field, err = processPropertyArrayVectorFloat(vs, fieldName, pro)
	} else if pro.FieldType == entity.FieldType_DOUBLE && pro.Array {
		field, err = processPropertyArrayVectorDouble(vs, fieldName, pro)
	} else {
		field, err = nil, fmt.Errorf("field:[%s]  this type:[%v] can use by array", fieldName, pro.FieldType)
	}
	return field, err
}

func processPropertyVectorIntLong(vs []*fastjson.Value, pro *entity.SpaceProperties) ([]byte, vearchpb.FieldOption) {
	buffer := bytes.Buffer{}
	for _, vv := range vs {
		buffer.Write(cbbytes.Int64ToByte(vv.GetInt64()))
	}
	opt := vearchpb.FieldOption_Null
	if pro.Option == 1 {
		opt = vearchpb.FieldOption_Index
	}
	return buffer.Bytes(), opt
}

func processPropertyArrayVectorLong(vs []*fastjson.Value, fieldName string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	buffer, opt := processPropertyVectorIntLong(vs, pro)
	field := &vearchpb.Field{
		Name:   fieldName,
		Type:   vearchpb.FieldType_LONG,
		Value:  buffer,
		Option: opt,
	}
	return field, nil
}

func processPropertyArrayVectorFloat(vs []*fastjson.Value, fieldName string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	buffer := bytes.Buffer{}
	for _, vv := range vs {
		buffer.Write(cbbytes.Float32ToByte(float32(vv.GetFloat64())))
	}
	opt := vearchpb.FieldOption_Null
	if pro.Option == 1 {
		opt = vearchpb.FieldOption_Index
	}
	field := &vearchpb.Field{
		Name:   fieldName,
		Type:   vearchpb.FieldType_FLOAT,
		Value:  buffer.Bytes(),
		Option: opt,
	}
	return field, nil
}

func processPropertyArrayVectorDouble(vs []*fastjson.Value, fieldName string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	buffer := bytes.Buffer{}
	for _, vv := range vs {
		buffer.Write(cbbytes.Float64ToByte(vv.GetFloat64()))
	}
	opt := vearchpb.FieldOption_Null
	if pro.Option == 1 {
		opt = vearchpb.FieldOption_Index
	}
	field := &vearchpb.Field{
		Name:   fieldName,
		Type:   vearchpb.FieldType_DOUBLE,
		Value:  buffer.Bytes(),
		Option: opt,
	}
	return field, nil
}

func processProperty(docVal *rutil.DocVal, v *fastjson.Value, retrievalType string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	fieldName := docVal.FieldName
	path := docVal.Path
	if len(path) == 0 && FieldsIndex[fieldName] > 0 {
		log.Error("filed name [%s]  is an internal field that cannot be used", fieldName)
		return nil, fmt.Errorf("filed name [%s]  is an internal field that cannot be used", fieldName)
	}

	pathString := fieldName
	if len(path) > 0 {
		pathString = encodePath(append(path, fieldName))
	}

	if v.Type() == fastjson.TypeNull {
		return nil, fmt.Errorf("filed name [%s]  type is null", fieldName)
	}

	field := &vearchpb.Field{Name: fieldName}
	err := fmt.Errorf("parse param processProperty err :%s", fieldName)

	switch v.Type() {
	case fastjson.TypeString:
		field, err = processPropertyString(v, pathString, pro)
	case fastjson.TypeNumber:
		field, err = processPropertyNumber(v, pathString, pro)
	case fastjson.TypeTrue, fastjson.TypeFalse:
		field, err = processPropertyBool(v, pathString, pro)
	case fastjson.TypeObject:
		field, err = processPropertyObject(v, pathString, pro, retrievalType)
		//parseJson(append(path, fieldName), v, retrievalType,pro)
	case fastjson.TypeArray:
		field, err = processPropertyArray(v, pathString, pro, fieldName)
	}
	return field, err
}

const pathSeparator = "."

func encodePath(pathElements []string) string {
	return strings.Join(pathElements, pathSeparator)
}

func processField(fieldName string, fieldType vearchpb.FieldType, value []byte, opt vearchpb.FieldOption) (*vearchpb.Field, error) {
	field := &vearchpb.Field{
		Name:   fieldName,
		Type:   fieldType,
		Value:  value,
		Option: opt,
	}
	return field, nil
}

func processStringFieldGeoPoint(fieldName string, val string, opt vearchpb.FieldOption) (*vearchpb.Field, error) {
	field := &vearchpb.Field{Name: fieldName}
	err := fmt.Errorf("parse param StringGeoPoint err :%s", fieldName)
	lat, lon, err := rutil.ParseStringToGeoPoint(val)
	if err != nil {
		field = nil
	} else {
		code, err := cbbytes.FloatArrayByte([]float32{float32(lon), float32(lat)})
		if err != nil {
			field = nil
		} else {
			field, err = processField(fieldName, vearchpb.FieldType_GEOPOINT, code, opt)
			return field, err
		}
	}
	return field, err
}

func processString(pro *entity.SpaceProperties, fieldName, val string) (*vearchpb.Field, error) {
	opt := vearchpb.FieldOption_Null
	if pro.Option == 1 {
		opt = vearchpb.FieldOption_Index
	}

	var (
		field *vearchpb.Field
		err   error
	)

	switch pro.FieldType {
	case entity.FieldType_STRING:
		field, err = processField(fieldName, vearchpb.FieldType_STRING, []byte(val), opt)
	case entity.FieldType_DATE:
		// UTC time
		var f time.Time
		f, err = cast.ToTimeE(val)
		if err != nil {
			field, err = nil, fmt.Errorf("parse date %s faield, err %v", val, err)
		} else {
			field, err = processField(fieldName, vearchpb.FieldType_DATE, cbbytes.Int64ToByte(f.UnixNano()), opt)
		}
	case entity.FieldType_INT:
		var i int32
		i, err = cast.ToInt32E(val)
		if err != nil {
			field, err = nil, fmt.Errorf("parse string %s to integer failed, err %v", val, err)
		} else {
			field, err = processField(fieldName, vearchpb.FieldType_INT, cbbytes.Int32ToByte(i), opt)
		}
	case entity.FieldType_LONG:
		var i int64
		i, err = cast.ToInt64E(val)
		if err != nil {
			field, err = nil, fmt.Errorf("parse string %s to long failed, err %v", val, err)
		} else {
			field, err = processField(fieldName, vearchpb.FieldType_LONG, cbbytes.Int64ToByte(i), opt)
		}
	case entity.FieldType_FLOAT:
		var f float32
		f, err = cast.ToFloat32E(val)
		if err != nil {
			field, err = nil, fmt.Errorf("parse string %s to float32 failed, err %v", val, err)
		} else {
			field, err = processField(fieldName, vearchpb.FieldType_FLOAT, cbbytes.Float32ToByte(f), opt)
		}
	case entity.FieldType_DOUBLE:
		var f float64
		f, err = cast.ToFloat64E(val)
		if err != nil {
			field, err = nil, fmt.Errorf("parse string %s to float64 failed, err %v", val, err)
		} else {
			field, err = processField(fieldName, vearchpb.FieldType_DOUBLE, cbbytes.Float64ToByte(f), opt)
		}
	default:
		field, err = nil, fmt.Errorf("parse param processString err :%s", fieldName)
	}
	return field, err
}

func processNumber(pro *entity.SpaceProperties, fieldName string, val *fastjson.Value) (*vearchpb.Field, error) {
	opt := vearchpb.FieldOption_Null
	if pro.Option == 1 {
		opt = vearchpb.FieldOption_Index
	}

	var (
		field *vearchpb.Field
		err   error
	)
	switch pro.FieldType {
	case entity.FieldType_INT:
		var i int
		i, err = val.Int()
		if err != nil {
			return nil, err
		}
		field, err = processField(fieldName, vearchpb.FieldType_INT, cbbytes.Int32ToByte(int32(i)), opt)
	case entity.FieldType_LONG:
		var i int64
		i, err = val.Int64()
		if err != nil {
			return nil, err
		}
		field, err = processField(fieldName, vearchpb.FieldType_LONG, cbbytes.Int64ToByte(i), opt)
	case entity.FieldType_FLOAT:
		var i float64
		i, err = val.Float64()
		if err != nil {
			return nil, err
		}
		field, err = processField(fieldName, vearchpb.FieldType_FLOAT, cbbytes.Float32ToByte(float32(i)), opt)
	case entity.FieldType_DOUBLE:
		var i float64
		i, err = val.Float64()
		if err != nil {
			return nil, err
		}
		field, err = processField(fieldName, vearchpb.FieldType_DOUBLE, cbbytes.Float64ToByteNew(i), opt)
	case entity.FieldType_DATE:
		var i int64
		i, err = val.Int64()
		if err != nil {
			return nil, err
		}
		field, err = processField(fieldName, vearchpb.FieldType_DATE, cbbytes.Int64ToByte(i*1e6), opt)
	default:
		field, err = nil, fmt.Errorf("string mismatch field:[%s] value:[%v] type:[%v] ", fieldName, val, pro.FieldType)
	}
	return field, err
}

func processGeoPoint(pro *entity.SpaceProperties, fieldName string, lon, lat float64) (*vearchpb.Field, error) {
	switch pro.FieldType {
	case entity.FieldType_GEOPOINT:
		code, err := cbbytes.FloatArrayByte([]float32{float32(lon), float32(lat)})
		if err != nil {
			return nil, err
		}
		opt := vearchpb.FieldOption_Null
		if pro.Option == 1 {
			opt = vearchpb.FieldOption_Index
		}
		return processField(fieldName, vearchpb.FieldType_GEOPOINT, code, opt)
	default:
		return nil, fmt.Errorf("string mismatch field:[%s] value:[%f,%f] type:[%v] ", fieldName, lon, lat, pro.FieldType)
	}
}

func processBool(pro *entity.SpaceProperties, fieldName string, val bool) (*vearchpb.Field, error) {
	switch pro.FieldType {
	case entity.FieldType_BOOL:
		opt := vearchpb.FieldOption_Null
		if pro.Option == 1 {
			opt = vearchpb.FieldOption_Index
		}
		return processField(fieldName, vearchpb.FieldType_BOOL, cbbytes.BoolToByte(val), opt)
	default:
		return nil, fmt.Errorf("string mismatch field:[%s] type:[%v] ", fieldName, pro.FieldType)
	}
}

func processVectorBinary(pro *entity.SpaceProperties, fieldName string, val []uint8, source string) (*vearchpb.Field, error) {
	switch pro.FieldType {
	case entity.FieldType_VECTOR:
		if pro.Dimension > 0 && (pro.Dimension)/8 != len(val) {
			return nil, fmt.Errorf("processVectorBinary field:[%s] vector_length err ,schema is:[%d] but input :[%d]", fieldName, pro.Dimension, len(val))
		}

		bs, err := cbbytes.VectorBinaryToByte(val, source)
		if err != nil {
			return nil, err
		}
		opt := vearchpb.FieldOption_Null
		if pro.Option == 1 {
			opt = vearchpb.FieldOption_Index
		}

		return processField(fieldName, vearchpb.FieldType_VECTOR, bs, opt)
	default:
		return nil, fmt.Errorf("processVectorBinary field:[%s] value %v mismatch field type %v", fieldName, val, pro.FieldType)
	}
}

func processVector(pro *entity.SpaceProperties, fieldName string, val []float32, source string) (*vearchpb.Field, error) {
	field := &vearchpb.Field{Name: fieldName}
	err := fmt.Errorf("parse param processVector err,fieldName:%s", fieldName)

	switch pro.FieldType {
	case entity.FieldType_VECTOR:
		if pro.Dimension > 0 && pro.Dimension != len(val) {
			field, err = nil, fmt.Errorf("field:[%s] vector_length err ,schema is:[%d] but input :[%d]", fieldName, pro.Dimension, len(val))
			return field, err
		}

		bs, err := cbbytes.VectorToByte(val, source)
		if err != nil {
			field = nil
			log.Error("processVector VectorToByte error: %v", err)
		} else {
			opt := vearchpb.FieldOption_Null
			if pro.Option == 1 {
				opt = vearchpb.FieldOption_Index
			}
			field, err = processField(fieldName, vearchpb.FieldType_VECTOR, bs, opt)
			field.Source = source
			return field, err
		}
	default:
		field, err = nil, fmt.Errorf("field:[%s] value %v mismatch field type %v", fieldName, val, pro.FieldType)
	}
	return field, err
}

func docParse(ctx context.Context, r *http.Request, space *entity.Space, args *vearchpb.UpdateRequest) (err error) {
	body, err := netutil.GetReqBody(r)
	if err != nil {
		return err
	}
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		spaceProperties = spacePro
	}
	fields, err := MapDocument(body, space, spaceProperties)
	if err != nil {
		return err
	}
	args.Doc = &vearchpb.Document{Fields: fields}
	return nil
}

func docBulkParse(ctx context.Context, r *http.Request, space *entity.Space, args *vearchpb.BulkRequest) (err error) {
	body, err := netutil.GetReqBody(r)
	if err != nil {
		return err
	}
	sr := strings.NewReader(string(body))
	br := bufio.NewScanner(sr)

	docs := make([]*vearchpb.Document, 0)
	for br.Scan() {
		line := string(br.Bytes())
		if len(line) < 1 {
			continue
		}
		jsonMap, err := cbjson.ByteToJsonMap(br.Bytes())
		if err != nil {
			return err
		}
		indexJsonMap := jsonMap.GetJsonMap("index")
		primaryKey := indexJsonMap.GetJsonValString("_id")
		br.Scan()
		source := br.Bytes()
		spaceProperties := space.SpaceProperties
		if spaceProperties == nil {
			spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
			spaceProperties = spacePro
		}
		fields, err := MapDocument(source, space, spaceProperties)
		if err != nil {
			return err
		}
		doc := &vearchpb.Document{PKey: primaryKey, Fields: fields}

		docs = append(docs, doc)
	}
	args.Docs = docs
	return nil
}

func docSearchParse(r *http.Request, space *entity.Space, searchReq *vearchpb.SearchRequest) (err error) {
	reqBodyStart := time.Now()
	reqBody, err := netutil.GetReqBody(r)
	if config.LogInfoPrintSwitch {
		reqBodyCostTime := time.Now().Sub(reqBodyStart).Seconds() * 1000
		reqBodyCostTimeStr := strconv.FormatFloat(reqBodyCostTime, 'f', -1, 64)
		searchReq.Head.Params["reqBodyCostTime"] = reqBodyCostTimeStr
	}
	if err == nil {
		if len(reqBody) != 0 {
			searchDoc := &request.SearchDocumentRequest{}
			err := cbjson.Unmarshal(reqBody, searchDoc)
			if err != nil {
				err = fmt.Errorf("query param convert json err: [%s]", string(reqBody))
			} else {
				err = searchParamToSearchPb(searchDoc, searchReq, space, false)
			}
		} else {
			err = fmt.Errorf("query param is null")
		}
	}
	return
}

func docSearchByIdsParse(r *http.Request, space *entity.Space) (fieldsParam []string, ids []string, reqBodyByte []byte, err error) {
	var error error
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		error = err
	} else {
		if len(reqBody) != 0 {
			reqBodyByte = reqBody
			queryParam := struct {
				Query json.RawMessage `json:"query"`
			}{}

			err1 := cbjson.Unmarshal(reqBody, &queryParam)
			if err1 != nil {
				log.Error("docSearchByIdsParse cbjson.Unmarshal error :%v", err1)
				error = fmt.Errorf("query param Unmarshal error")
				return nil, nil, reqBodyByte, error
			}

			if idIsLong(space) {
				idsArr := struct {
					Ids    []int64  `json:"ids"`
					Fields []string `json:"fields"`
				}{}

				err1 := json.Unmarshal(queryParam.Query, &idsArr)
				if err1 != nil {
					error = fmt.Errorf("query param Unmarshal error")
					return nil, nil, reqBodyByte, error
				}
				fieldsParam = idsArr.Fields
				if len(idsArr.Ids) > 0 {
					ids = make([]string, 0)
					for _, int64id := range idsArr.Ids {
						ids = append(ids, strconv.FormatInt(int64id, 10))
					}
				} else {
					error = fmt.Errorf("query param id is null")
					return nil, nil, reqBodyByte, error
				}
			} else {
				idsArr := struct {
					Ids    []string `json:"ids"`
					Fields []string `json:"fields"`
				}{}

				err1 := json.Unmarshal(queryParam.Query, &idsArr)
				if err1 != nil {
					error = fmt.Errorf("query param Unmarshal error")
					return nil, nil, reqBodyByte, error
				}

				fieldsParam = idsArr.Fields
				if len(idsArr.Ids) > 0 {
					ids = idsArr.Ids
				} else {
					error = fmt.Errorf("query param id is null")
					return nil, nil, reqBodyByte, error
				}
			}
		} else {
			log.Error("len of reqBody: %d", len(reqBody))
			error = fmt.Errorf("len of reqBody: %d", len(reqBody))
			return nil, nil, reqBodyByte, error
		}
	}

	if ids == nil || len(ids) == 0 {
		error = fmt.Errorf("id is empty")
		return nil, nil, reqBodyByte, error
	}

	if len(ids) > 100 {
		error = fmt.Errorf("id max 100 now id is: %d", len(ids))
		return nil, nil, reqBodyByte, error
	}
	return fieldsParam, ids, reqBodyByte, error

}

func arrayToMap(feilds []string) map[string]string {
	mapObj := make(map[string]string)
	for _, feild := range feilds {
		mapObj[feild] = feild
	}
	return mapObj
}

func docSearchByFeaturesParse(space *entity.Space, reqBody []byte, searchReq *vearchpb.SearchRequest, items []*vearchpb.Item) (err error) {

	searchDoc := &request.SearchDocumentRequest{}
	err = cbjson.Unmarshal(reqBody, searchDoc)
	if err != nil {
		err = fmt.Errorf("query param convert json err: [%s]", string(reqBody))
	} else {
		sortOrder, error := searchDoc.SortOrder()
		err = error
		if err == nil {
			sortFieldArr := make([]*vearchpb.SortField, 0)
			for _, sort := range sortOrder {
				sortFieldArr = append(sortFieldArr, &vearchpb.SortField{Field: sort.SortField(), Type: sort.GetSortOrder()})
			}
			searchReq.SortFields = sortFieldArr
			err := searchParamToSearchPb(searchDoc, searchReq, space, true)
			if err == nil {
				queryByte, err := parseQueryForIdFeature(searchDoc.Query, space, items)
				if err == nil {
					err = parseQuery(queryByte, searchReq, space)
				}
			}
		}
	}
	return
}

func docBulkSearchParse(r *http.Request, space *entity.Space, head *vearchpb.RequestHead) (searchReqs []*vearchpb.SearchRequest, err error) {
	var error error
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		error = err
	} else {
		if len(reqBody) != 0 {

			var paramMap []map[string]interface{}
			if err := json.Unmarshal([]byte(reqBody), &paramMap); err != nil {
				log.Error("docBulkSearchParse cbjson.Unmarshal error :%v", err)
				error = fmt.Errorf("query param Unmarshal error")
				return nil, error
			}
			paramMapSize := len(paramMap)
			if paramMapSize > 100 {
				log.Error("docBulkSearchParse param more than 100, param size:%d", paramMapSize)
				error = fmt.Errorf("query param more than 100, param size:%d", paramMapSize)
				return nil, error
			}

			if paramMapSize == 0 {
				log.Error("docBulkSearchParse param less than 1, param size:%d", paramMapSize)
				error = fmt.Errorf("query param less than 1, param size:%d", paramMapSize)
				return nil, error
			}

			var searchRequest request.SearchRequestPo
			if paramMap != nil && paramMapSize > 0 {
				searchDocReqArr := make([]*request.SearchDocumentRequest, paramMapSize)
				for i := 0; i < paramMapSize; i++ {
					searchDocReqArr[i] = &request.SearchDocumentRequest{Parallel: true}
				}
				searchRequest.SearchDocumentRequestArr = searchDocReqArr
			}
			err := cbjson.Unmarshal(reqBody, &searchRequest.SearchDocumentRequestArr)
			if err != nil {
				log.Error("param Unmarshal error :%v", err)
				error = fmt.Errorf("query param Unmarshal error")
			} else {
				//searchRequestArr := make([]*vearchpb.SearchRequest,0)
				for i := 0; i < len(searchRequest.SearchDocumentRequestArr); i++ {
					serchDocReq := searchRequest.SearchDocumentRequestArr[i]
					searchRequest := &vearchpb.SearchRequest{}
					searchRequest.Head = head
					sortOrder, err := serchDocReq.SortOrder()
					if err != nil {
						error = fmt.Errorf("sortorder param error")
						break
					} else {
						sortFieldArr := make([]*vearchpb.SortField, 0)
						for _, sort := range sortOrder {
							sortFieldArr = append(sortFieldArr, &vearchpb.SortField{Field: sort.SortField(), Type: sort.GetSortOrder()})
						}
						searchRequest.SortFields = sortFieldArr
						err = searchParamToSearchPb(serchDocReq, searchRequest, space, false)
						if err == nil {
							searchReqs = append(searchReqs, searchRequest)
						} else {
							error = err
							break
						}
					}
				}
			}
		} else {
			log.Error("len of reqBody: %d", len(reqBody))
			error = fmt.Errorf("len of reqBody: %d", len(reqBody))
		}
	}
	return searchReqs, error
}

func doLogPrintSwitchParse(r *http.Request) (printSwitch bool, err error) {
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		return false, err
	}
	temp := struct {
		PrintSwitch bool `json:"print_switch,omitempty"`
	}{}
	err = json.Unmarshal(reqBody, &temp)
	if err != nil {
		err = fmt.Errorf("doLogPrintSwitchParse param convert json err: [%s]", string(reqBody))
		return false, err
	}
	return temp.PrintSwitch, nil
}

