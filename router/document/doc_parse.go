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

	maxStrLen        = 65535
	maxIndexedStrLen = 255
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
	if pro == nil {
		return nil, fmt.Errorf("unrecognizable field %s %v", pathString, pro)
	}

	propertyValueByte, err := v.StringBytes()
	if err != nil {
		return nil, err
	}

	propertyValueString := string(propertyValueByte)
	field, err := processString(pro, pathString, propertyValueString)
	if err != nil {
		return nil, err
	}
	return field, nil
}

func processPropertyNumber(v *fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	if pro == nil {
		return nil, fmt.Errorf("unrecognizable field %s %v", pathString, pro)
	}
	field, err := processNumber(pro, pathString, v)
	if err != nil {
		return nil, err
	}
	return field, nil
}

func processPropertyBool(v *fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	propertyValBool, err := v.Bool()
	if err != nil {
		return nil, err
	}
	if pro == nil {
		return nil, fmt.Errorf("unrecognizable field %s %v", pathString, pro)
	}
	field, err := processBool(pro, pathString, propertyValBool)
	if err != nil {
		return nil, err
	}
	return field, nil
}

func processPropertyObjectVectorBinary(feature []*fastjson.Value, source []byte, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
	vector := make([]uint8, len(feature))
	for i := 0; i < len(feature); i++ {
		uint8Value, err := feature[i].Int()
		if err != nil {
			log.Error("vector can not to uint8 %v", feature[i])
			return nil, fmt.Errorf("vector can not to uint8 %v", feature[i])
		}
		if uint8Value < 0 || uint8Value > 255 {
			return nil, fmt.Errorf("vector value overflows constant: %v", uint8Value)
		}
		vector[i] = uint8(uint8Value)
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
	if pro.FieldType == entity.FieldType_VECTOR {
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
	vs, err := v.Array()
	if err != nil {
		field = nil
	}
	if pro.FieldType == entity.FieldType_STRING && pro.Array {
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
		field, err = nil, fmt.Errorf("field:[%s]  this type:[%v] can't use as array", fieldName, pro.FieldType)
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
		isIndex := false
		if pro.Index != nil && *pro.Index {
			isIndex = true
		}
		if isIndex && len(val) > maxIndexedStrLen {
			err = fmt.Errorf("indexed string len should less than %d", maxIndexedStrLen)
		} else if len(val) > maxStrLen {
			err = fmt.Errorf("string len should less than %d", maxStrLen)
		} else {
			field, err = processField(fieldName, vearchpb.FieldType_STRING, []byte(val), opt)
		}
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
	if err != nil {
		return
	}
	if config.LogInfoPrintSwitch {
		reqBodyCostTime := time.Since(reqBodyStart).Seconds() * 1000
		reqBodyCostTimeStr := strconv.FormatFloat(reqBodyCostTime, 'f', -1, 64)
		searchReq.Head.Params["reqBodyCostTime"] = reqBodyCostTimeStr
	}
	if len(reqBody) == 0 {
		err = fmt.Errorf("query param is null")
		return
	}

	searchDoc := &request.SearchDocumentRequest{}
	err = cbjson.Unmarshal(reqBody, searchDoc)
	if err != nil {
		err = fmt.Errorf("query param convert json err: [%s]", string(reqBody))
		return
	}
	err = searchParamToSearchPb(searchDoc, searchReq, space, false)
	return
}

func docSearchByIdsParse(r *http.Request, space *entity.Space) (fieldsParam []string, ids []string, reqBodyByte []byte, err error) {
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(reqBody) == 0 {
		log.Error("len of reqBody: %d", len(reqBody))
		err = fmt.Errorf("len of reqBody: %d", len(reqBody))
		return nil, nil, nil, err
	}

	reqBodyByte = reqBody
	queryParam := struct {
		Query json.RawMessage `json:"query"`
	}{}

	err = cbjson.Unmarshal(reqBody, &queryParam)
	if err != nil {
		log.Error("docSearchByIdsParse cbjson.Unmarshal error :%v", err)
		err = fmt.Errorf("docSearchByIdsParse cbjson.Unmarshal error :%v", err)
		return nil, nil, reqBodyByte, err
	}

	if idIsLong(space) {
		idsArr := struct {
			Ids    []int64  `json:"ids"`
			Fields []string `json:"fields"`
		}{}

		err = json.Unmarshal(queryParam.Query, &idsArr)
		if err != nil {
			err = fmt.Errorf("query param Unmarshal error :%v", err)
			return nil, nil, reqBodyByte, err
		}
		fieldsParam = idsArr.Fields
		if len(idsArr.Ids) == 0 {
			err = fmt.Errorf("query param id is null")
			return nil, nil, reqBodyByte, err
		}
		ids = make([]string, 0)
		for _, int64id := range idsArr.Ids {
			ids = append(ids, strconv.FormatInt(int64id, 10))
		}
	} else {
		idsArr := struct {
			Ids    []string `json:"ids"`
			Fields []string `json:"fields"`
		}{}

		err = json.Unmarshal(queryParam.Query, &idsArr)
		if err != nil {
			err = fmt.Errorf("query param Unmarshal error :%v", err)
			return nil, nil, reqBodyByte, err
		}

		fieldsParam = idsArr.Fields
		if len(idsArr.Ids) == 0 {
			err = fmt.Errorf("query param id is null")
			return nil, nil, reqBodyByte, err
		}
		ids = idsArr.Ids
	}

	if len(ids) == 0 {
		err = fmt.Errorf("id is empty")
		return nil, nil, reqBodyByte, err
	}

	if len(ids) > 500 {
		err = fmt.Errorf("id max 500 now id is: %d", len(ids))
		return nil, nil, reqBodyByte, err
	}
	return fieldsParam, ids, reqBodyByte, err
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
		return
	}
	sortOrder, err := searchDoc.SortOrder()
	if err != nil {
		return
	}

	sortFieldArr := make([]*vearchpb.SortField, 0)
	for _, sort := range sortOrder {
		sortFieldArr = append(sortFieldArr, &vearchpb.SortField{Field: sort.SortField(), Type: sort.GetSortOrder()})
	}
	searchReq.SortFields = sortFieldArr
	err = searchParamToSearchPb(searchDoc, searchReq, space, true)
	if err != nil {
		return
	}

	queryByte, err := parseQueryForIdFeature(searchDoc.Query, space, items)
	if err != nil {
		return
	}
	err = parseQuery(queryByte, searchReq, space)
	return
}

func docBulkSearchParse(r *http.Request, space *entity.Space, head *vearchpb.RequestHead) (searchReqs []*vearchpb.SearchRequest, err error) {
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		return
	}

	if len(reqBody) == 0 {
		log.Error("len of reqBody: %d", len(reqBody))
		err = fmt.Errorf("len of reqBody: %d", len(reqBody))
		return
	}

	var paramMap []map[string]interface{}
	if err := json.Unmarshal([]byte(reqBody), &paramMap); err != nil {
		log.Error("docBulkSearchParse cbjson.Unmarshal error :%v", err)
		err = fmt.Errorf("query param Unmarshal error")
		return nil, err
	}
	paramMapSize := len(paramMap)
	if paramMapSize > 100 {
		log.Error("docBulkSearchParse param more than 100, param size:%d", paramMapSize)
		err = fmt.Errorf("query param more than 100, param size:%d", paramMapSize)
		return nil, err
	}

	if paramMapSize == 0 {
		log.Error("docBulkSearchParse param less than 1, param size:%d", paramMapSize)
		err = fmt.Errorf("query param less than 1, param size:%d", paramMapSize)
		return nil, err
	}

	var searchRequest request.SearchRequestPo
	if paramMap != nil && paramMapSize > 0 {
		searchDocReqArr := make([]*request.SearchDocumentRequest, paramMapSize)
		for i := 0; i < paramMapSize; i++ {
			searchDocReqArr[i] = &request.SearchDocumentRequest{Parallel: true}
		}
		searchRequest.SearchDocumentRequestArr = searchDocReqArr
	}
	err = cbjson.Unmarshal(reqBody, &searchRequest.SearchDocumentRequestArr)
	if err != nil {
		log.Error("param Unmarshal error :%v", err)
		err = fmt.Errorf("query param Unmarshal error")
		return nil, err
	}

	for i := 0; i < len(searchRequest.SearchDocumentRequestArr); i++ {
		serchDocReq := searchRequest.SearchDocumentRequestArr[i]
		searchRequest := &vearchpb.SearchRequest{}
		searchRequest.Head = head
		sortOrder, err := serchDocReq.SortOrder()
		if err != nil {
			break
		}
		sortFieldArr := make([]*vearchpb.SortField, 0)
		for _, sort := range sortOrder {
			sortFieldArr = append(sortFieldArr, &vearchpb.SortField{Field: sort.SortField(), Type: sort.GetSortOrder()})
		}
		searchRequest.SortFields = sortFieldArr
		err = searchParamToSearchPb(serchDocReq, searchRequest, space, false)
		if err != nil {
			break
		}
		searchReqs = append(searchReqs, searchRequest)
	}
	return searchReqs, err
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

func documentHeadParse(r *http.Request) (docRequest *request.DocumentRequest, dbName string, spaceName string, err error) {
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		return nil, "", "", err
	}

	if len(reqBody) == 0 {
		err = fmt.Errorf("document param is null")
		return nil, "", "", err
	}

	docRequest = &request.DocumentRequest{}
	err = cbjson.Unmarshal(reqBody, docRequest)
	if err != nil {
		err = fmt.Errorf("document param convert json err: [%s]", string(reqBody))
		return nil, "", "", err
	}

	return docRequest, docRequest.DbName, docRequest.SpaceName, nil
}

func documentParse(r *http.Request, docRequest *request.DocumentRequest, space *entity.Space, args *vearchpb.BulkRequest) (err error) {
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		spaceProperties = spacePro
	}

	docs := make([]*vearchpb.Document, 0)
	for _, docJson := range docRequest.Documents {
		jsonMap, err := cbjson.ByteToJsonMap(docJson)
		if err != nil {
			return err
		}
		primaryKey := jsonMap.GetJsonValString("id")

		fields, err := MapDocument(docJson, space, spaceProperties)
		if err != nil {
			return err
		}
		doc := &vearchpb.Document{PKey: primaryKey, Fields: fields}

		docs = append(docs, doc)
	}
	args.Docs = docs
	return nil
}

func documentRequestParse(r *http.Request, searchReq *vearchpb.SearchRequest) (searchDoc *request.SearchDocumentRequest, fileds []string, document_ids []string, err error) {
	reqBodyStart := time.Now()
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		return nil, nil, nil, err
	}
	if config.LogInfoPrintSwitch {
		reqBodyCostTime := time.Since(reqBodyStart).Seconds() * 1000
		reqBodyCostTimeStr := strconv.FormatFloat(reqBodyCostTime, 'f', -1, 64)
		searchReq.Head.Params["reqBodyCostTime"] = reqBodyCostTimeStr
	}
	if len(reqBody) == 0 {
		err = fmt.Errorf("query param is null")
		return nil, nil, nil, err
	}

	searchDoc = &request.SearchDocumentRequest{}
	err = cbjson.Unmarshal(reqBody, searchDoc)
	if err != nil {
		err = fmt.Errorf("query param convert json err: [%s]", string(reqBody))
		return nil, nil, nil, err
	}

	type Query struct {
		DocumentIds []string `json:"document_ids,omitempty"`
		Fields      []string `json:"fields"`
	}
	query := &Query{}
	err = cbjson.Unmarshal(searchDoc.Query, query)
	if err != nil {
		log.Error("documentRequestParse cbjson.Unmarshal error :%v", err)
		err = fmt.Errorf("documentRequestParse cbjson.Unmarshal error :%v", err)
		return nil, nil, nil, err
	}
	return searchDoc, query.Fields, query.DocumentIds, nil
}

func requestToPb(searchDoc *request.SearchDocumentRequest, space *entity.Space, searchReq *vearchpb.SearchRequest) (err error) {
	err = searchParamToSearchPb(searchDoc, searchReq, space, false)
	return err
}
