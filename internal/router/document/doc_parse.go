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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/valyala/fastjson"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/entity/request"
	"github.com/vearch/vearch/internal/pkg/cbbytes"
	"github.com/vearch/vearch/internal/pkg/log"
	"github.com/vearch/vearch/internal/pkg/netutil"
	"github.com/vearch/vearch/internal/pkg/vjson"
	"github.com/vearch/vearch/internal/proto/vearchpb"
)

const (
	// key index field
	IDField = "_id"

	maxStrLen        = 65535
	maxIndexedStrLen = 1024
)

// fields index map
var FieldsIndex = map[string]int{
	// value index
	IDField: 1,
}

// parse doc
func MapDocument(source []byte, space *entity.Space, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.Field, int, error) {
	var fast fastjson.Parser
	v, err := fast.ParseBytes(source)
	if err != nil {
		log.Warnf("bytes transform to json failed when inserting, err: %s ,data:%s", err.Error(), string(source))
		return nil, 0, errors.Wrap(err, "data format error, please check your input!")
	}
	var path []string
	return parseJSON(path, v, space, proMap)
}

func parseJSON(path []string, v *fastjson.Value, space *entity.Space, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.Field, int, error) {
	fields := make([]*vearchpb.Field, 0)
	obj, err := v.Object()
	if err != nil {
		log.Warnf("data format error, object is required but received %s", v.Type().String())
		return nil, 0, fmt.Errorf("data format error, object is required but received %s", v.Type().String())
	}

	haveNoField := false
	errorField := ""
	haveVector := 0
	parseErr := fmt.Errorf("")

	obj.Visit(func(key []byte, val *fastjson.Value) {
		fieldName := string(key)
		if fieldName == IDField {
			return
		}
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
		docV := GetDocVal()
		if docV == nil {
			docV = &DocVal{FieldName: fieldName, Path: path}
		} else {
			docV.FieldName = fieldName
			docV.Path = path
		}

		defer func() {
			PutDocVal(docV)
		}()
		field, err := processProperty(docV, val, space.Index.Type, pro)
		if err != nil {
			log.Error("processProperty parse field:[%s] err: %v", fieldName, err)
			parseErr = err
			return
		}
		if field != nil && field.Type == vearchpb.FieldType_VECTOR && field.Value != nil {
			haveVector += 1
		}
		fields = append(fields, field)
	})

	if parseErr.Error() != "" {
		return nil, haveVector, fmt.Errorf("%s", parseErr.Error())
	}

	if haveNoField {
		return nil, haveVector, fmt.Errorf("unrecognizable field, %s is not found in space fields", errorField)
	}

	return fields, haveVector, nil
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

func processPropertyObjectVectorBinary(feature []*fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
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
	field, err := processVectorBinary(pro, pathString, vector)
	if err != nil {
		log.Error("process vector binary err:[%s] vector value:[%v]", err.Error(), vector)
		return nil, fmt.Errorf("process vector binary err:[%s] vector value:[%v]", err.Error(), vector)
	}
	return field, nil
}

func processPropertyObjectVectorOther(feature []*fastjson.Value, pathString string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
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
	field, err := processVector(pro, pathString, vector)
	if err != nil {
		log.Error("%s vector value:[%v]", err.Error(), vector)
		return nil, fmt.Errorf("%s vector value:[%v]", err.Error(), vector)
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

func processPropertyObject() (*vearchpb.Field, error) {
	return nil, nil
}

func processPropertyArray(v *fastjson.Value, pathString string, pro *entity.SpaceProperties, fieldName string, indexType string) (*vearchpb.Field, error) {
	field := &vearchpb.Field{Name: fieldName}
	vs, err := v.Array()
	if err != nil {
		return nil, err
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
	} else if pro.FieldType == entity.FieldType_VECTOR {
		if len(vs) == 0 {
			err := fmt.Errorf("vector field %s feature value should be arrry, but is: %v", pathString, v)
			return field, err
		}
		if indexType == "BINARYIVF" {
			field, err = processPropertyObjectVectorBinary(vs, pathString, pro)
		} else {
			field, err = processPropertyObjectVectorOther(vs, pathString, pro)
		}
	} else {
		field, err = nil, fmt.Errorf("field:[%s] type:[%v] can't use as array", fieldName, pro.FieldType.String())
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

func processProperty(docVal *DocVal, v *fastjson.Value, indexType string, pro *entity.SpaceProperties) (*vearchpb.Field, error) {
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
		field, err = processPropertyObject()
	case fastjson.TypeArray:
		field, err = processPropertyArray(v, pathString, pro, fieldName, indexType)
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
		if pro.Index != nil {
			isIndex = true
		}
		if isIndex && len(val) > maxIndexedStrLen {
			err = fmt.Errorf("string field %s indexed, length should less than %d", fieldName, maxIndexedStrLen)
		} else if len(val) > maxStrLen {
			err = fmt.Errorf("string field %s length should less than %d", fieldName, maxStrLen)
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
		field, err = nil, fmt.Errorf("field[%s] value mismatch, value:[%v] type:[%v]", fieldName, val, pro.FieldType)
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
		return nil, fmt.Errorf("field:[%s] value mismatch, type:[%s]", fieldName, pro.FieldType)
	}
}

func processVectorBinary(pro *entity.SpaceProperties, fieldName string, val []uint8) (*vearchpb.Field, error) {
	switch pro.FieldType {
	case entity.FieldType_VECTOR:
		if pro.Dimension > 0 && (pro.Dimension)/8 != len(val) {
			return nil, fmt.Errorf("processVectorBinary field:[%s] vector length has error, dimension in space is:[%d] but input length:[%d]", fieldName, pro.Dimension, len(val))
		}

		bs, err := cbbytes.VectorBinaryToByte(val)
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

func processVector(pro *entity.SpaceProperties, fieldName string, val []float32) (*vearchpb.Field, error) {
	field := &vearchpb.Field{Name: fieldName}
	err := fmt.Errorf("parse param processVector err,fieldName:%s", fieldName)

	switch pro.FieldType {
	case entity.FieldType_VECTOR:
		if pro.Dimension > 0 && pro.Dimension != len(val) {
			field, err = nil, fmt.Errorf("field:[%s] vector length has error, dimension in space is:[%d] but input length:[%d]", fieldName, pro.Dimension, len(val))
			return field, err
		}

		bs, err := cbbytes.VectorToByte(val)
		if err != nil {
			field = nil
			log.Error("processVector VectorToByte error: %v", err)
		} else {
			opt := vearchpb.FieldOption_Null
			if pro.Option == 1 {
				opt = vearchpb.FieldOption_Index
			}
			field, err = processField(fieldName, vearchpb.FieldType_VECTOR, bs, opt)
			return field, err
		}
	default:
		field, err = nil, fmt.Errorf("field:[%s] value %v mismatch field type %v", fieldName, val, pro.FieldType)
	}
	return field, err
}

func arrayToMap(feilds []string) map[string]string {
	mapObj := make(map[string]string)
	for _, feild := range feilds {
		mapObj[feild] = feild
	}
	return mapObj
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
		err = fmt.Errorf("doLogPrintSwitchParse param convert json %s err: %v", string(reqBody), err)
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
	err = vjson.Unmarshal(reqBody, docRequest)
	if err != nil {
		err = fmt.Errorf("documentRequest param convert json %s err: %v", string(reqBody), err)
		return nil, "", "", err
	}

	return docRequest, docRequest.DbName, docRequest.SpaceName, nil
}

func documentParse(ctx context.Context, handler *DocumentHandler, r *http.Request, docRequest *request.DocumentRequest, space *entity.Space, args *vearchpb.BulkRequest) (err error) {
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spaceProperties, _ = entity.UnmarshalPropertyJSON(space.Fields)
	}
	vectorFieldNum := 0
	for _, value := range spaceProperties {
		if value.FieldType == vearchpb.FieldType_VECTOR {
			vectorFieldNum += 1
		}
	}
	docs := make([]*vearchpb.Document, 0)
	for _, docJson := range docRequest.Documents {
		jsonMap, err := vjson.ByteToJsonMap(docJson)
		if err != nil {
			return err
		}
		primaryKey := jsonMap.GetJsonValString(IDField)

		fields, haveVector, err := MapDocument(docJson, space, spaceProperties)
		if err != nil {
			return err
		}

		if haveVector != vectorFieldNum {
			if primaryKey == "" {
				err = fmt.Errorf("vector field num:%d is not equal to vector num of space fields:%d and document_id is empty", haveVector, vectorFieldNum)
				return err
			}
			arg := &vearchpb.GetRequest{}
			uriParams := make(map[string]string)
			uriParams["db_name"] = args.Head.DbName
			uriParams["space_name"] = args.Head.SpaceName
			uriParams["_id"] = primaryKey
			uriParamsMap := netutil.NewMockUriParams(uriParams)
			arg.Head = setRequestHead(uriParamsMap, r)
			arg.PrimaryKeys = make([]string, 1)
			arg.PrimaryKeys[0] = primaryKey
			reply := handler.docService.getDocs(ctx, arg)

			_, err := docGetResponse(handler.client, arg, reply, nil, false)
			if err != nil {
				return err
			}

			err = fmt.Errorf("document not exist so cann't update")
			if reply == nil {
				return err
			}
			if len(reply.Items) == 0 {
				return err
			}
			if reply.Items[0].Err != nil && reply.Items[0].Err.Code != vearchpb.ErrorEnum_SUCCESS {
				return err
			}
		}
		doc := &vearchpb.Document{PKey: primaryKey, Fields: fields}

		docs = append(docs, doc)
	}
	args.Docs = docs
	if len(args.Docs) == 0 {
		err = fmt.Errorf("empty documents, should set at least one document")
		return err
	}
	return nil
}

func documentRequestParse(r *http.Request) (searchDoc *request.SearchDocumentRequest, err error) {
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		return nil, err
	}
	if len(reqBody) == 0 {
		err = fmt.Errorf("query param is null")
		return nil, err
	}

	searchDoc = &request.SearchDocumentRequest{}
	err = vjson.Unmarshal(reqBody, searchDoc)
	if err != nil {
		err = fmt.Errorf("SearchDocumentRequest param convert json %s err: %v", string(reqBody), err)
		return nil, err
	}

	return searchDoc, nil
}

func IndexRequestParse(r *http.Request) (index *request.IndexRequest, err error) {
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		return nil, err
	}
	if len(reqBody) == 0 {
		err = fmt.Errorf("index param is null")
		return nil, err
	}

	index = &request.IndexRequest{}
	err = vjson.Unmarshal(reqBody, index)
	if err != nil {
		err = fmt.Errorf("index param convert json %s err: %v", string(reqBody), err)
		return nil, err
	}
	return index, nil
}
