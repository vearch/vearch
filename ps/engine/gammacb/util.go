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

package gammacb

/*
#cgo CFLAGS : -Ilib/include
#cgo LDFLAGS: -Llib/lib -lgamma

#include "gamma_api.h"
*/
import "C"
import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/engine/idl/fbs-gen/go/gamma_api"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/ps/engine/register"
	"github.com/vearch/vearch/ps/engine/sortorder"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/log"
)

var INT, LONG, FLOAT, DOUBLE, STRING, VECTOR C.enum_DataType = C.INT, C.LONG, C.FLOAT, C.DOUBLE, C.STRING, C.VECTOR

var empty = []byte{0}

//get cgo byte array
func byteArray(bytes []byte) *C.struct_ByteArray {
	if len(bytes) == 0 {
		return C.MakeByteArray((*C.char)(unsafe.Pointer(&empty[0])), C.int(len(bytes)))
	}

	return C.MakeByteArray((*C.char)(unsafe.Pointer(&bytes[0])), C.int(len(bytes)))
}

func byteArrayStr(str string) *C.struct_ByteArray {
	return byteArray([]byte(str))
}

func byteArrayInt64(intV int64) *C.struct_ByteArray {
	return byteArray(int64ToBytes(intV))
}

func int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func bytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func newField(name string, value []byte, typed C.enum_DataType) *C.struct_Field {
	return C.MakeField(byteArrayStr(name), byteArray(value), nil, typed)
}

func newFieldBySource(name string, value []byte, source string, typed C.enum_DataType) *C.struct_Field {
	result := newField(name, value, typed)
	if source != "" {
		result.source = byteArrayStr(source)
	}
	return result
}

func mapping2Table(cfg register.EngineConfig, m *mapping.IndexMapping) (*C.struct_Table, error) {
	vfs := make([]*C.struct_VectorInfo, 0)
	fs := make([]*C.struct_FieldInfo, 0)

	engine := cfg.Space.Engine
	idTypeStr := engine.IdType
	idIsLong := false
	if idTypeStr != "" && ("long" == idTypeStr || "Long" == idTypeStr) {
		idIsLong = true
		fs = append(fs, C.MakeFieldInfo(byteArrayStr(mapping.IdField), LONG, C.char(0)))
	} else {
		fs = append(fs, C.MakeFieldInfo(byteArrayStr(mapping.IdField), STRING, C.char(0)))
	}

	//fs = append(fs, C.MakeFieldInfo(byteArrayStr(mapping.VersionField), LONG, C.char(0)))
	//fs = append(fs, C.MakeFieldInfo(byteArrayStr(mapping.SlotField), INT, C.char(0)))

	err := m.SortRangeField(func(key string, value *mapping.DocumentMapping) error {

		switch value.Field.FieldType() {
		case pspb.FieldType_STRING:
			value.Field.Options()
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), STRING, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_FLOAT:
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), DOUBLE, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_DATE:
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), LONG, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_LONG:
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), LONG, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_INT:
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), INT, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_BOOL:
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), INT, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_VECTOR:
			fieldMapping := value.Field.FieldMappingI.(*mapping.VectortFieldMapping)
			hasSource := C.char(0)
			if fieldMapping.HasSource {
				hasSource = C.char(1)
			}
			vf := C.MakeVectorInfo(byteArrayStr(key), VECTOR, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index), C.int(fieldMapping.Dimension), byteArrayStr(fieldMapping.ModelId), byteArrayStr(fieldMapping.StoreType), byteArray(fieldMapping.StoreParam), hasSource)
			vfs = append(vfs, vf)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	table := &C.struct_Table{name: byteArrayStr(cast.ToString(cfg.PartitionID))}

	if len(vfs) > 0 {
		arr := C.MakeVectorInfos(C.int(len(vfs)))
		for i, f := range vfs {
			C.SetVectorInfo(arr, C.int(i), f)
		}
		table.vectors_info = arr
		table.vectors_num = C.int(len(vfs))
	}

	if len(fs) > 0 {
		arr := C.MakeFieldInfos(C.int(len(fs)))

		for i, f := range fs {
			log.Info("add field:[%s] option:[%s]", CbArr2ByteArray(f.name), C.int(f.is_index))
			C.SetFieldInfo(arr, C.int(i), f)
		}

		table.fields = arr
		table.fields_num = C.int(len(fs))
	}

	/*metricType := 1
	switch engine.MetricType {
	case "InnerProduct":
		metricType = 0
	case "L2":
		metricType = 1
	default:
		return nil, fmt.Errorf("metric_type only support `InnerProduct` ,`L2`")
	}*/

	//table.ivfpq_param = C.MakeIVFPQParameters(C.int(metricType), C.int(*engine.Nprobe), C.int(*engine.Ncentroids), C.int(*engine.Nsubvector), C.int(*engine.NbitsPerIdx))
	table.retrieval_type = byteArrayStr(engine.RetrievalType)
	table.retrieval_param = byteArray(engine.RetrievalParam)
	//check id_type
	idType := 0
	if idIsLong {
		idType = 1
	}
	table.id_type = (C.uchar)(idType)
	return table, nil
}

//create doc
func DocCmd2Document(docCmd *pspb.DocCmd, idType string) (*C.struct_Doc, error) {

	/*if docCmd.Version <= 0 {
		docCmd.Version = 1
	}*/

	fields := make([]*C.struct_Field, 0, len(docCmd.Fields)+2)

	if idType != "" && ("long" == idType || "Long" == idType) {
		int64Id, err := strconv.ParseInt(docCmd.DocId, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("table id is long but docId is string , docId Convert long error")
		}

		if toByte, e := cbbytes.ValueToByte(int64Id); e != nil {
			return nil, e
		} else {
			fields = append(fields, newField(mapping.IdField, toByte, LONG))
		}
	} else {
		fields = append(fields, newField(mapping.IdField, []byte(docCmd.DocId), STRING))
	}

	//version
	/*if toByte, e := cbbytes.ValueToByte(docCmd.Version); e != nil {
		return nil, e
	} else {
		fields = append(fields, newField(mapping.VersionField, toByte, LONG))
	}

	if toByte, e := cbbytes.ValueToByte(docCmd.Slot); e != nil {
		return nil, e
	} else {
		fields = append(fields, newField(mapping.SlotField, toByte, INT))
	}*/

	checkVectorHave := false
	for _, f := range docCmd.Fields {

		if f.Value == nil {
			return nil, fmt.Errorf("miss field value by name:%s", f.Name)
		}

		if mapping.FieldsIndex[f.Name] > 0 {
			continue
		}

		switch f.Type {
		case pspb.FieldType_STRING:
			fields = append(fields, newField(f.Name, f.Value, STRING))
		case pspb.FieldType_FLOAT:
			fields = append(fields, newField(f.Name, f.Value, FLOAT))
		case pspb.FieldType_DATE:
			fields = append(fields, newField(f.Name, f.Value, LONG))
		case pspb.FieldType_INT:
			fields = append(fields, newField(f.Name, f.Value, INT))
		case pspb.FieldType_LONG:
			fields = append(fields, newField(f.Name, f.Value, LONG))
		case pspb.FieldType_BOOL:
			fields = append(fields, newField(f.Name, f.Value, INT))
		case pspb.FieldType_VECTOR:
			length := int(cbbytes.ByteToUInt32(f.Value))
			if length > 0 {
				checkVectorHave = true
			}
			fields = append(fields, newFieldBySource(f.Name, f.Value[4:length+4], string(f.Value[length+4:]), VECTOR))
		default:
			log.Debug("gamma invalid field type:[%v]", f.Type)
		}
	}

	if !checkVectorHave {
		return nil, fmt.Errorf("insert field data no vector please check")
	}

	arr := C.MakeFields(C.int(len(fields)))
	for i, f := range fields {
		C.SetField(arr, C.int(i), f)
	}

	return &C.struct_Doc{fields: arr, fields_num: C.int(len(fields))}, nil
}

func (ge *gammaEngine) Doc2DocResultCGO(doc *C.struct_Doc, idIsLong bool) *response.DocResult {

	result := response.DocResult{
		Found:     true,
		DB:        ge.GetSpace().DBId,
		Space:     ge.GetSpace().Id,
		Partition: ge.GetPartitionID(),
	}

	fieldNum := int(doc.fields_num)

	source := make(map[string]interface{})

	dimension := ge.indexMapping.GetField("vector").FieldMappingI.(*mapping.VectortFieldMapping).Dimension

	var err error

	for i := 0; i < fieldNum; i++ {
		fv := C.GetField(doc, C.int(i))
		name := string(CbArr2ByteArray(fv.name))

		switch name {
		/*		case mapping.VersionField:
					result.Version = int64(cbbytes.ByteArray2UInt64(CbArr2ByteArray(fv.value)))
				case mapping.SlotField:
					result.SlotID = uint32(cbbytes.ByteArray2UInt64(CbArr2ByteArray(fv.value)))*/
		case mapping.IdField:
			if idIsLong {
				id := int64(cbbytes.ByteArray2UInt64(CbArr2ByteArray(fv.value)))
				result.Id = strconv.FormatInt(id, 10)
			} else {
				result.Id = string(CbArr2ByteArray(fv.value))
			}
		default:
			field := ge.GetMapping().GetField(name)
			if field == nil {
				log.Error("can not found mappping by field:[%s]", name)
				continue
			}
			switch field.FieldType() {
			case pspb.FieldType_STRING:
				tempValue := string(CbArr2ByteArray(fv.value))
				if field.FieldMappingI.(*mapping.StringFieldMapping).Array {
					source[name] = strings.Split(tempValue, string([]byte{'\001'}))
				} else {
					source[name] = tempValue
				}
			case pspb.FieldType_INT:
				source[name] = cbbytes.Bytes2Int32(CbArr2ByteArray(fv.value))
			case pspb.FieldType_LONG:
				source[name] = cbbytes.Bytes2Int(CbArr2ByteArray(fv.value))
			case pspb.FieldType_BOOL:
				if cbbytes.Bytes2Int(CbArr2ByteArray(fv.value)) == 0 {
					source[name] = false
				} else {
					source[name] = true
				}
			case pspb.FieldType_DATE:
				u := cbbytes.Bytes2Int(CbArr2ByteArray(fv.value))
				source[name] = time.Unix(u/1e6, u%1e6)
			case pspb.FieldType_FLOAT:
				source[name] = cbbytes.ByteToFloat64(CbArr2ByteArray(fv.value))
			case pspb.FieldType_VECTOR:
				if ge.space.Engine.RetrievalType == "BINARYIVF" {
					featureByteC := CbArr2ByteArray(fv.value)

					unit8s, uri, err := cbbytes.ByteToVectorBinary(featureByteC, dimension)
					if err != nil {
						return response.NewErrDocResult(result.Id, err)
					}
					source[name] = map[string]interface{}{
						"source":  uri,
						"feature": unit8s,
					}
				} else {
					float32s, uri, err := cbbytes.ByteToVector(CbArr2ByteArray(fv.value))
					if err != nil {
						return response.NewErrDocResult(result.Id, err)
					}
					source[name] = map[string]interface{}{
						"source":  uri,
						"feature": float32s,
					}
				}

			default:
				log.Warn("can not set value by type:[%v] ", field.FieldType())
			}
		}
	}
	marshal, err := json.Marshal(source)
	if err != nil {
		return response.NewErrDocResult(result.Id, err)
	}
	result.Source = marshal

	/*if marshal, err := json.Marshal(source); err != nil {
		log.Warn("can not marshl source :[%v] ", err.Error())
	} else {
		result.Source = marshal
	}*/

	return &result
}

func (ge *gammaEngine) ResultItem2DocResult(item *gamma_api.ResultItem, idType string) *response.DocResult {
	result := ge.Doc2DocResult(item, idType)
	result.Score = float64(item.Score())
	result.Extra = item.Extra()
	result.SortValues = []sortorder.SortValue{
		&sortorder.FloatSortValue{
			Val: result.Score,
		},
	}
	return result
}

func (ge *gammaEngine) Doc2DocResult(item *gamma_api.ResultItem, idType string) *response.DocResult {

	result := response.DocResult{
		Found:     true,
		DB:        ge.GetSpace().DBId,
		Space:     ge.GetSpace().Id,
		Partition: ge.GetPartitionID(),
	}

	fieldNum := item.NameLength()
	source := make(map[string]interface{})

	dimension := ge.indexMapping.GetField("vector").FieldMappingI.(*mapping.VectortFieldMapping).Dimension

	var err error

	for i := 0; i < fieldNum; i++ {

		name := string(item.Name(i))
		value := item.Value(i)

		switch name {
		/*case mapping.VersionField:
			result.Version = int64(cbbytes.ByteArray2UInt64(value))
		case mapping.SlotField:
			result.SlotID = uint32(cbbytes.ByteArray2UInt64(value))*/
		case mapping.IdField:
			if idType != "" && ("long" == idType || "Long" == idType) {
				id := int64(cbbytes.ByteArray2UInt64(value))
				result.Id = strconv.FormatInt(id, 10)
			} else {
				result.Id = string(value)
			}
		case mapping.SourceField:
			result.Source = value
		default:
			field := ge.GetMapping().GetField(name)
			if field == nil {
				log.Error("can not found mappping by field:[%s]", name)
				continue
			}
			switch field.FieldType() {
			case pspb.FieldType_STRING:
				tempValue := string(value)
				if field.FieldMappingI.(*mapping.StringFieldMapping).Array {
					source[name] = strings.Split(tempValue, string([]byte{'\001'}))
				} else {
					source[name] = tempValue
				}
			case pspb.FieldType_INT:
				source[name] = cbbytes.ByteToUInt32(value)
			case pspb.FieldType_LONG:
				source[name] = cbbytes.ByteToUInt64(value)
			case pspb.FieldType_BOOL:
				if cbbytes.Bytes2Int(value) == 0 {
					source[name] = false
				} else {
					source[name] = true
				}
			case pspb.FieldType_DATE:
				u := cbbytes.Bytes2Int(value)
				source[name] = time.Unix(u/1e6, u%1e6)
			case pspb.FieldType_FLOAT:
				source[name] = cbbytes.ByteToFloat64(value)
			case pspb.FieldType_VECTOR:
				if ge.space.Engine.RetrievalType == "BINARYIVF" {
					unit8s, uri, err := cbbytes.ByteToVectorBinary(value, dimension)
					if err != nil {
						return response.NewErrDocResult(result.Id, err)
					}
					source[name] = map[string]interface{}{
						"source":  uri,
						"feature": unit8s,
					}
				} else {
					float32s, uri, err := cbbytes.ByteToVector(value)
					if err != nil {
						return response.NewErrDocResult(result.Id, err)
					}
					source[name] = map[string]interface{}{
						"source":  uri,
						"feature": float32s,
					}
				}

			default:
				log.Warn("can not set value by type:[%v] ", field.FieldType())
			}
		}
	}
	marshal, err := json.Marshal(source)
	if err != nil {
		return response.NewErrDocResult(result.Id, err)
	}
	result.Source = marshal

	if marshal, err := json.Marshal(source); err != nil {
		log.Warn("can not marshl source :[%v] ", err.Error())
	} else {
		result.Source = marshal
	}

	return &result
}

func (ge *gammaEngine) DocCmd2WriteResult(docCmd *pspb.DocCmd, gammaStartTime time.Time) *response.DocResult {
	return &response.DocResult{
		Id:        docCmd.DocId,
		DB:        ge.GetSpace().DBId,
		Space:     ge.GetSpace().Id,
		Found:     true,
		Partition: ge.GetPartitionID(),
		Version:   docCmd.Version,
		SlotID:    docCmd.Slot,
		Type:      docCmd.Type,
		CostTime:  &response.CostTime{GammaStartTime: gammaStartTime, GammaEndTime: time.Now()},
	}
}

//make c byte array to go byte array
func CbArr2ByteArray(arr *C.struct_ByteArray) []byte {
	if arr == nil {
		return []byte{}
	}
	var oids []byte
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&oids)))
	sliceHeader.Cap = int(arr.len)
	sliceHeader.Len = int(arr.len)
	sliceHeader.Data = uintptr(unsafe.Pointer(arr.value))
	return cbbytes.CloneBytes(oids)
}

func CbArr2ByteArrayUnsafe(arr *C.struct_ByteArray) []byte {
	if arr == nil {
		return []byte{}
	}
	var oids []byte
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&oids)))
	sliceHeader.Cap = int(arr.len)
	sliceHeader.Len = int(arr.len)
	sliceHeader.Data = uintptr(unsafe.Pointer(arr.value))
	return oids
}

func rowDateToFloatArray(data []byte, dimension int) ([]float32, error) {

	if len(data) < dimension {
		return nil, fmt.Errorf("vector query length err, need feature num:[%d]", dimension)
	}

	var result []float32

	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func rowDateToUInt8Array(data []byte, dimension int) ([]uint8, error) {

	if len(data) < dimension {
		return nil, fmt.Errorf("vector query length err, need feature num:[%d]", dimension)
	}

	var result []uint8

	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}
