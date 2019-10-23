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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spf13/cast"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/ps/engine/register"
	"github.com/vearch/vearch/ps/engine/sortorder"
	"github.com/vearch/vearch/util/bytes"
	"reflect"
	"strings"
	"time"
	"unsafe"
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

	fs = append(fs, C.MakeFieldInfo(byteArrayStr(mapping.IdField), STRING, C.char(0)))
	fs = append(fs, C.MakeFieldInfo(byteArrayStr(mapping.SourceField), STRING, C.char(0)))
	fs = append(fs, C.MakeFieldInfo(byteArrayStr(mapping.VersionField), LONG, C.char(0)))
	fs = append(fs, C.MakeFieldInfo(byteArrayStr(mapping.SlotField), INT, C.char(0)))

	err := m.RangeField(func(key string, value *mapping.DocumentMapping) error {

		switch value.Field.FieldType() {
		case pspb.FieldType_KEYWORD:
			value.Field.Options()
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), STRING, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_FLOAT:
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), DOUBLE, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_INT, pspb.FieldType_DATE:
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), LONG, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_BOOL:
			fs = append(fs, C.MakeFieldInfo(byteArrayStr(key), INT, C.char((value.Field.Options()&pspb.FieldOption_Index)/pspb.FieldOption_Index)))
		case pspb.FieldType_VECTOR:
			fieldMapping := value.Field.FieldMappingI.(*mapping.VectortFieldMapping)
			vf := C.MakeVectorInfo(byteArrayStr(key), VECTOR, C.int(fieldMapping.Dimension), byteArrayStr(fieldMapping.ModelId), byteArrayStr(fieldMapping.RetrievalType), byteArrayStr(fieldMapping.StoreType), byteArray(fieldMapping.StoreParam))
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

	engine := cfg.Space.Engine

	table.ivfpq_param = C.MakeIVFPQParameters(C.int(*engine.MetricType), C.int(*engine.Nprobe), C.int(*engine.Ncentroids), C.int(*engine.Nsubvector), C.int(*engine.NbitsPerIdx))

	return table, nil
}

//create doc
func DocCmd2Document(docCmd *pspb.DocCmd) (*C.struct_Doc, error) {

	if docCmd.Version <= 0 {
		docCmd.Version = 1
	}

	fields := make([]*C.struct_Field, 0, len(docCmd.Fields)+1)

	fields = append(fields, newField(mapping.IdField, []byte(docCmd.DocId), STRING))

	//version
	if toByte, e := bytes.ValueToByte(docCmd.Version); e != nil {
		return nil, e
	} else {
		fields = append(fields, newField(mapping.VersionField, toByte, INT))
	}

	for _, f := range docCmd.Fields {

		if f.Value == nil {
			return nil, fmt.Errorf("miss field value by name:%s", f.Name)
		}

		if mapping.FieldsIndex[f.Name] > 0 {
			continue
		}

		switch f.Type {
		case pspb.FieldType_TEXT:
			log.Error("gamma engine not support text field:[%s]", f.Name)
		case pspb.FieldType_KEYWORD:
			fields = append(fields, newField(f.Name, []byte(f.Value.Text), STRING))
		case pspb.FieldType_FLOAT:
			if toByte, err := bytes.ValueToByte(f.Value.Float); err != nil {
				return nil, err
			} else {
				fields = append(fields, newField(f.Name, toByte, DOUBLE))
			}
		case pspb.FieldType_INT:
			if toByte, err := bytes.ValueToByte(f.Value.Int); err != nil {
				return nil, err
			} else {
				fields = append(fields, newField(f.Name, toByte, LONG))
			}
		case pspb.FieldType_DATE:
			if f.Value.Time == nil {
				return nil, errors.New("miss date field value")
			}
			date := time.Unix(f.Value.Time.Sec, f.Value.Time.Usec)
			if toByte, err := bytes.ValueToByte(date.Nanosecond()); err != nil {
				return nil, err
			} else {
				fields = append(fields, newField(f.Name, toByte, LONG))
			}
		case pspb.FieldType_BOOL:
			var v int
			if !f.Value.Bool {
				v = 1
			}
			if toByte, err := bytes.ValueToByte(v); err != nil {
				return nil, err
			} else {
				fields = append(fields, newField(f.Name, toByte, INT))
			}
		case pspb.FieldType_VECTOR:
			code := bytes.UnsafeFloat32SliceAsByteSlice(f.Value.Vector.Feature)
			fields = append(fields, newFieldBySource(f.Name, code, f.Value.Vector.Source, VECTOR))
		default:
			log.Debug("gamma invalid field type:[%v]", f.Type)
		}
	}

	arr := C.MakeFields(C.int(len(fields)))
	for i, f := range fields {
		C.SetField(arr, C.int(i), f)
	}

	return &C.struct_Doc{fields: arr, fields_num: C.int(len(fields))}, nil
}

func (ge *gammaEngine) ResultItem2DocResult(item *C.struct_ResultItem) *response.DocResult {
	result := ge.Doc2DocResult(item.doc)
	result.Score = float64(item.score)
	result.Extra = CbArr2ByteArray(item.extra)
	result.SortValues = []sortorder.SortValue{
		&sortorder.FloatSortValue{
			Val: result.Score,
		},
	}
	return result
}

func (ge *gammaEngine) Doc2DocResult(doc *C.struct_Doc) *response.DocResult {

	result := response.DocResult{
		Found:     true,
		DB:        ge.GetSpace().DBId,
		Space:     ge.GetSpace().Id,
		Partition: ge.GetPartitionID(),
	}

	fieldNum := int(doc.fields_num)

	source := make(map[string]interface{})

	var err error

	for i := 0; i < fieldNum; i++ {
		fv := C.GetField(doc, C.int(i))
		name := string(CbArr2ByteArray(fv.name))

		switch name {
		case mapping.VersionField:
			result.Version = int64(bytes.ByteArray2UInt64(CbArr2ByteArray(fv.value)))
		case mapping.SlotField:
			result.SlotID = uint32(bytes.ByteArray2UInt64(CbArr2ByteArray(fv.value)))
		case mapping.IdField:
			result.Id = string(CbArr2ByteArray(fv.value))
		case mapping.SourceField:
			result.Source = CbArr2ByteArray(fv.value)
		default:
			field := ge.GetMapping().GetField(name)
			if field == nil {
				log.Error("can not found mappping by field:[%s]", name)
				continue
			}
			switch field.FieldType() {
			case pspb.FieldType_TEXT:
				source[name] = string(CbArr2ByteArray(fv.value))
			case pspb.FieldType_KEYWORD:
				tempValue := string(CbArr2ByteArray(fv.value))
				if field.FieldMappingI.(*mapping.KeywordFieldMapping).Array {
					source[name] = strings.Split(tempValue, string([]byte{'\001'}))
				} else {
					source[name] = tempValue
				}
			case pspb.FieldType_INT:
				source[name] = bytes.Bytes2Int(CbArr2ByteArray(fv.value))
			case pspb.FieldType_FLOAT:
				source[name] = bytes.ByteToFloat64(CbArr2ByteArray(fv.value))
			case pspb.FieldType_VECTOR:
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

func (ge *gammaEngine) DocCmd2WriteResult(docCmd *pspb.DocCmd) *response.DocResult {
	return &response.DocResult{
		Id:        docCmd.DocId,
		DB:        ge.GetSpace().DBId,
		Space:     ge.GetSpace().Id,
		Found:     true,
		Partition: ge.GetPartitionID(),
		Version:   docCmd.Version,
		SlotID:    docCmd.Slot,
		Type:      docCmd.Type,
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
	return bytes.CloneBytes(oids)
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
