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

package client

/*
#cgo CFLAGS : -Ilib/include
#cgo LDFLAGS: -Llib/lib -lgamma
*/

import (
	"encoding/binary"
	"encoding/json"
	"github.com/vearch/vearch/ps/engine/sortorder"
	"strconv"
	"strings"
	"time"

	"github.com/vearch/vearch/engine/idl/fbs-gen/go/gamma_api"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/log"
)

var empty = []byte{0}

func ResultItem2DocResult(item *gamma_api.ResultItem, partitionID uint32,
	arrayBool bool, spaceId int64, dbId int64, fieldType map[string]pspb.FieldType) *response.DocResult {
	result := Doc2DocResult(item, partitionID, arrayBool, spaceId, dbId, fieldType)
	result.Score = float64(item.Score())
	result.Extra = item.Extra()
	result.SortValues = []sortorder.SortValue{
		&sortorder.FloatSortValue{
			Val: result.Score,
		},
	}
	return result
}

func ResultItem2DocIDResult(item *gamma_api.ResultItem, partitionID uint32,
	spaceId int64, dbId int64, idType string) *response.DocResult {
	result := response.DocResult{
		Found:     true,
		DB:        dbId,
		Space:     spaceId,
		Partition: partitionID,
	}
	if idType != "" && ("long" == idType || "Long" == idType) {
		id := int64(cbbytes.ByteArray2UInt64(item.Value(0)))
		result.Id = strconv.FormatInt(id, 10)
	} else {
		result.Id = string(item.Value(0))
	}
	result.Score = float64(item.Score())
	result.SortValues = []sortorder.SortValue{
		&sortorder.FloatSortValue{
			Val: result.Score,
		},
	}
	return &result
}

func Doc2DocResult(item *gamma_api.ResultItem, partitionID uint32,
	arrayBool bool, spaceId int64, dbId int64, fieldType map[string]pspb.FieldType) *response.DocResult {

	result := response.DocResult{
		Found:     true,
		DB:        dbId,
		Space:     spaceId,
		Partition: partitionID,
	}

	fieldNum := item.NameLength()

	source := make(map[string]interface{})

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
			id := int64(cbbytes.ByteArray2UInt64(value))
			result.Id = strconv.FormatInt(id, 10)
		case mapping.SourceField:
			result.Source = value
		default:
			field := fieldType[name]
			if field == 0 {
				log.Error("can not found mappping by field:[%s]", name)
				continue
			}
			switch field {
			case pspb.FieldType_STRING:
				tempValue := string(value)
				if arrayBool {
					source[name] = strings.Split(tempValue, string([]byte{'\001'}))
				} else {
					source[name] = tempValue
				}
			case pspb.FieldType_INT:
				source[name] = cbbytes.Bytes2Int32(value)
			case pspb.FieldType_LONG:
				source[name] = cbbytes.Bytes2Int(value)
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

				float32s, uri, err := cbbytes.ByteToVector(value)
				if err != nil {
					return response.NewErrDocResult(result.Id, err)
				}
				source[name] = map[string]interface{}{
					"source":  uri,
					"feature": float32s,
				}

			default:
				log.Warn("can not set value by type:[%v] ", field)
			}
		}
	}
	marshal, err := json.Marshal(source)
	if err != nil {
		log.Warn("can not marshl source :[%v] ", err.Error())
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

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}
