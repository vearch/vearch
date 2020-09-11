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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
)

func docGetResponse(client *client.Client, args *vearchpb.GetRequest, reply *vearchpb.GetResponse, returnFieldsMap map[string]string) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	isArray := false
	if reply != nil && reply.Items != nil && len(reply.Items) > 1 {
		builder.BeginArray()
		isArray = true
	}
	for _, item := range reply.Items {
		doc := item.Doc
		builder.BeginObject()
		if args == nil || reply == nil || reply.Items == nil || len(reply.Items) < 1 {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
		}
		space, err := client.Space(context.Background(), args.Head.DbName, args.Head.SpaceName)
		if err != nil {
			return nil, err
		}
		builder.Field("_index")
		builder.ValueString(args.Head.DbName)

		builder.More()
		builder.Field("_type")
		builder.ValueString(args.Head.SpaceName)

		//doc := reply.Items[0].Doc
		builder.More()
		builder.Field("_id")
		if idIsLong(space) {
			idInt64, err := strconv.ParseInt(doc.PKey, 10, 64)
			if err == nil {
				builder.ValueNumeric(idInt64)
			}
		} else {
			builder.ValueString(doc.PKey)
		}
		builder.More()
		builder.Field("found")
		if doc.Fields != nil {
			builder.ValueBool(true)
			source, _ := docFieldSerialize(doc, space, returnFieldsMap)
			builder.More()
			builder.Field("_source")
			builder.ValueInterface(source)
		} else {
			builder.ValueBool(false)
		}
		builder.EndObject()
	}

	if isArray {
		builder.EndArray()
	}
	return builder.Output()
}

func docDeleteResponses(client *client.Client, args *vearchpb.DeleteRequest, reply *vearchpb.DeleteResponse) ([]byte, error) {
	if args == nil || reply == nil || reply.Items == nil || len(reply.Items) < 1 {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}
	return docResponse(client, args.Head, reply.Items)
}

func docUpdateResponses(client *client.Client, args *vearchpb.UpdateRequest, reply *vearchpb.UpdateResponse) ([]byte, error) {
	if args == nil || reply == nil || reply.Head == nil || reply.Head.Err == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}
	headErr := reply.Head.Err
	if headErr.Code != vearchpb.ErrorEnum_SUCCESS {
		return nil, vearchpb.NewError(headErr.Code, errors.New(headErr.Msg))
	}
	space, err := client.Space(context.Background(), args.Head.DbName, args.Head.SpaceName)
	if err != nil {
		return nil, err
	}
	return docResultSerialize(space, args.Head, &vearchpb.Item{Doc: &vearchpb.Document{PKey: args.Doc.PKey}})
}

func docBulkResponses(client *client.Client, args *vearchpb.BulkRequest, reply *vearchpb.BulkResponse) ([]byte, error) {
	if args == nil || reply == nil || reply.Items == nil || len(reply.Items) < 1 {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}
	return docResponse(client, args.Head, reply.Items)
}

func docResponse(client *client.Client, head *vearchpb.RequestHead, items []*vearchpb.Item) ([]byte, error) {
	space, err := client.Space(context.Background(), head.DbName, head.SpaceName)
	if err != nil {
		return nil, err
	}
	var builder = cbjson.ContentBuilderFactory()
	if len(items) > 1 {
		builder.BeginArray()
	}
	for idx, item := range items {
		if idx != 0 {
			builder.More()
		}
		if result, err := docResultSerialize(space, head, item); err != nil {
			return nil, err
		} else {
			builder.ValueRaw(string(result))
		}
	}
	if len(items) > 1 {
		builder.EndArray()
	}
	return builder.Output()
}

func docResultSerialize(space *entity.Space, head *vearchpb.RequestHead, item *vearchpb.Item) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	builder.BeginObject()
	builder.Field("_index")
	builder.ValueString(head.DbName)

	builder.More()
	builder.Field("_type")
	builder.ValueString(head.SpaceName)

	doc := item.Doc
	builder.More()
	builder.Field("_id")
	if idIsLong(space) {
		idInt64, err := strconv.ParseInt(doc.PKey, 10, 64)
		if err == nil {
			builder.ValueNumeric(idInt64)
		}
	} else {
		builder.ValueString(doc.PKey)
	}
	if item.Err != nil {
		builder.More()
		builder.Field("status")
		builder.ValueNumeric(cast.ToInt64(vearchpb.ErrCode(item.Err.Code)))

		builder.More()
		builder.Field("error")
		builder.ValueString(item.Err.Msg)
	} else {
		builder.More()
		builder.Field("status")
		builder.ValueNumeric(cast.ToInt64(vearchpb.ErrCode(vearchpb.ErrorEnum_SUCCESS)))
	}
	builder.EndObject()
	return builder.Output()
}

func idIsLong(space *entity.Space) bool {
	idIsLong := false
	idType := space.Engine.IdType
	if strings.EqualFold("long", idType) {
		idIsLong = true
	}
	return idIsLong
}
func docFieldSerialize(doc *vearchpb.Document, space *entity.Space, returnFieldsMap map[string]string) (json.RawMessage, error) {
	source := make(map[string]interface{})
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		spaceProperties = spacePro
	}
	for _, fv := range doc.Fields {
		name := fv.Name
		if name == mapping.IdField {
			continue
		}
		if (returnFieldsMap != nil && returnFieldsMap[name] != "") || returnFieldsMap == nil {
			field := spaceProperties[name]
			if field == nil {
				log.Error("can not found mappping by field:[%s]", name)
				continue

			}
			switch field.FieldType {
			case entity.FieldType_STRING:
				tempValue := string(fv.Value)
				if field.Array {
					source[name] = strings.Split(tempValue, string([]byte{'\001'}))
				} else {
					source[name] = tempValue
				}
			case entity.FieldType_INT:
				source[name] = cbbytes.Bytes2Int32(fv.Value)
			case entity.FieldType_LONG:
				source[name] = cbbytes.Bytes2Int(fv.Value)
			case entity.FieldType_BOOL:
				if cbbytes.Bytes2Int(fv.Value) == 0 {
					source[name] = false
				} else {
					source[name] = true
				}
			case entity.FieldType_DATE:
				u := cbbytes.Bytes2Int(fv.Value)
				source[name] = time.Unix(u/1e6, u%1e6)
			case entity.FieldType_FLOAT:
				source[name] = cbbytes.ByteToFloat32(fv.Value)
			case entity.FieldType_VECTOR:
				if strings.Compare(space.Engine.RetrievalType, "BINARYIVF") == 0 {
					featureByteC := fv.Value
					dimension := field.Dimension
					if dimension != 0 {
						unit8s, _, err := cbbytes.ByteToVectorBinary(featureByteC, dimension)
						if err != nil {
							return nil, err
						}
						source[name] = map[string]interface{}{
							"source":  fv.Source,
							"feature": unit8s,
						}
					} else {
						log.Error("GetSource can not found dimension by field:[%s]", name)
					}

				} else {
					float32s, s, err := cbbytes.ByteToVector(fv.Value)
					if err != nil {
						return nil, err
					}
					source[name] = map[string]interface{}{
						"source":  s,
						"feature": float32s,
					}
				}

			default:
				log.Warn("can not set value by type:[%v] ", field.FieldType)

			}
		}
	}
	marshal, err := json.Marshal(source)
	if err != nil {
		return nil, err

	}
	return marshal, nil

}

func ToContent(sr *vearchpb.SearchResult, head *vearchpb.RequestHead, took time.Duration, space *entity.Space) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field("took")
	builder.ValueNumeric(int64(took) / 1e6)

	builder.More()
	builder.Field("timed_out")
	builder.ValueBool(sr.Timeout)

	builder.More()
	builder.Field("_shards")
	builder.ValueInterface(sr.Status)

	builder.More()
	builder.BeginObjectWithField("hits")

	builder.Field("total")
	total := len(sr.ResultItems)
	builder.ValueNumeric(int64(total))

	builder.More()
	builder.Field("max_score")
	builder.ValueFloat(sr.MaxScore)

	if sr.ResultItems != nil {
		builder.More()
		builder.BeginArrayWithField("hits")
		content, err := DocToContent(sr.ResultItems, head, space)
		if err != nil {
			return nil, err
		}
		builder.ValueRaw(string(content))

		builder.EndArray()
	}

	builder.EndObject()

	/*if sr.Explain != nil && len(sr.Explain) > 0 {
		builder.More()
		builder.Field("_explain")
		builder.ValueInterface(sr.Explain)
	}*/

	builder.EndObject()

	return builder.Output()

}

func DocToContent(dh []*vearchpb.ResultItem, head *vearchpb.RequestHead, space *entity.Space) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	idIsLong := idIsLong(space)
	for i, u := range dh {

		if i != 0 {
			builder.More()
		}
		builder.BeginObject()

		builder.Field("_index")
		builder.ValueString(head.DbName)

		builder.More()
		builder.Field("_type")
		builder.ValueString(head.SpaceName)

		builder.More()
		builder.Field("_id")
		if idIsLong {
			idInt64, err := strconv.ParseInt(u.PKey, 10, 64)
			if err == nil {
				builder.ValueNumeric(idInt64)
			}
		} else {
			builder.ValueString(u.PKey)
		}

		if u.Fields != nil {
			builder.More()
			builder.Field("_score")
			builder.ValueFloat(float64(u.Score))

			if u.Extra != "" && len(u.Extra) > 0 {
				builder.More()
				var extra map[string]interface{}
				if err := json.Unmarshal([]byte(u.Extra), &extra); err == nil {
					builder.Field("_extra")
					builder.ValueInterface(extra)
				}
			}
			if u.Source != nil {
				var sourceJson json.RawMessage
				if err := json.Unmarshal(u.Source, &sourceJson); err != nil {
					log.Error("DocToContent Source Unmarshal error:%v", err)
				} else {
					builder.More()
					builder.Field("_source")
					builder.ValueInterface(sourceJson)
				}
			}
		}

		builder.EndObject()
	}

	return builder.Output()
}

func ToContents(srs []*vearchpb.SearchResult, head *vearchpb.RequestHead, took time.Duration, space *entity.Space) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("took")
	builder.ValueNumeric(int64(took) / 1e6)

	builder.More()

	builder.Field("results")

	builder.BeginArray()
	for i, sr := range srs {
		if bytes, err := ToContent(sr, head, took, space); err != nil {
			return nil, err
		} else {
			builder.ValueRaw(string(bytes))
		}

		if i+1 < len(srs) {
			builder.More()
		}
	}
	builder.EndArray()

	builder.EndObject()

	return builder.Output()
}

func ToContentIds(srs []*vearchpb.SearchResult, space *entity.Space) ([]byte, error) {
	idIsLong := idIsLong(space)
	bs := bytes.Buffer{}
	bs.WriteString("[")
	for _, sr := range srs {
		if sr.ResultItems != nil {
			for j, u := range sr.ResultItems {
				if j != 0 {
					bs.WriteString(",")
				}
				for _, fv := range u.Fields {
					name := fv.Name
					switch name {
					case mapping.IdField:
						if idIsLong {
							id := int64(cbbytes.ByteArray2UInt64(fv.Value))
							bs.WriteString(strconv.FormatInt(id, 10))
						} else {
							bs.WriteString("\"")
							bs.WriteString(string(fv.Value))
							bs.WriteString("\"")
						}
					}
				}
			}
		}
	}
	bs.WriteString("]")
	return bs.Bytes(), nil
}

func queryDocByIdsNoResult(took time.Duration) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("took")
	builder.ValueNumeric(int64(took) / 1e6)

	builder.More()

	builder.Field("results")

	builder.BeginArray()

	builder.EndArray()

	builder.EndObject()

	return builder.Output()
}

func GetVectorFieldValue(doc *vearchpb.Document, space *entity.Space) (floatFeatureMap map[string][]float32, binaryFeatureMap map[string][]int32, err error) {
	source := make(map[string]interface{})
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		spaceProperties = spacePro
	}
	for _, fv := range doc.Fields {
		name := fv.Name
		if name == mapping.IdField {
			continue
		}
		field := spaceProperties[name]
		if field == nil {
			log.Error("can not found mappping by field:[%s]", name)
			continue

		}
		switch field.FieldType {
		case entity.FieldType_STRING:
			tempValue := string(fv.Value)
			if field.Array {
				source[name] = strings.Split(tempValue, string([]byte{'\001'}))
			} else {
				source[name] = tempValue
			}
		case entity.FieldType_INT:
			source[name] = cbbytes.Bytes2Int32(fv.Value)
		case entity.FieldType_LONG:
			source[name] = cbbytes.Bytes2Int(fv.Value)
		case entity.FieldType_BOOL:
			if cbbytes.Bytes2Int(fv.Value) == 0 {
				source[name] = false
			} else {
				source[name] = true
			}
		case entity.FieldType_DATE:
			u := cbbytes.Bytes2Int(fv.Value)
			source[name] = time.Unix(u/1e6, u%1e6)
		case entity.FieldType_FLOAT:
			source[name] = cbbytes.ByteToFloat32(fv.Value)
		case entity.FieldType_VECTOR:
			if strings.Compare(space.Engine.RetrievalType, "BINARYIVF") == 0 {
				featureByteC := fv.Value
				dimension := field.Dimension
				if dimension != 0 {
					unit8s, _, err := cbbytes.ByteToVectorBinary(featureByteC, dimension)
					if err != nil {
						return nil, nil, err
					}
					source[name] = map[string]interface{}{
						"source":  fv.Source,
						"feature": unit8s,
					}
					if binaryFeatureMap != nil {
						binaryFeatureMap[name] = unit8s
					} else {
						binaryFeatureMap = make(map[string][]int32)
						binaryFeatureMap[name] = unit8s
					}
				} else {
					log.Error("GetSource can not found dimension by field:[%s]", name)
				}

			} else {
				float32s, s, err := cbbytes.ByteToVector(fv.Value)
				log.Error("vector.Field.value len %d, source is [%s]", len(fv.Value), s)
				if err != nil {
					return nil, nil, err
				}
				source[name] = map[string]interface{}{
					"source":  s,
					"feature": float32s,
				}
				if floatFeatureMap != nil {
					floatFeatureMap[name] = float32s
				} else {
					floatFeatureMap = make(map[string][]float32)
					floatFeatureMap[name] = float32s
				}
			}

		default:
			log.Warn("can not set value by type:[%v] ", field.FieldType)
		}
	}
	return floatFeatureMap, binaryFeatureMap, nil
}

func MakeQueryFeature(floatFeatureMap map[string][]float32, binaryFeatureMap map[string][]int32) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("sum")

	builder.BeginArray()
	i := 0
	if floatFeatureMap != nil {
		for key, value := range floatFeatureMap {
			if i != 0 {
				builder.More()
			}
			builder.BeginObject()
			builder.Field("field")
			builder.ValueString(key)
			builder.More()
			builder.Field("feature")
			builder.ValueInterface(value)
			builder.EndObject()
			i++
		}
	} else {
		for key, value := range binaryFeatureMap {
			if i != 0 {
				builder.More()
			}
			builder.BeginObject()
			builder.Field("field")
			builder.ValueString(key)
			builder.More()
			builder.Field("feature")
			builder.ValueInterface(value)
			builder.EndObject()
			i++
		}
	}

	builder.EndArray()

	builder.EndObject()

	return builder.Output()
}

func ForceMergeToContent(shards *vearchpb.SearchStatus) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field("_shards")
	builder.ValueInterface(shards)
	builder.EndObject()

	return builder.Output()
}

func FlushToContent(shards *vearchpb.SearchStatus) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field("_shards")
	builder.ValueInterface(shards)
	builder.EndObject()

	return builder.Output()
}

func SearchNullToContent(searchStatus vearchpb.SearchStatus, took time.Duration) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field("took")
	builder.ValueNumeric(int64(took) / 1e6)

	builder.More()
	builder.Field("timed_out")
	builder.ValueBool(false)

	builder.More()
	builder.Field("_shards")
	builder.ValueInterface(searchStatus)

	builder.More()
	builder.BeginObjectWithField("hits")

	builder.Field("total")
	builder.ValueNumeric(0)

	builder.More()
	builder.Field("max_score")
	builder.ValueFloat(-1)

	builder.EndObject()

	return builder.Output()

}
