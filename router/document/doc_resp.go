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
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
)

func docGetResponse(client *client.Client, args *vearchpb.GetRequest, reply *vearchpb.GetResponse, returnFieldsMap map[string]string, isBatch bool) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	isArray := false
	if isBatch || (reply != nil && reply.Items != nil && len(reply.Items) > 1) {
		builder.BeginArray()
		isArray = true
	}
	if args == nil || reply == nil || reply.Items == nil || len(reply.Items) < 1 {
		if reply.GetHead() != nil && reply.GetHead().Err != nil && reply.GetHead().Err.Code != vearchpb.ErrorEnum_SUCCESS {
			err := reply.GetHead().Err
			return nil, vearchpb.NewError(err.Code, errors.New(err.Msg))
		}
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}
	for i, item := range reply.Items {
		if i != 0 {
			builder.More()
		}
		doc := item.Doc
		builder.BeginObject()
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
			} else {
				builder.ValueString(doc.PKey)
			}
		} else {
			builder.ValueString(doc.PKey)
		}
		builder.More()
		builder.Field("found")
		if doc.Fields != nil {
			builder.ValueBool(true)
			source, _ := docFieldSerialize(doc, space, returnFieldsMap, true)
			builder.More()
			builder.Field("_source")
			builder.ValueInterface(source)
		} else {
			builder.ValueBool(false)
		}
		if item.Err != nil {
			builder.More()
			builder.Field("msg")
			builder.ValueString(item.Err.Msg)
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
		if reply.GetHead() != nil && reply.GetHead().Err != nil && reply.GetHead().Err.Code != vearchpb.ErrorEnum_SUCCESS {
			err := reply.GetHead().Err
			return nil, vearchpb.NewError(err.Code, errors.New(err.Msg))
		}
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}
	return docResponse(client, args.Head, reply.Items)
}

func docUpdateResponses(client *client.Client, args *vearchpb.UpdateRequest, reply *vearchpb.UpdateResponse) ([]byte, error) {
	if args == nil || reply == nil || reply.Head == nil || reply.Head.Err == nil {
		if reply.GetHead() != nil && reply.GetHead().Err != nil && reply.GetHead().Err.Code != vearchpb.ErrorEnum_SUCCESS {
			err := reply.GetHead().Err
			return nil, vearchpb.NewError(err.Code, errors.New(err.Msg))
		}
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
		if reply.GetHead() != nil && reply.GetHead().Err != nil && reply.GetHead().Err.Code != vearchpb.ErrorEnum_SUCCESS {
			err := reply.GetHead().Err
			return nil, vearchpb.NewError(err.Code, errors.New(err.Msg))
		}
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
	builder.BeginArray()
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
	builder.EndArray()
	return builder.Output()
}

func documentUpsertResponse(client *client.Client, args *vearchpb.BulkRequest, reply *vearchpb.BulkResponse) ([]byte, error) {
	if args == nil || reply == nil || reply.Items == nil || len(reply.Items) < 1 {
		if reply.GetHead() != nil && reply.GetHead().Err != nil && reply.GetHead().Err.Code != vearchpb.ErrorEnum_SUCCESS {
			err := reply.GetHead().Err
			return nil, vearchpb.NewError(err.Code, errors.New(err.Msg))
		}
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}

	space, err := client.Space(context.Background(), args.Head.DbName, args.Head.SpaceName)
	if err != nil {
		return nil, err
	}

	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field("code")
	if reply.Head == nil || reply.Head.Err == nil {
		builder.ValueNumeric(0)
	} else {
		if reply.Head != nil && reply.Head.Err != nil {
			builder.ValueNumeric(int64(reply.Head.Err.Code))
			builder.More()
			builder.Field("msg")
			builder.ValueString(reply.Head.Err.Msg)
		} else {
			builder.ValueNumeric(1)
		}
	}

	var total int64
	for _, item := range reply.Items {
		if item.Err == nil {
			total += 1
		} else {
			if item.Err.Msg == "success" && item.Err.Code == vearchpb.ErrorEnum_SUCCESS {
				total += 1
			}
		}
	}
	builder.More()
	builder.Field("total")
	builder.ValueNumeric(total)

	builder.More()
	builder.BeginArrayWithField("document_ids")
	for idx, item := range reply.Items {
		if idx != 0 {
			builder.More()
		}
		if result, err := documentResultSerialize(space, item); err != nil {
			return nil, err
		} else {
			builder.ValueRaw(string(result))
		}
	}
	builder.EndArray()

	builder.EndObject()

	return builder.Output()
}

func documentResultSerialize(space *entity.Space, item *vearchpb.Item) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	builder.BeginObject()
	if item == nil {
		builder.Field("error")
		builder.ValueString("duplicate id")
		builder.EndObject()
		return builder.Output()
	}
	doc := item.Doc
	builder.Field("_id")
	if idIsLong(space) {
		idInt64, err := strconv.ParseInt(doc.PKey, 10, 64)
		if err == nil {
			builder.ValueNumeric(idInt64)
		} else {
			builder.ValueString(doc.PKey)
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

func documentGetResponse(client *client.Client, args *vearchpb.GetRequest, reply *vearchpb.GetResponse, returnFieldsMap map[string]string, vectorValue bool) ([]byte, error) {
	if args == nil || reply == nil || reply.Items == nil || len(reply.Items) < 1 {
		if reply.GetHead() != nil && reply.GetHead().Err != nil && reply.GetHead().Err.Code != vearchpb.ErrorEnum_SUCCESS {
			err := reply.GetHead().Err
			return nil, vearchpb.NewError(err.Code, errors.New(err.Msg))
		}
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}

	space, err := client.Space(context.Background(), args.Head.DbName, args.Head.SpaceName)
	if err != nil {
		return nil, err
	}
	var builder = cbjson.ContentBuilderFactory()
	builder.BeginObject()

	builder.Field("code")
	if reply.Head == nil || reply.Head.Err == nil {
		builder.ValueNumeric(int64(vearchpb.ErrorEnum_SUCCESS))
		builder.More()
		builder.Field("msg")
		builder.ValueString("success")
	} else {
		if reply.Head != nil && reply.Head.Err != nil {
			builder.ValueNumeric(int64(reply.Head.Err.Code))
			builder.More()
			builder.Field("msg")
			builder.ValueString(reply.Head.Err.Msg)
		} else {
			builder.ValueNumeric(int64(vearchpb.ErrorEnum_INTERNAL_ERROR))
		}
	}

	var total int64
	for _, item := range reply.Items {
		if item.Doc.Fields != nil {
			total += 1
		} else {
			if item.Err.Msg == "success" && item.Err.Code == vearchpb.ErrorEnum_SUCCESS {
				total += 1
			}
		}
	}
	builder.More()
	builder.Field("total")
	builder.ValueNumeric(total)

	builder.More()
	builder.BeginArrayWithField("documents")
	for i, item := range reply.Items {
		if i != 0 {
			builder.More()
		}
		builder.BeginObject()

		doc := item.Doc
		builder.Field("_id")
		if idIsLong(space) {
			idInt64, err := strconv.ParseInt(doc.PKey, 10, 64)
			if err == nil {
				builder.ValueNumeric(idInt64)
			} else {
				builder.ValueString(doc.PKey)
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
		}

		if doc.Fields != nil {
			source, _ := docFieldSerialize(doc, space, returnFieldsMap, vectorValue)
			builder.More()
			builder.Field("_source")
			builder.ValueInterface(source)
		}
		builder.EndObject()
	}
	builder.EndArray()
	builder.EndObject()

	return builder.Output()
}

func documentSearchResponse(srs []*vearchpb.SearchResult, head *vearchpb.ResponseHead, took time.Duration, space *entity.Space, response_type string) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field("code")
	if head == nil || head.Err == nil {
		builder.ValueNumeric(int64(vearchpb.ErrorEnum_SUCCESS))
		builder.More()
		builder.Field("msg")
		builder.ValueString("success")
	} else {
		if head != nil && head.Err != nil {
			builder.ValueNumeric(int64(head.Err.Code))
			builder.More()
			builder.Field("msg")
			builder.ValueString(head.Err.Msg)
		} else {
			builder.ValueNumeric(int64(vearchpb.ErrorEnum_INTERNAL_ERROR))
		}
	}

	//	builder.More()
	//	builder.Field("took")
	//	builder.ValueNumeric(int64(took) / 1e6)

	if response_type == request.QueryResponse {
		if srs == nil {
			//	searchStatus := &vearchpb.SearchStatus{Failed: 0, Successful: 0, Total: 0}
			//	builder.More()
			//	builder.Field("timed_out")
			//	builder.ValueBool(false)

			//	builder.More()
			//	builder.Field("_shards")
			//	builder.ValueInterface(searchStatus)

			builder.More()
			builder.Field("total")
			builder.ValueNumeric(0)
		} else {
			//	builder.More()
			//	builder.Field("timed_out")
			//	builder.ValueBool(sr.Timeout)

			//	builder.More()
			//	builder.Field("_shards")
			//	builder.ValueInterface(sr.Status)
			builder.More()
			builder.Field("total")
			total := len(srs[0].ResultItems)
			builder.ValueNumeric(int64(total))
		}
	}

	builder.More()
	builder.BeginArrayWithField("documents")

	if len(srs) > 1 {
		var wg sync.WaitGroup
		respChain := make(chan map[int][]byte, len(srs))
		for i, sr := range srs {
			wg.Add(1)
			go func(sr *vearchpb.SearchResult, space *entity.Space, index int) {
				bytes, err := documentToContent(sr.ResultItems, space, response_type)
				if err == nil {
					respMap := make(map[int][]byte)
					respMap[index] = bytes
					respChain <- respMap
				}
				wg.Done()
			}(sr, space, i)
		}
		wg.Wait()
		close(respChain)

		byteArr := make([][]byte, len(srs))
		for resp := range respChain {
			for index, value := range resp {
				byteArr[index] = value
			}
		}

		for i := 0; i < len(srs); i++ {
			builder.ValueRaw(string(byteArr[i]))
			if i+1 < len(srs) {
				builder.More()
			}
		}
	} else {
		for i, sr := range srs {
			if bytes, err := documentToContent(sr.ResultItems, space, response_type); err != nil {
				return nil, err
			} else {
				builder.ValueRaw(string(bytes))
			}

			if i+1 < len(srs) {
				builder.More()
			}
		}
	}

	builder.EndArray()

	/*if sr.Explain != nil && len(sr.Explain) > 0 {
		builder.More()
		builder.Field("_explain")
		builder.ValueInterface(sr.Explain)
	}*/

	builder.EndObject()

	return builder.Output()
}

func documentToContent(dh []*vearchpb.ResultItem, space *entity.Space, response_type string) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	idIsLong := idIsLong(space)
	if response_type == request.SearchResponse {
		builder.BeginArray()
	}
	for i, u := range dh {

		if i != 0 {
			builder.More()
		}
		builder.BeginObject()

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
			if response_type == request.SearchResponse {
				builder.More()
				builder.Field("_score")
				builder.ValueFloat(float64(u.Score))
			}

			/*if u.Extra != "" && len(u.Extra) > 0 {
				builder.More()
				var extra map[string]interface{}
				if err := json.Unmarshal([]byte(u.Extra), &extra); err == nil {
					builder.Field("_extra")
					builder.ValueInterface(extra)
				}
			}*/
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
	if response_type == request.SearchResponse {
		builder.EndArray()
	}

	return builder.Output()
}

func documentDeleteResponse(items []*vearchpb.Item, head *vearchpb.ResponseHead, resultIds []string) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	for _, item := range items {
		if item.Err == nil {
			resultIds = append(resultIds, item.Doc.PKey)
		}
	}

	builder.BeginObject()
	builder.Field("code")
	if head == nil || head.Err == nil {
		builder.ValueNumeric(int64(vearchpb.ErrorEnum_SUCCESS))
		builder.More()
		builder.Field("msg")
		builder.ValueString("success")
	} else {
		if head != nil && head.Err != nil {
			builder.ValueNumeric(int64(head.Err.Code))
			builder.More()
			builder.Field("msg")
			builder.ValueString(head.Err.Msg)
		} else {
			builder.ValueNumeric(int64(vearchpb.ErrorEnum_INTERNAL_ERROR))
		}
	}

	builder.More()
	builder.Field("total")
	builder.ValueNumeric(int64(len(resultIds)))

	builder.More()
	builder.Field("document_ids")
	if len(resultIds) != 0 {
		builder.ValueInterface(resultIds)
	} else {
		builder.ValueString("[]")
	}

	builder.EndObject()

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

	if item == nil {
		builder.Field("error")
		builder.ValueString("duplicate id")
		builder.EndObject()
		return builder.Output()
	}
	doc := item.Doc
	builder.More()
	builder.Field("_id")
	if idIsLong(space) {
		idInt64, err := strconv.ParseInt(doc.PKey, 10, 64)
		if err == nil {
			builder.ValueNumeric(idInt64)
		} else {
			builder.ValueString(doc.PKey)
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

func docFieldSerialize(doc *vearchpb.Document, space *entity.Space, returnFieldsMap map[string]string, vectorValue bool) (json.RawMessage, error) {
	source := make(map[string]interface{})
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		spaceProperties = spacePro
	}
	for _, fv := range doc.Fields {
		name := fv.Name
		if name == mapping.IdField && returnFieldsMap == nil {
			if strings.EqualFold("long", space.Engine.IdType) {
				source[name] = cbbytes.Bytes2Int(fv.Value)
			} else {
				source[name] = string(fv.Value)
			}
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
			case entity.FieldType_DOUBLE:
				source[name] = cbbytes.ByteToFloat64New(fv.Value)
			case entity.FieldType_VECTOR:
				if vectorValue {
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
				}

			default:
				log.Warn("can not set value by type:[%v] ", field.FieldType)

			}
		}
	}
	var marshal []byte
	var err error
	if len(source) > 0 {
		marshal, err = json.Marshal(source)
	}
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

			/*if u.Extra != "" && len(u.Extra) > 0 {
				builder.More()
				var extra map[string]interface{}
				if err := json.Unmarshal([]byte(u.Extra), &extra); err == nil {
					builder.Field("_extra")
					builder.ValueInterface(extra)
				}
			}*/
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

	if srs != nil && len(srs) > 1 {
		var wg sync.WaitGroup
		respChain := make(chan map[int][]byte, len(srs))
		for i, sr := range srs {
			wg.Add(1)
			go func(sr *vearchpb.SearchResult, head *vearchpb.RequestHead, took time.Duration, space *entity.Space, index int) {
				bytes, err := ToContent(sr, head, took, space)
				if err == nil {
					respMap := make(map[int][]byte)
					respMap[index] = bytes
					respChain <- respMap
				}
				wg.Done()
			}(sr, head, took, space, i)
		}
		wg.Wait()
		close(respChain)

		byteArr := make([][]byte, len(srs))
		for resp := range respChain {
			for index, value := range resp {
				byteArr[index] = value
			}
		}

		for i := 0; i < len(srs); i++ {
			builder.ValueRaw(string(byteArr[i]))
			if i+1 < len(srs) {
				builder.More()
			}
		}
	} else {
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
				//log.Error("vector.Field.value len %d, source is [%s]", len(fv.Value), s)
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

func MakeQueryFeature(floatFeatureMap map[string][]float32, binaryFeatureMap map[string][]int32, query_type string) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field(query_type)

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

func IndexResponseToContent(shards *vearchpb.SearchStatus) ([]byte, error) {
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

	builder.EndObject()

	return builder.Output()
}

func deleteByQueryResult(resp *vearchpb.DelByQueryeResponse) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("code")
	if resp.Head == nil || resp.Head.Err == nil {
		builder.ValueNumeric(0)
		builder.More()
		builder.Field("msg")
		builder.ValueString("success")
	} else {
		if resp.Head != nil && resp.Head.Err != nil {
			builder.ValueNumeric(int64(resp.Head.Err.Code))
			builder.More()
			builder.Field("msg")
			builder.ValueString(resp.Head.Err.Msg)
		} else {
			builder.ValueNumeric(1)
		}
	}

	builder.More()

	builder.Field("total")
	builder.ValueNumeric(int64(resp.DelNum))

	builder.More()
	builder.Field("document_ids")
	if resp.IdsStr != nil {
		builder.ValueInterface(resp.IdsStr)
	} else if resp.IdsLong != nil {
		builder.ValueInterface(resp.IdsLong)
	} else {
		builder.ValueString("[]")
	}

	builder.EndObject()

	return builder.Output()
}

func docPrintLogSwitchResponse(printLogSwitch bool) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	builder.BeginObject()
	builder.Field("print_log_switch")
	builder.ValueBool(printLogSwitch)
	builder.EndObject()

	return builder.Output()
}
