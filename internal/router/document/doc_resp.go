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

	"github.com/bytedance/sonic"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/internal/client"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/entity/request"
	"github.com/vearch/vearch/internal/pkg/cbbytes"
	"github.com/vearch/vearch/internal/pkg/cbjson"
	"github.com/vearch/vearch/internal/pkg/log"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/ps/engine/mapping"
)

func docGetResponse(client *client.Client, args *vearchpb.GetRequest, reply *vearchpb.GetResponse, returnFieldsMap map[string]string, isBatch bool) ([]byte, error) {
	if args == nil || reply == nil || reply.Items == nil || len(reply.Items) < 1 {
		if reply.GetHead() != nil && reply.GetHead().Err != nil && reply.GetHead().Err.Code != vearchpb.ErrorEnum_SUCCESS {
			err := reply.GetHead().Err
			return nil, vearchpb.NewError(err.Code, errors.New(err.Msg))
		}
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}

	var response []interface{}

	for _, item := range reply.Items {
		doc := item.Doc
		space, err := client.Space(context.Background(), args.Head.DbName, args.Head.SpaceName)
		if err != nil {
			return nil, err
		}

		docMap := make(map[string]interface{})
		docMap["_index"] = args.Head.DbName
		docMap["_type"] = args.Head.SpaceName
		docMap["_id"] = doc.PKey

		docMap["found"] = doc.Fields != nil
		if doc.Fields != nil {
			source, _, _ := docFieldSerialize(doc, space, returnFieldsMap, true)
			docMap["_source"] = source
		}

		if item.Err != nil {
			docMap["msg"] = item.Err.Msg
		}
		response = append(response, docMap)
	}

	var jsonData []byte
	var err error
	if isBatch || len(response) > 1 {
		jsonData, err = sonic.Marshal(response)
	} else {
		jsonData, err = sonic.Marshal(response[0])
	}

	if err != nil {
		return nil, err
	}

	return jsonData, nil
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

	results := make([]map[string]interface{}, len(items))
	for i, item := range items {
		result := docResult(space, head, item)
		results[i] = result
	}

	return sonic.Marshal(results)
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

	response := make(map[string]interface{})
	response["code"] = 0

	if reply.Head != nil && reply.Head.Err != nil {
		response["code"] = reply.Head.Err.Code
		response["msg"] = reply.Head.Err.Msg
	}

	var total int64
	for _, item := range reply.Items {
		if item.Err == nil || (item.Err.Msg == "success" && item.Err.Code == vearchpb.ErrorEnum_SUCCESS) {
			total++
		}
	}

	response["total"] = total

	documentIDs := make([]interface{}, 0)
	for _, item := range reply.Items {
		result := documentResultSerialize(space, item)
		documentIDs = append(documentIDs, result)
	}

	response["document_ids"] = documentIDs

	jsonData, err := sonic.Marshal(response)
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func documentResultSerialize(space *entity.Space, item *vearchpb.Item) map[string]interface{} {
	result := make(map[string]interface{})
	if item == nil {
		result["error"] = "duplicate id"
		return result
	}

	doc := item.Doc
	result["_id"] = doc.PKey

	if item.Err != nil {
		result["status"] = vearchpb.ErrCode(item.Err.Code)
		result["error"] = item.Err.Msg
	} else {
		result["status"] = vearchpb.ErrCode(vearchpb.ErrorEnum_SUCCESS)
	}
	return result
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

	response := make(map[string]interface{})

	if reply.Head == nil || reply.Head.Err == nil {
		response["code"] = vearchpb.ErrorEnum_SUCCESS
		response["msg"] = "success"
	} else {
		if reply.Head != nil && reply.Head.Err != nil {
			response["code"] = reply.Head.Err.Code
			response["msg"] = reply.Head.Err.Msg
		} else {
			response["code"] = vearchpb.ErrorEnum_INTERNAL_ERROR
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
	response["total"] = total

	documents := []interface{}{}
	for _, item := range reply.Items {
		doc := make(map[string]interface{})
		doc["_id"] = item.Doc.PKey

		if item.Err != nil {
			doc["status"] = cast.ToInt64(vearchpb.ErrCode(item.Err.Code))
			doc["error"] = item.Err.Msg
		}

		if item.Doc.Fields != nil {
			source, nextDocid, _ := docFieldSerialize(item.Doc, space, returnFieldsMap, vectorValue)
			if nextDocid > 0 {
				doc["_id"] = strconv.Itoa(int(nextDocid))
			}
			doc["_source"] = source
		}
		documents = append(documents, doc)
	}
	response["documents"] = documents

	return sonic.Marshal(response)
}

func documentSearchResponse(srs []*vearchpb.SearchResult, head *vearchpb.ResponseHead, space *entity.Space, response_type string) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field("code")
	if head == nil || head.Err == nil {
		builder.ValueNumeric(int64(vearchpb.ErrorEnum_SUCCESS))
		builder.More()
		builder.Field("msg")
		builder.ValueString("success")
	} else {
		if head.Err != nil {
			builder.ValueNumeric(int64(head.Err.Code))
			builder.More()
			builder.Field("msg")
			builder.ValueString(head.Err.Msg)
		} else {
			builder.ValueNumeric(int64(vearchpb.ErrorEnum_INTERNAL_ERROR))
		}
	}

	if response_type == request.QueryResponse {
		if srs == nil {
			builder.More()
			builder.Field("total")
			builder.ValueNumeric(0)
		} else {
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
	builder.EndObject()

	return builder.Output()
}

func documentToContent(dh []*vearchpb.ResultItem, space *entity.Space, response_type string) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()
	if response_type == request.SearchResponse {
		builder.BeginArray()
	}
	for i, u := range dh {

		if i != 0 {
			builder.More()
		}
		builder.BeginObject()

		builder.Field("_id")
		builder.ValueString(u.PKey)

		if u.Fields != nil {
			if response_type == request.SearchResponse {
				builder.More()
				builder.Field("_score")
				builder.ValueFloat(float64(u.Score))
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
	if response_type == request.SearchResponse {
		builder.EndArray()
	}

	return builder.Output()
}

func documentDeleteResponse(items []*vearchpb.Item, head *vearchpb.ResponseHead, resultIds []string) ([]byte, error) {
	response := make(map[string]interface{})

	for _, item := range items {
		if item.Err == nil {
			resultIds = append(resultIds, item.Doc.PKey)
		}
	}

	if head == nil || head.Err == nil {
		response["code"] = vearchpb.ErrorEnum_SUCCESS
		response["msg"] = "success"
	} else {
		response["code"] = head.Err.Code
		response["msg"] = head.Err.Msg
	}

	response["total"] = len(resultIds)

	response["document_ids"] = resultIds
	if len(resultIds) == 0 {
		response["document_ids"] = []string{}
	}

	return sonic.Marshal(response)
}

func docResult(space *entity.Space, head *vearchpb.RequestHead, item *vearchpb.Item) map[string]interface{} {
	result := make(map[string]interface{})

	result["_index"] = head.DbName
	result["_type"] = head.SpaceName

	if item == nil {
		result["error"] = "duplicate id"
		return result
	}

	doc := item.Doc
	result["_id"] = doc.PKey

	if item.Err != nil {
		result["status"] = vearchpb.ErrCode(item.Err.Code)
		result["error"] = item.Err.Msg
	} else {
		result["status"] = vearchpb.ErrCode(vearchpb.ErrorEnum_SUCCESS)
	}

	return result
}

func docResultSerialize(space *entity.Space, head *vearchpb.RequestHead, item *vearchpb.Item) ([]byte, error) {
	return sonic.Marshal(docResult(space, head, item))
}

func docFieldSerialize(doc *vearchpb.Document, space *entity.Space, returnFieldsMap map[string]string, vectorValue bool) (marshal json.RawMessage, nextDocid int32, err error) {
	source := make(map[string]interface{})
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		spaceProperties = spacePro
	}
	nextDocid = -1
	for _, fv := range doc.Fields {
		name := fv.Name
		if name == mapping.IdField && returnFieldsMap == nil {
			source[name] = string(fv.Value)
			continue
		}
		if (returnFieldsMap != nil && returnFieldsMap[name] != "") || returnFieldsMap == nil {
			field := spaceProperties[name]
			if field == nil {
				if name == "_docid" {
					nextDocid = cbbytes.Bytes2Int32(fv.Value)
					continue
				}
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
								return nil, nextDocid, err
							}
							source[name] = map[string]interface{}{
								"feature": unit8s,
							}
						} else {
							log.Error("GetSource can not found dimension by field:[%s]", name)
						}

					} else {
						float32s, _, err := cbbytes.ByteToVector(fv.Value)
						if err != nil {
							return nil, nextDocid, err
						}
						source[name] = map[string]interface{}{
							"feature": float32s,
						}
					}
				}

			default:
				log.Warn("can not set value by type:[%v] ", field.FieldType)
			}
		}
	}
	if len(source) > 0 {
		marshal, err = json.Marshal(source)
	}
	if err != nil {
		return nil, nextDocid, err
	}
	return marshal, nextDocid, nil
}

func ToContentBytes(sr *vearchpb.SearchResult, head *vearchpb.RequestHead, took time.Duration, space *entity.Space) ([]byte, error) {
	content, err := toContent(sr, head, took, space)
	if err != nil {
		return nil, err
	}
	return sonic.Marshal(content)
}

func toContent(sr *vearchpb.SearchResult, head *vearchpb.RequestHead, took time.Duration, space *entity.Space) (map[string]interface{}, error) {
	hitsContent, err := docToContent(sr.ResultItems, head)
	if err != nil {
		return nil, err
	}

	content := map[string]interface{}{
		"took":      int64(took) / 1e6,
		"timed_out": sr.Timeout,
		"_shards":   sr.Status,
		"hits": map[string]interface{}{
			"total":     len(sr.ResultItems),
			"max_score": sr.MaxScore,
			"hits":      hitsContent,
		},
	}

	return content, nil
}

func docToContent(dh []*vearchpb.ResultItem, head *vearchpb.RequestHead) ([]interface{}, error) {
	docContents := make([]interface{}, len(dh))
	for i, u := range dh {
		docContent := make(map[string]interface{})
		docContent["_index"] = head.DbName
		docContent["_type"] = head.SpaceName
		docContent["_id"] = u.PKey

		if u.Fields != nil {
			docContent["_score"] = float64(u.Score)
			if u.Source != nil {
				var sourceJson json.RawMessage
				if err := json.Unmarshal(u.Source, &sourceJson); err != nil {
					log.Error("Error unmarshaling _source: %v", err)
					return nil, err
				}
				docContent["_source"] = sourceJson
			}
		}

		docContents[i] = docContent
	}

	return docContents, nil
}

func ToContents(srs []*vearchpb.SearchResult, head *vearchpb.RequestHead, took time.Duration, space *entity.Space) ([]byte, error) {
	results := make([]map[string]interface{}, len(srs))

	if len(srs) > 1 {
		var wg sync.WaitGroup
		respChan := make(chan struct {
			Index   int
			Content map[string]interface{}
		}, len(srs))
		for i, sr := range srs {
			wg.Add(1)
			go func(sr *vearchpb.SearchResult, index int) {
				defer wg.Done()
				if content, err := toContent(sr, head, took, space); err == nil {
					respChan <- struct {
						Index   int
						Content map[string]interface{}
					}{Index: index, Content: content}
				}
			}(sr, i)
		}
		wg.Wait()
		close(respChan)

		for resp := range respChan {
			results[resp.Index] = resp.Content
		}
	} else {
		for i, sr := range srs {
			if content, err := toContent(sr, head, took, space); err == nil {
				results[i] = content
			} else {
				return nil, err
			}
		}
	}

	response := map[string]interface{}{
		"took":    int64(took) / 1e6,
		"results": results,
	}

	return sonic.Marshal(response)
}

func ToContentIds(srs []*vearchpb.SearchResult, space *entity.Space) ([]byte, error) {
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
						bs.WriteString("\"")
						bs.WriteString(string(fv.Value))
						bs.WriteString("\"")
					}
				}
			}
		}
	}
	bs.WriteString("]")
	return bs.Bytes(), nil
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
		case entity.FieldType_DOUBLE:
			source[name] = cbbytes.ByteToFloat64New(fv.Value)
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
			log.Warn("can not set value by name:[%v], type:[%v] ", name, field.FieldType)
		}
	}
	return floatFeatureMap, binaryFeatureMap, nil
}

func MakeQueryFeature(floatFeatureMap map[string][]float32, binaryFeatureMap map[string][]int32, query_type string) ([]byte, error) {
	features := make([]map[string]interface{}, 0)

	if floatFeatureMap != nil {
		for key, value := range floatFeatureMap {
			feature := map[string]interface{}{
				"field":   key,
				"feature": value,
			}
			features = append(features, feature)
		}
	} else {
		for key, value := range binaryFeatureMap {
			feature := map[string]interface{}{
				"field":   key,
				"feature": value,
			}
			features = append(features, feature)
		}
	}

	query := map[string]interface{}{
		query_type: features,
	}

	return sonic.Marshal(query)
}

func ForceMergeToContent(shards *vearchpb.SearchStatus) ([]byte, error) {
	response := map[string]interface{}{
		"_shards": shards,
	}

	return sonic.Marshal(response)
}

func FlushToContent(shards *vearchpb.SearchStatus) ([]byte, error) {
	response := map[string]interface{}{
		"_shards": shards,
	}

	return sonic.Marshal(response)
}

func IndexResponseToContent(shards *vearchpb.SearchStatus) ([]byte, error) {
	response := map[string]interface{}{
		"_shards": shards,
	}

	return sonic.Marshal(response)
}

func SearchNullToContent(searchStatus vearchpb.SearchStatus, took time.Duration) ([]byte, error) {
	response := map[string]interface{}{
		"took":      int64(took) / 1e6,
		"timed_out": false,
		"_shards":   searchStatus,
		"hits": map[string]interface{}{
			"total":     0,
			"max_score": -1,
		},
	}

	return sonic.Marshal(response)
}

func deleteByQueryResult(resp *vearchpb.DelByQueryeResponse) ([]byte, error) {
	result := make(map[string]interface{})

	if resp.Head == nil || resp.Head.Err == nil {
		result["code"] = 0
		result["msg"] = "success"
	} else {
		result["code"] = resp.Head.Err.Code
		result["msg"] = resp.Head.Err.Msg
	}

	result["total"] = resp.DelNum

	if resp.IdsStr != nil {
		result["document_ids"] = resp.IdsStr
	} else if resp.IdsLong != nil {
		result["document_ids"] = resp.IdsLong
	} else {
		result["document_ids"] = []string{}
	}

	return sonic.Marshal(result)
}

func docPrintLogSwitchResponse(printLogSwitch bool) ([]byte, error) {
	response := map[string]bool{
		"print_log_switch": printLogSwitch,
	}

	return sonic.Marshal(response)
}
