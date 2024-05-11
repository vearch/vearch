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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/cbbytes"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine/mapping"
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
			docFieldSerialize(doc, space, returnFieldsMap, true, docMap)
		}

		if item.Err != nil {
			docMap["msg"] = item.Err.Msg
		}
		response = append(response, docMap)
	}

	var jsonData []byte
	var err error
	if isBatch || len(response) > 1 {
		jsonData, err = vjson.Marshal(response)
	} else {
		jsonData, err = vjson.Marshal(response[0])
	}

	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func documentUpsertResponse(args *vearchpb.BulkRequest, reply *vearchpb.BulkResponse) (map[string]interface{}, error) {
	if args == nil || reply == nil || reply.Items == nil || len(reply.Items) < 1 {
		if reply.GetHead() != nil && reply.GetHead().Err != nil && reply.GetHead().Err.Code != vearchpb.ErrorEnum_SUCCESS {
			err := reply.GetHead().Err
			return nil, vearchpb.NewError(err.Code, errors.New(err.Msg))
		}
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil)
	}

	response := make(map[string]interface{})

	if reply.Head != nil && reply.Head.Err != nil {
		if reply.Head.Err.Code != vearchpb.ErrorEnum_SUCCESS {
			return nil, vearchpb.NewError(reply.Head.Err.Code, nil)
		}
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
		result := documentResultSerialize(item)
		documentIDs = append(documentIDs, result)
	}

	response["document_ids"] = documentIDs

	return response, nil
}

func documentResultSerialize(item *vearchpb.Item) map[string]interface{} {
	result := make(map[string]interface{})
	if item == nil {
		result["msg"] = "duplicate id"
		result["code"] = http.StatusInternalServerError
		return result
	}

	doc := item.Doc
	result["_id"] = doc.PKey

	if item.Err != nil {
		if item.Err.Msg != "success" {
			result["code"] = http.StatusNotFound
			result["msg"] = item.Err.Msg
		}
	}
	return result
}

func documentGetResponse(client *client.Client, args *vearchpb.GetRequest, reply *vearchpb.GetResponse, returnFieldsMap map[string]string, vectorValue bool) (map[string]interface{}, error) {
	if args == nil || reply == nil || len(reply.Items) < 1 {
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

	if reply.Head != nil && reply.Head.Err != nil {
		if reply.Head.Err.Code != vearchpb.ErrorEnum_SUCCESS {
			return nil, vearchpb.NewError(reply.Head.Err.Code, errors.New(reply.Head.Err.Msg))
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
			doc["code"] = http.StatusNotFound
			doc["msg"] = item.Err.Msg
		}

		if item.Doc.Fields != nil {
			nextDocid, _ := docFieldSerialize(item.Doc, space, returnFieldsMap, vectorValue, doc)
			if nextDocid > 0 {
				doc["_docid"] = strconv.Itoa(int(nextDocid))
			}
		}
		documents = append(documents, doc)
	}
	response["documents"] = documents

	return response, nil
}

func documentQueryResponse(srs []*vearchpb.SearchResult, head *vearchpb.ResponseHead) (map[string]interface{}, error) {
	response := make(map[string]interface{})

	if head != nil && head.Err != nil {
		if head.Err.Code != vearchpb.ErrorEnum_SUCCESS {
			return nil, vearchpb.NewError(head.Err.Code, errors.New(head.Err.Msg))
		}
	}

	if len(srs) == 0 {
		response["total"] = 0
	} else {
		response["total"] = len(srs[0].ResultItems)
	}

	var documents []json.RawMessage
	if len(srs) > 1 {
		var wg sync.WaitGroup
		resp := make([][]byte, len(srs))
		for i, sr := range srs {
			wg.Add(1)
			go func(sr *vearchpb.SearchResult, index int) {
				defer wg.Done()

				bytes, err := documentQueryToContent(sr.ResultItems)
				if err != nil {
					return
				}
				resp[index] = json.RawMessage(bytes)
			}(sr, i)
		}

		wg.Wait()

		for _, msg := range resp {
			documents = append(documents, msg)
		}
	} else {
		for _, sr := range srs {
			bytes, err := documentQueryToContent(sr.ResultItems)
			if err != nil {
				return nil, err
			}
			documents = append(documents, json.RawMessage(bytes))
		}
	}

	if len(documents) > 0 {
		response["documents"] = documents[0]
	} else {
		response["documents"] = []json.RawMessage{}
	}

	return response, nil
}

func documentSearchResponse(srs []*vearchpb.SearchResult, head *vearchpb.ResponseHead) (map[string]interface{}, error) {
	response := make(map[string]interface{})

	if head != nil && head.Err != nil {
		if head.Err.Code != vearchpb.ErrorEnum_SUCCESS {
			return nil, vearchpb.NewError(head.Err.Code, errors.New(head.Err.Msg))
		}
	}

	var documents []json.RawMessage
	if len(srs) > 1 {
		var wg sync.WaitGroup
		resp := make([][]byte, len(srs))
		for i, sr := range srs {
			wg.Add(1)
			go func(sr *vearchpb.SearchResult, index int) {
				defer wg.Done()

				bytes, err := documentSearchToContent(sr.ResultItems)
				if err != nil {
					return
				}
				resp[index] = json.RawMessage(bytes)
			}(sr, i)
		}

		wg.Wait()

		for _, msg := range resp {
			documents = append(documents, msg)
		}
	} else {
		for _, sr := range srs {
			bytes, err := documentSearchToContent(sr.ResultItems)
			if err != nil {
				return nil, err
			}
			documents = append(documents, json.RawMessage(bytes))
		}
	}
	response["documents"] = documents
	return response, nil
}

func documentQueryToContent(dh []*vearchpb.ResultItem) ([]byte, error) {
	contents := make([]map[string]interface{}, 0)

	for _, u := range dh {
		content := make(map[string]interface{})
		content["_id"] = u.PKey

		if u.Source != nil {
			var source map[string]interface{}
			if err := vjson.Unmarshal(u.Source, &source); err != nil {
				log.Error("DocToContent Source Unmarshal error:%v", err)
			} else {
				for k, v := range source {
					if _, exists := content[k]; !exists {
						content[k] = v
					}
				}
			}
		}

		contents = append(contents, content)
	}

	return vjson.Marshal(contents)
}

func documentSearchToContent(dh []*vearchpb.ResultItem) ([]byte, error) {
	contents := make([]map[string]interface{}, 0)

	for _, u := range dh {
		content := make(map[string]interface{})
		content["_id"] = u.PKey

		if u.Fields != nil {
			content["_score"] = &u.Score
		}

		if u.Source != nil {
			var source map[string]interface{}
			if err := vjson.Unmarshal(u.Source, &source); err != nil {
				log.Error("DocToContent Source Unmarshal error:%v", err)
			} else {
				for k, v := range source {
					if _, exists := content[k]; !exists {
						content[k] = v
					}
				}
			}
		}

		contents = append(contents, content)
	}

	return vjson.Marshal(contents)
}

func documentDeleteResponse(items []*vearchpb.Item, head *vearchpb.ResponseHead, resultIds []string) (map[string]interface{}, error) {
	response := make(map[string]interface{})

	for _, item := range items {
		if item.Err == nil {
			resultIds = append(resultIds, item.Doc.PKey)
		}
	}

	if head != nil && head.Err != nil {
		if head.Err.Code != 0 {
			return nil, vearchpb.NewError(head.Err.Code, errors.New(head.Err.Msg))
		}
	}

	response["total"] = len(resultIds)

	response["document_ids"] = resultIds
	if len(resultIds) == 0 {
		response["document_ids"] = []string{}
	}

	return response, nil
}

func docFieldSerialize(doc *vearchpb.Document, space *entity.Space, returnFieldsMap map[string]string, vectorValue bool, docOut map[string]interface{}) (nextDocid int32, err error) {
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
		spaceProperties = spacePro
	}
	nextDocid = -1
	for _, fv := range doc.Fields {
		name := fv.Name
		if name == mapping.IdField && returnFieldsMap == nil {
			docOut[name] = string(fv.Value)
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
			case vearchpb.FieldType_STRING:
				tempValue := string(fv.Value)
				docOut[name] = tempValue
			case vearchpb.FieldType_STRINGARRAY:
				tempValue := string(fv.Value)
				docOut[name] = strings.Split(tempValue, string([]byte{'\001'}))
			case vearchpb.FieldType_INT:
				docOut[name] = cbbytes.Bytes2Int32(fv.Value)
			case vearchpb.FieldType_LONG:
				docOut[name] = cbbytes.Bytes2Int(fv.Value)
			case vearchpb.FieldType_BOOL:
				if cbbytes.Bytes2Int(fv.Value) == 0 {
					docOut[name] = false
				} else {
					docOut[name] = true
				}
			case vearchpb.FieldType_DATE:
				u := cbbytes.Bytes2Int(fv.Value)
				docOut[name] = time.Unix(u/1e6, u%1e6)
			case vearchpb.FieldType_FLOAT:
				docOut[name] = cbbytes.ByteToFloat32(fv.Value)
			case vearchpb.FieldType_DOUBLE:
				docOut[name] = cbbytes.ByteToFloat64New(fv.Value)
			case vearchpb.FieldType_VECTOR:
				if !vectorValue {
					break
				}
				if space.Index.Type == "BINARYIVF" {
					featureByteC := fv.Value
					dimension := field.Dimension
					unit8s, err := cbbytes.ByteToVectorBinary(featureByteC, dimension)
					if err != nil {
						return nextDocid, err
					}
					docOut[name] = unit8s
				} else {
					float32s, err := cbbytes.ByteToVectorForFloat32(fv.Value)
					if err != nil {
						return nextDocid, err
					}
					docOut[name] = float32s
				}

			default:
				log.Warn("can not set value by type:[%v] ", field.FieldType)
			}
		}
	}

	return nextDocid, err
}

func GetVectorFieldValue(doc *vearchpb.Document, space *entity.Space) (floatFeatureMap map[string][]float32, binaryFeatureMap map[string][]int32, err error) {
	source := make(map[string]interface{})
	spaceProperties := space.SpaceProperties
	if spaceProperties == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
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
		case vearchpb.FieldType_STRING:
			tempValue := string(fv.Value)
			source[name] = tempValue
		case vearchpb.FieldType_STRINGARRAY:
			tempValue := string(fv.Value)
			source[name] = strings.Split(tempValue, string([]byte{'\001'}))
		case vearchpb.FieldType_INT:
			source[name] = cbbytes.Bytes2Int32(fv.Value)
		case vearchpb.FieldType_LONG:
			source[name] = cbbytes.Bytes2Int(fv.Value)
		case vearchpb.FieldType_BOOL:
			if cbbytes.Bytes2Int(fv.Value) == 0 {
				source[name] = false
			} else {
				source[name] = true
			}
		case vearchpb.FieldType_DATE:
			u := cbbytes.Bytes2Int(fv.Value)
			source[name] = time.Unix(u/1e6, u%1e6)
		case vearchpb.FieldType_FLOAT:
			source[name] = cbbytes.ByteToFloat32(fv.Value)
		case vearchpb.FieldType_DOUBLE:
			source[name] = cbbytes.ByteToFloat64New(fv.Value)
		case vearchpb.FieldType_VECTOR:
			if space.Index.Type == "BINARYIVF" {
				featureByteC := fv.Value
				dimension := field.Dimension
				if dimension != 0 {
					unit8s, err := cbbytes.ByteToVectorBinary(featureByteC, dimension)
					if err != nil {
						return nil, nil, err
					}
					source[name] = unit8s
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
				float32s, err := cbbytes.ByteToVectorForFloat32(fv.Value)
				if err != nil {
					return nil, nil, err
				}
				source[name] = float32s
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

func ForceMergeToContent(shards *vearchpb.SearchStatus) ([]byte, error) {
	response := map[string]interface{}{
		"_shards": shards,
	}

	return vjson.Marshal(response)
}

func FlushToContent(shards *vearchpb.SearchStatus) ([]byte, error) {
	response := map[string]interface{}{
		"_shards": shards,
	}

	return vjson.Marshal(response)
}

func IndexResponseToContent(shards *vearchpb.SearchStatus) map[string]interface{} {
	response := map[string]interface{}{
		"_shards": shards,
	}

	return response
}

func SearchNullToContent(searchStatus *vearchpb.SearchStatus, took time.Duration) ([]byte, error) {
	response := map[string]interface{}{
		"took":      int64(took) / 1e6,
		"timed_out": false,
		"_shards":   searchStatus,
		"hits": map[string]interface{}{
			"total":     0,
			"max_score": -1,
		},
	}

	return vjson.Marshal(response)
}

func deleteByQueryResult(resp *vearchpb.DelByQueryeResponse) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	if resp.Head != nil && resp.Head.Err != nil {
		if resp.Head.Err.Code != vearchpb.ErrorEnum_SUCCESS {
			return nil, vearchpb.NewError(resp.Head.Err.Code, nil)
		}
	}
	result["total"] = resp.DelNum

	if resp.IdsStr != nil {
		result["document_ids"] = resp.IdsStr
	} else if resp.IdsLong != nil {
		result["document_ids"] = resp.IdsLong
	} else {
		result["document_ids"] = []string{}
	}

	return result, nil
}

func docPrintLogSwitchResponse(printLogSwitch bool) ([]byte, error) {
	response := map[string]bool{
		"print_log_switch": printLogSwitch,
	}

	return vjson.Marshal(response)
}
