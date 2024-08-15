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
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/entity/request"
	"github.com/vearch/vearch/v3/internal/pkg/cbbytes"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine/mapping"
	"github.com/vearch/vearch/v3/internal/ps/engine/sortorder"
)

const (
	URLQueryFrom     = "from"
	UrlQueryRouting  = "routing"
	UrlQueryTypedKey = "typed_keys"
	UrlQueryVersion  = "version"
	UrlQueryOpType   = "op_type"
	UrlQueryTimeout  = "timeout"
	DefaultSize      = 50
	WeightedRanker   = "WeightedRanker"
)

type VectorQuery struct {
	Field        string          `json:"field"`
	FeatureData  json.RawMessage `json:"feature"`
	Feature      []float32       `json:"-"`
	FeatureUint8 []uint8         `json:"-"`
	Symbol       string          `json:"symbol"`
	Value        *float64        `json:"value"`
	Format       *string         `json:"format,omitempty"`
	MinScore     *float64        `json:"min_score,omitempty"`
	MaxScore     *float64        `json:"max_score,omitempty"`
	IndexType    string          `json:"index_type"`
}

type Range struct {
	Gt  json.RawMessage
	Gte json.RawMessage
	Lt  json.RawMessage
	Lte json.RawMessage
}

type Term struct {
	Value json.RawMessage
}

func parseFilter(filters *request.Filter, space *entity.Space) ([]*vearchpb.RangeFilter, []*vearchpb.TermFilter, error) {
	rfs := make([]*vearchpb.RangeFilter, 0)
	tfs := make([]*vearchpb.TermFilter, 0)

	var err error

	proMap := space.SpaceProperties
	if proMap == nil {
		proMap, err = entity.UnmarshalPropertyJSON(space.Fields)
		if err != nil {
			return nil, nil, err
		}
	}

	if filters != nil {
		if filters.Operator != "AND" {
			return nil, nil, vearchpb.NewError(vearchpb.ErrorEnum_FILTER_OPERATOR_TYPE_ERR, nil)
		}
		rangeConditionMap := make(map[string]*Range)
		termConditionMap := make(map[string]*Term)
		for _, condition := range filters.Conditions {
			if condition.Operator == "<" {
				cm, ok := rangeConditionMap[condition.Field]
				if !ok {
					cm = &Range{
						Lt: condition.Value,
					}
					rangeConditionMap[condition.Field] = cm
				} else {
					cm.Lt = condition.Value
				}
			} else if condition.Operator == "<=" {
				cm, ok := rangeConditionMap[condition.Field]
				if !ok {
					cm = &Range{
						Lte: condition.Value,
					}
					rangeConditionMap[condition.Field] = cm
				} else {
					cm.Lte = condition.Value
				}
			} else if condition.Operator == ">" {
				cm, ok := rangeConditionMap[condition.Field]
				if !ok {
					cm = &Range{
						Gt: condition.Value,
					}
					rangeConditionMap[condition.Field] = cm
				} else {
					cm.Gt = condition.Value
				}
			} else if condition.Operator == ">=" {
				cm, ok := rangeConditionMap[condition.Field]
				if !ok {
					cm = &Range{
						Gte: condition.Value,
					}
					rangeConditionMap[condition.Field] = cm
				} else {
					cm.Gte = condition.Value
				}
			} else if condition.Operator == "IN" {
				tmp := make([]string, 0)
				err := json.Unmarshal(condition.Value, &tmp)
				if err != nil {
					log.Error(err)
					return nil, nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, err)
				}
				tm, ok := termConditionMap[condition.Field]
				if !ok {
					tm = &Term{
						Value: condition.Value,
					}
					termConditionMap[condition.Field] = tm
				} else {
					tm.Value = condition.Value
				}
			} else {
				return nil, nil, vearchpb.NewError(vearchpb.ErrorEnum_FILTER_CONDITION_OPERATOR_TYPE_ERR, nil)
			}
		}
		filter, err := parseRange(rangeConditionMap, proMap)
		if err != nil {
			return nil, nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("parseRange err %s", err.Error()))
		}
		if len(filter) != 0 {
			rfs = append(rfs, filter...)
		}
		tmFilter, err := parseTerm(termConditionMap, proMap)
		if err != nil {
			return nil, nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("parseTerm err %s", err.Error()))
		}
		if len(tmFilter) != 0 {
			tfs = append(tfs, tmFilter...)
		}
	}

	return rfs, tfs, nil
}

func parseSearch(vectors []json.RawMessage, filters *request.Filter, req *vearchpb.SearchRequest, space *entity.Space) error {
	vqs := make([]*vearchpb.VectorQuery, 0)

	var err error
	var reqNum int

	if len(vectors) > 0 {
		req.MultiVectorRank = 1
		if reqNum, vqs, err = parseVectors(reqNum, vqs, vectors, space); err != nil {
			return err
		}
	}
	if len(vqs) > 0 {
		req.VecFields = vqs
	}

	rfs, tfs, err := parseFilter(filters, space)
	if err != nil {
		return err
	}
	if len(rfs) > 0 {
		req.RangeFilters = rfs
	}
	if len(tfs) > 0 {
		req.TermFilters = tfs
	}

	if reqNum <= 0 {
		reqNum = 1
	}

	req.ReqNum = int32(reqNum)
	return nil
}

func parseRanker(data json.RawMessage, req *vearchpb.SearchRequest) error {
	ranker := &request.Ranker{}
	err := vjson.Unmarshal(data, ranker)
	if err != nil {
		err = vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("ranker param convert json %s err: %v", string(data), err))
		return err
	}
	if ranker.Type != WeightedRanker {
		err = vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unsupport ranker type: %s, now only support %s", ranker.Type, WeightedRanker))
		return err
	}
	// TODO
	// check ranker.Params
	req.Ranker = string(data)
	return nil
}

func unmarshalArray[T any](data []byte, dimension int) ([]T, error) {
	if len(data) < dimension {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("vector embedding length [%d] err, should be:[%d]", len(data), dimension))
	}

	var result []T
	if err := vjson.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	if len(result) > 0 {
		if _, ok := any(result).([]float32); ok && (len(result)%dimension) != 0 {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("vector embedding length [%d] err, dimension is [%d], not equals dimension multiple:[%d]", len(result), dimension, (len(result)%dimension)))
		}
	}

	return result, nil
}

func parseVectors(reqNum int, vqs []*vearchpb.VectorQuery, tmpArr []json.RawMessage, space *entity.Space) (int, []*vearchpb.VectorQuery, error) {
	var err error
	indexType := space.Index.Type
	proMap := space.SpaceProperties
	if proMap == nil {
		proMap, _ = entity.UnmarshalPropertyJSON(space.Fields)
	}
	for i := 0; i < len(tmpArr); i++ {
		vqTemp := &VectorQuery{}
		if err = vjson.Unmarshal(tmpArr[i], vqTemp); err != nil {
			return reqNum, vqs, err
		}

		if vqTemp.IndexType != "" {
			indexType = vqTemp.IndexType
		}
		docField := proMap[vqTemp.Field]

		if docField == nil {
			return reqNum, vqs, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field:[%s] not found in space fields", vqTemp.Field))
		}

		if docField.FieldType != vearchpb.FieldType_VECTOR {
			return reqNum, vqs, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field:[%s] is not vector type", vqTemp.Field))
		}

		if vqTemp.FeatureData == nil || len(vqTemp.FeatureData) == 0 {
			return reqNum, vqs, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("vector embedding is null"))
		}

		d := docField.Dimension
		queryNum := 0
		validate := 0
		if indexType == "BINARYIVF" {
			if vqTemp.FeatureUint8, err = unmarshalArray[uint8](vqTemp.FeatureData, d/8); err != nil {
				return reqNum, vqs, err
			}
			queryNum = len(vqTemp.FeatureUint8) / (d / 8)
			validate = len(vqTemp.FeatureUint8) % (d / 8)
		} else {
			if vqTemp.Feature, err = unmarshalArray[float32](vqTemp.FeatureData, d); err != nil {
				return reqNum, vqs, err
			}
			queryNum = len(vqTemp.Feature) / d
			validate = len(vqTemp.Feature) % d
		}

		if queryNum == 0 || validate != 0 {
			return reqNum, vqs, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("vector field:[%s] embedding length [%d] err, dimension [%d] needs to be divided", vqTemp.Field, len(vqTemp.Feature), d))
		}

		if reqNum == 0 {
			reqNum = queryNum
		} else if reqNum != queryNum {
			return reqNum, vqs, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("vector field:[%s] not same as queryNum, embedding length [%d], dimension [%d] ", vqTemp.Field, len(vqTemp.Feature), d))
		}

		if indexType != "BINARYIVF" {
			if vqTemp.Format != nil && len(*vqTemp.Format) > 0 {
				switch *vqTemp.Format {
				case "normalization", "normal":
				case "no":
				default:
					return reqNum, vqs, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unknow vector process format:[%s]", *vqTemp.Format))
				}
			}
		}

		vq, err := vqTemp.ToC(indexType)
		if err != nil {
			return reqNum, vqs, err
		}
		vqs = append(vqs, vq)
	}
	return reqNum, vqs, nil
}

func parseRange(rangeConditionMap map[string]*Range, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.RangeFilter, error) {
	var (
		min, max                   interface{}
		minInclusive, maxInclusive bool
	)

	rangeFilters := make([]*vearchpb.RangeFilter, 0)

	for field, rv := range rangeConditionMap {
		docField := proMap[field]

		if docField == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field:[%s] not found in space fields", field))
		}

		if docField.FieldType == vearchpb.FieldType_STRING {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("range filter should be numberic type, field:[%s] is string which should be term filter", field))
		}

		if docField.Option&entity.FieldOption_Index != entity.FieldOption_Index {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field:[%s] not set index", field))
		}

		var start, end json.RawMessage

		if rv.Gte != nil {
			minInclusive = true
			start = rv.Gte
		} else if rv.Gt != nil {
			minInclusive = false
			start = rv.Gt
		}

		if rv.Lte != nil {
			maxInclusive = true
			end = rv.Lte
		} else if rv.Lt != nil {
			maxInclusive = false
			end = rv.Lt
		}

		switch docField.FieldType {
		case vearchpb.FieldType_INT:
			var minNum, maxNum int32

			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("INT %s Unmarshal err %s", string(start), err.Error()))
				}
			} else {
				minNum = math.MinInt32
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("INT %s Unmarshal err %s", string(end), err.Error()))
				}
			} else {
				maxNum = math.MaxInt32
			}

			min, max = minNum, maxNum
		case vearchpb.FieldType_LONG:
			var minNum, maxNum int64

			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("LONG %s Unmarshal err %s", string(start), err.Error()))
				}
			} else {
				minNum = math.MinInt64
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("LONG %s Unmarshal err %s", string(end), err.Error()))
				}
			} else {
				maxNum = math.MaxInt64
			}

			min, max = minNum, maxNum
		case vearchpb.FieldType_FLOAT:
			var minNum, maxNum float32

			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("FLOAT %s Unmarshal err %s", string(start), err.Error()))
				}
			} else {
				minNum = -math.MaxFloat32
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("FLOAT %s Unmarshal err %s", string(end), err.Error()))
				}
			} else {
				maxNum = math.MaxFloat32
			}

			min, max = minNum, maxNum
		case vearchpb.FieldType_DOUBLE:
			var minNum, maxNum float64

			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("FLOAT64 %s Unmarshal err %s", string(start), err.Error()))
				}
			} else {
				minNum = -math.MaxFloat64
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("FLOAT64 %s Unmarshal err %s", string(end), err.Error()))
				}
			} else {
				maxNum = math.MaxFloat64
			}

			min, max = minNum, maxNum
		case vearchpb.FieldType_DATE:
			var minNum, maxNum int64
			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					var dateStr string
					new_err := json.Unmarshal(start, &dateStr)
					if new_err != nil {
						return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", string(start), err.Error()))
					}
					f, err := cast.ToTimeE(dateStr)
					if err != nil {
						return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", string(start), err.Error()))
					}
					minNum = f.UnixNano()
				} else {
					minNum = minNum * 1e9
				}
			} else {
				minNum = math.MinInt64
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					var dateStr string
					if err := json.Unmarshal(start, &dateStr); err != nil {
						return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", string(start), err.Error()))
					}
					f, err := cast.ToTimeE(dateStr)
					if err != nil {
						return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", string(start), err.Error()))
					}
					maxNum = f.UnixNano()
				} else {
					maxNum = maxNum * 1e9
				}
			} else {
				maxNum = math.MaxInt64
			}
			min, max = minNum, maxNum
		}

		var minByte, maxByte []byte

		minByte, err := cbbytes.ValueToByte(min)
		if err != nil {
			return nil, err
		}

		maxByte, err = cbbytes.ValueToByte(max)
		if err != nil {
			return nil, err
		}

		if minByte == nil || maxByte == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("range filter param is null or have not gte lte"))
		}

		rangeFilter := vearchpb.RangeFilter{
			Field:        field,
			LowerValue:   minByte,
			UpperValue:   maxByte,
			IncludeLower: minInclusive,
			IncludeUpper: maxInclusive,
		}
		rangeFilters = append(rangeFilters, &rangeFilter)
	}

	return rangeFilters, nil
}

func parseTerm(tm map[string]*Term, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.TermFilter, error) {
	isUnion := int32(1)

	termFilters := make([]*vearchpb.TermFilter, 0)

	for field, rv := range tm {
		fd := proMap[field]

		if fd == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field:[%s] not found in space fields", field))
		}

		if fd.FieldType != vearchpb.FieldType_STRING && fd.FieldType != vearchpb.FieldType_STRINGARRAY {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("term filter should be string type or stringArray type, field:[%s] is numberic type which should be range filter", field))
		}

		if fd.Option&entity.FieldOption_Index != entity.FieldOption_Index {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field:[%s] not set index, please check space", field))
		}

		buf := bytes.Buffer{}
		var v interface{}
		err := vjson.Unmarshal(rv.Value, &v)
		if err != nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal [%s] err %s", string(rv.Value), err.Error()))
		}
		if ia, ok := v.([]interface{}); ok {
			for i, obj := range ia {
				buf.WriteString(cast.ToString(obj))
				if i != len(ia)-1 {
					buf.WriteRune('\001')
				}
			}
		} else {
			buf.WriteString(cast.ToString(rv.Value))
		}

		termFilter := vearchpb.TermFilter{
			Field:   field,
			Value:   buf.Bytes(),
			IsUnion: isUnion,
		}
		termFilters = append(termFilters, &termFilter)
	}

	return termFilters, nil
}

func (query *VectorQuery) ToC(indexType string) (*vearchpb.VectorQuery, error) {
	var codeByte []byte
	if indexType == "BINARYIVF" {
		code, err := cbbytes.UInt8ArrayToByteArray(query.FeatureUint8)
		if err != nil {
			return nil, err
		}
		codeByte = code
	} else {
		code, err := cbbytes.FloatArrayByte(query.Feature)
		if err != nil {
			return nil, err
		}
		codeByte = code
	}

	if query.MinScore == nil {
		minFloat64 := -math.MaxFloat64
		query.MinScore = &minFloat64
	}
	if query.MaxScore == nil {
		maxFLoat64 := math.MaxFloat64
		query.MaxScore = &maxFLoat64
	}

	if query.Value != nil {
		switch strings.TrimSpace(query.Symbol) {
		case ">":
			query.MinScore = query.Value
		case ">=":
			query.MinScore = query.Value
		case "<":
			query.MaxScore = query.Value
		case "<=":
			query.MaxScore = query.Value
		default:
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("symbol value unknow:[%s]", query.Symbol))
		}
	}

	vectorQuery := &vearchpb.VectorQuery{
		Name:      query.Field,
		Value:     codeByte,
		MinScore:  *query.MinScore,
		MaxScore:  *query.MaxScore,
		IndexType: indexType,
	}
	return vectorQuery, nil
}

func queryRequestToPb(searchDoc *request.SearchDocumentRequest, space *entity.Space, queryReq *vearchpb.QueryRequest) error {
	queryReq.IsVectorValue = searchDoc.VectorValue
	queryReq.Fields = searchDoc.Fields

	queryReq.Limit = searchDoc.Limit
	if queryReq.Limit == 0 {
		queryReq.Limit = DefaultSize
	}

	if queryReq.Head.Params != nil && queryReq.Head.Params["queryOnlyId"] != "" {
		queryReq.Fields = []string{mapping.IdField}
	} else {
		spaceProKeyMap := space.SpaceProperties
		if spaceProKeyMap == nil {
			spaceProKeyMap, _ = entity.UnmarshalPropertyJSON(space.Fields)
		}
		vectorFieldArr := make([]string, 0)
		if queryReq.Fields == nil || len(queryReq.Fields) == 0 {
			queryReq.Fields = make([]string, 0)
			spaceProKeyMap := space.SpaceProperties
			if spaceProKeyMap == nil {
				spaceProKeyMap, _ = entity.UnmarshalPropertyJSON(space.Fields)
			}
			for fieldName, property := range spaceProKeyMap {
				if property.Type != "" && strings.Compare(property.Type, "vector") != 0 {
					queryReq.Fields = append(queryReq.Fields, fieldName)
				}
				if property.Type != "" && strings.Compare(property.Type, "vector") == 0 {
					vectorFieldArr = append(vectorFieldArr, fieldName)
				}
			}
			queryReq.Fields = append(queryReq.Fields, mapping.IdField)
		} else {
			for _, field := range queryReq.Fields {
				if field != mapping.IdField {
					if spaceProKeyMap[field] == nil {
						return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field [%s] is not exist in the space", field))
					}
				}
			}
		}

		if searchDoc.VectorValue {
			queryReq.Fields = append(queryReq.Fields, vectorFieldArr...)
		}
	}

	hasID := false
	for _, f := range queryReq.Fields {
		if f == mapping.IdField {
			hasID = true
		}
	}

	if !hasID {
		queryReq.Fields = append(queryReq.Fields, mapping.IdField)
	}

	queryFieldMap := make(map[string]string)
	for _, feild := range queryReq.Fields {
		queryFieldMap[feild] = feild
	}

	sortOrder, err := searchDoc.SortOrder()
	if err != nil {
		return err
	}

	spaceProMap := space.SpaceProperties
	if spaceProMap == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
		spaceProMap = spacePro
	}
	sortFieldMap := make(map[string]string)

	sortFieldArr := make([]*vearchpb.SortField, 0, len(sortOrder))

	for _, sort := range sortOrder {
		sortField := sort.SortField()
		if !(sortField == "_score" || sortField == "_id" || (spaceProMap[sortField] != nil)) {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("sort field [%s] not space field", sortField))
		}

		sortFieldArr = append(sortFieldArr, &vearchpb.SortField{Field: sort.SortField(), Type: sort.GetSortOrder()})

		if sortField != "_score" && sortField != "_id" && queryFieldMap[sortField] == "" {
			queryReq.Fields = append(queryReq.Fields, sortField)
		}

		sortDesc := sort.GetSortOrder()
		if sortDesc {
			sortFieldMap[sortField] = "true"
		} else {
			sortFieldMap[sortField] = "false"
		}
	}

	queryReq.SortFields = sortFieldArr
	queryReq.SortFieldMap = sortFieldMap

	if searchDoc.Filters != nil {
		rfs, tfs, err := parseFilter(searchDoc.Filters, space)
		if err != nil {
			return err
		}
		if len(rfs) > 0 {
			queryReq.RangeFilters = rfs
		}
		if len(tfs) > 0 {
			queryReq.TermFilters = tfs
		}
	}
	if searchDoc.DocumentIds != nil && len(*searchDoc.DocumentIds) > 0 {
		queryReq.DocumentIds = *searchDoc.DocumentIds
		queryReq.Limit = int32(len(queryReq.DocumentIds))
	}
	if searchDoc.PartitionId != nil {
		queryReq.PartitionId = int32(*searchDoc.PartitionId)
	}
	if searchDoc.Next != nil {
		queryReq.Next = *searchDoc.Next
	}

	if queryReq.Limit <= 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("query limit[topN] is zero"))
	}

	queryReq.Head.ClientType = searchDoc.LoadBalance
	return nil
}

func requestToPb(searchDoc *request.SearchDocumentRequest, space *entity.Space, searchReq *vearchpb.SearchRequest) error {
	searchReq.IsVectorValue = searchDoc.VectorValue
	searchReq.L2Sqrt = searchDoc.L2Sqrt
	searchReq.Fields = searchDoc.Fields
	searchReq.IsBruteSearch = searchDoc.IsBruteSearch

	metricType := ""
	if searchDoc.IndexParams != nil {
		searchReq.IndexParams = string(searchDoc.IndexParams)
	}

	searchReq.TopN = searchDoc.Limit
	if searchReq.TopN == 0 {
		searchReq.TopN = DefaultSize
	}

	if searchReq.Head.Params != nil && searchReq.Head.Params["queryOnlyId"] != "" {
		searchReq.Fields = []string{mapping.IdField}
	} else {
		spaceProKeyMap := space.SpaceProperties
		if spaceProKeyMap == nil {
			spaceProKeyMap, _ = entity.UnmarshalPropertyJSON(space.Fields)
		}
		vectorFieldArr := make([]string, 0)
		if searchReq.Fields == nil || len(searchReq.Fields) == 0 {
			searchReq.Fields = make([]string, 0)
			spaceProKeyMap := space.SpaceProperties
			if spaceProKeyMap == nil {
				spaceProKeyMap, _ = entity.UnmarshalPropertyJSON(space.Fields)
			}
			for fieldName, property := range spaceProKeyMap {
				if property.Type != "" && strings.Compare(property.Type, "vector") != 0 {
					searchReq.Fields = append(searchReq.Fields, fieldName)
				}
				if property.Type != "" && strings.Compare(property.Type, "vector") == 0 {
					vectorFieldArr = append(vectorFieldArr, fieldName)
				}
			}
			searchReq.Fields = append(searchReq.Fields, mapping.IdField)
		} else {
			for _, field := range searchReq.Fields {
				if field != mapping.IdField {
					if spaceProKeyMap[field] == nil {
						return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("field [%s] is not exist in the space", field))
					}
				}
			}
		}

		if searchDoc.VectorValue {
			searchReq.Fields = append(searchReq.Fields, vectorFieldArr...)
		}
	}

	hasID := false
	for _, f := range searchReq.Fields {
		if f == mapping.IdField {
			hasID = true
		}
	}

	if !hasID {
		searchReq.Fields = append(searchReq.Fields, mapping.IdField)
	}

	queryFieldMap := make(map[string]string)
	for _, feild := range searchReq.Fields {
		queryFieldMap[feild] = feild
	}

	sortOrder, err := searchDoc.SortOrder()
	if err != nil {
		return err
	}

	if metricType == "" && space != nil && space.Index != nil {
		indexParams := &entity.IndexParams{}
		err := vjson.Unmarshal(space.Index.Params, indexParams)
		if err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal err:[%s] , space.Index.IndexParams:[%s]", err.Error(), string(space.Index.Params)))
		}
		metricType = indexParams.MetricType
	}

	if metricType != "" && metricType == "L2" {
		sortOrder = sortorder.SortOrder{&sortorder.SortScore{Desc: false}}
	}
	spaceProMap := space.SpaceProperties
	if spaceProMap == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
		spaceProMap = spacePro
	}
	sortFieldMap := make(map[string]string)

	sortFieldArr := make([]*vearchpb.SortField, 0, len(sortOrder))

	for _, sort := range sortOrder {
		sortField := sort.SortField()
		if !(sortField == "_score" || sortField == "_id" || (spaceProMap[sortField] != nil)) {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("sort field [%s] not space field", sortField))
		}

		sortFieldArr = append(sortFieldArr, &vearchpb.SortField{Field: sort.SortField(), Type: sort.GetSortOrder()})

		if sortField != "_score" && sortField != "_id" && queryFieldMap[sortField] == "" {
			searchReq.Fields = append(searchReq.Fields, sortField)
		}

		sortDesc := sort.GetSortOrder()
		if sortDesc {
			sortFieldMap[sortField] = "true"
		} else {
			sortFieldMap[sortField] = "false"
		}
	}

	searchReq.SortFields = sortFieldArr
	searchReq.SortFieldMap = sortFieldMap

	err = parseSearch(searchDoc.Vectors, searchDoc.Filters, searchReq, space)
	if err != nil {
		return err
	}

	if searchDoc.Ranker != nil && string(searchDoc.Ranker) != "" && len(searchDoc.Vectors) > 1 {
		err = parseRanker(searchDoc.Ranker, searchReq)
		if err != nil {
			return err
		}
	}

	searchReq.Head.ClientType = searchDoc.LoadBalance
	return nil
}

func ToContentMapFloatFeature(space *entity.Space, items []*vearchpb.Item) map[string][]float32 {
	nameFeatureMap := make(map[string][]float32)
	for _, u := range items {
		if u != nil {
			floatFeatureMap, _, err := GetVectorFieldValue(u.Doc, space)
			if floatFeatureMap != nil && err == nil {
				for key, value := range floatFeatureMap {
					nameFeatureMap[key] = append(nameFeatureMap[key], value...)
				}
			}
		}
	}
	return nameFeatureMap
}

func ToContentMapBinaryFeature(space *entity.Space, items []*vearchpb.Item) map[string][]int32 {
	nameFeatureMap := make(map[string][]int32)
	for _, u := range items {
		_, binaryFeatureMap, err := GetVectorFieldValue(u.Doc, space)
		if binaryFeatureMap != nil && err == nil {
			for key, value := range binaryFeatureMap {
				nameFeatureMap[key] = append(nameFeatureMap[key], value...)
			}
		}
	}
	return nameFeatureMap
}
