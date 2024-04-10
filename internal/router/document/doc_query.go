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
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/entity/request"
	"github.com/vearch/vearch/internal/pkg/cbbytes"
	"github.com/vearch/vearch/internal/pkg/vjson"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/ps/engine/mapping"
	"github.com/vearch/vearch/internal/ps/engine/sortorder"
)

const (
	URLQueryFrom     = "from"
	UrlQueryRouting  = "routing"
	UrlQueryTypedKey = "typed_keys"
	UrlQueryVersion  = "version"
	UrlQueryOpType   = "op_type"
	UrlQueryTimeout  = "timeout"
	DefaultSize      = 50
)

type VectorQuery struct {
	Field        string          `json:"field"`
	FeatureData  json.RawMessage `json:"feature"`
	Feature      []float32       `json:"-"`
	FeatureUint8 []uint8         `json:"-"`
	Symbol       string          `json:"symbol"`
	Value        *float64        `json:"value"`
	Boost        *float64        `json:"boost"`
	Format       *string         `json:"format,omitempty"`
	MinScore     *float64        `json:"min_score,omitempty"`
	MaxScore     *float64        `json:"max_score,omitempty"`
	IndexType    string          `json:"index_type"`
	HasBoost     *int32          `json:"has_boost"`
}

var defaultBoost = float64(1)
var defaultHasBoost = int32(0)

type Range struct {
	Gt  json.RawMessage
	Gte json.RawMessage
	Lt  json.RawMessage
	Lte json.RawMessage
}

type Term struct {
	Value json.RawMessage
}

func parseQuery(vectors []json.RawMessage, filters *request.Filter, req *vearchpb.SearchRequest, space *entity.Space) error {
	vqs := make([]*vearchpb.VectorQuery, 0)
	rfs := make([]*vearchpb.RangeFilter, 0)
	tfs := make([]*vearchpb.TermFilter, 0)

	var err error
	var reqNum int

	if len(vectors) > 0 {
		req.MultiVectorRank = 1
		if reqNum, vqs, err = parseVectors(reqNum, vqs, vectors, space); err != nil {
			return err
		}
	}

	proMap := space.SpaceProperties
	if proMap == nil {
		proMap, _ = entity.UnmarshalPropertyJSON(space.Fields)
	}

	if filters != nil {
		if filters.Operator != "AND" {
			return fmt.Errorf("operator %v not supported", filters.Operator)
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
				tm, ok := termConditionMap[condition.Field]
				if !ok {
					tm = &Term{
						Value: condition.Value,
					}
					termConditionMap[condition.Field] = tm
				} else {
					tm.Value = condition.Value
				}
			}
		}
		filter, err := parseRange(rangeConditionMap, proMap)
		if err != nil {
			return fmt.Errorf("%v parseRange err %s", rangeConditionMap, err.Error())
		}
		if len(filter) != 0 {
			rfs = append(rfs, filter...)
		}
		tmFilter, err := parseTerm(termConditionMap, proMap)
		if err != nil {
			return fmt.Errorf("%v parseTerm err %s", termConditionMap, err.Error())
		}
		if len(tmFilter) != 0 {
			tfs = append(tfs, tmFilter...)
		}
	}

	if len(vqs) > 0 {
		req.VecFields = vqs
	}

	if len(tfs) > 0 {
		req.TermFilters = tfs
	}

	if len(rfs) > 0 {
		req.RangeFilters = rfs
	}

	if reqNum <= 0 {
		reqNum = 1
	}

	req.ReqNum = int32(reqNum)
	return nil
}

func unmarshalArray[T any](data []byte, dimension int) ([]T, error) {
	if len(data) < dimension {
		return nil, fmt.Errorf("vector query length err, need feature num:[%d]", dimension)
	}

	var result []T
	if err := vjson.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	if len(result) > 0 {
		if _, ok := any(result).([]float32); ok && (len(result)%dimension) != 0 {
			return nil, fmt.Errorf("vector query length err, not equals dimension multiple:[%d]", (len(result) % dimension))
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
			return reqNum, vqs, fmt.Errorf("query has err for field:[%s] not found in space fields", vqTemp.Field)
		}

		if docField.FieldType != entity.FieldType_VECTOR {
			return reqNum, vqs, fmt.Errorf("query has err for field:[%s] is not vector type", vqTemp.Field)
		}

		if vqTemp.FeatureData == nil || len(vqTemp.FeatureData) == 0 {
			return reqNum, vqs, fmt.Errorf("query has err for feature is null")
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
			return reqNum, vqs, fmt.Errorf("query has err for field:[%s] dimension size mapping:[%d] query:[%d]", vqTemp.Field, len(vqTemp.Feature), d)
		}

		if reqNum == 0 {
			reqNum = queryNum
		} else if reqNum != queryNum {
			return reqNum, vqs, fmt.Errorf("query has err for field:[%s] not same queryNum mapping:[%d] query:[%d] ", vqTemp.Field, len(vqTemp.Feature), d)
		}

		if indexType != "BINARYIVF" {
			if vqTemp.Format != nil && len(*vqTemp.Format) > 0 {
				switch *vqTemp.Format {
				case "normalization", "normal":
				case "no":
				default:
					return reqNum, vqs, fmt.Errorf("unknow vector process format:[%s]", *vqTemp.Format)
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
			return nil, fmt.Errorf("field:[%s] not found in space fields", field)
		}

		if docField.FieldType == entity.FieldType_STRING {
			return nil, fmt.Errorf("range filter should be numberic type, field:[%s] is string which should be term filter", field)
		}

		if docField.Option&entity.FieldOption_Index != entity.FieldOption_Index {
			return nil, fmt.Errorf("field:[%s] not set index, please check space", field)
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
		case entity.FieldType_INT:
			var minNum, maxNum int32

			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					return nil, fmt.Errorf("INT %s Unmarshal err %s", string(start), err.Error())
				}
			} else {
				minNum = math.MinInt32
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					return nil, fmt.Errorf("INT %s Unmarshal err %s", string(end), err.Error())
				}
			} else {
				maxNum = math.MaxInt32
			}

			min, max = minNum, maxNum
		case entity.FieldType_LONG:
			var minNum, maxNum int64

			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					return nil, fmt.Errorf("LONG %s Unmarshal err %s", string(start), err.Error())
				}
			} else {
				minNum = math.MinInt64
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					return nil, fmt.Errorf("LONG %s Unmarshal err %s", string(end), err.Error())
				}
			} else {
				maxNum = math.MaxInt64
			}

			min, max = minNum, maxNum
		case entity.FieldType_FLOAT:
			var minNum, maxNum float32

			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					return nil, fmt.Errorf("FLOAT %s Unmarshal err %s", string(start), err.Error())
				}
			} else {
				minNum = -math.MaxFloat32
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					return nil, fmt.Errorf("FLOAT %s Unmarshal err %s", string(end), err.Error())
				}
			} else {
				maxNum = math.MaxFloat32
			}

			min, max = minNum, maxNum
		case entity.FieldType_DOUBLE:
			var minNum, maxNum float64

			if start != nil {
				err := vjson.Unmarshal(start, &minNum)
				if err != nil {
					return nil, fmt.Errorf("FLOAT64 %s Unmarshal err %s", string(start), err.Error())
				}
			} else {
				minNum = -math.MaxFloat64
			}

			if end != nil {
				err := vjson.Unmarshal(end, &maxNum)
				if err != nil {
					return nil, fmt.Errorf("FLOAT64 %s Unmarshal err %s", string(end), err.Error())
				}
			} else {
				maxNum = math.MaxFloat64
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
			return nil, fmt.Errorf("range param is null or have not gte lte")
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
	var isUnion int32
	isUnion = 1

	termFilters := make([]*vearchpb.TermFilter, 0)

	for field, rv := range tm {
		fd := proMap[field]

		if fd == nil {
			return nil, fmt.Errorf("field:[%s] not found in space fields", field)
		}

		if fd.FieldType != entity.FieldType_STRING {
			return nil, fmt.Errorf("term filter should be string type, field:[%s] is numberic type which should be range filter", field)
		}

		if fd.Option&entity.FieldOption_Index != entity.FieldOption_Index {
			return nil, fmt.Errorf("field:[%s] not set index, please check space", field)
		}

		buf := bytes.Buffer{}
		var v interface{}
		err := vjson.Unmarshal(rv.Value, &v)
		if err != nil {
			return nil, fmt.Errorf("unmarshal [%s] err %s", string(rv.Value), err.Error())
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
			return nil, fmt.Errorf("symbol value unknow:[%s]", query.Symbol)
		}
	}

	if query.Boost == nil {
		query.Boost = &defaultBoost
	}

	if query.HasBoost == nil {
		query.HasBoost = &defaultHasBoost
	}

	vectorQuery := &vearchpb.VectorQuery{
		Name:      query.Field,
		Value:     codeByte,
		MinScore:  *query.MinScore,
		MaxScore:  *query.MaxScore,
		Boost:     *query.Boost,
		HasBoost:  *query.HasBoost,
		IndexType: indexType,
	}
	return vectorQuery, nil
}

func requestToPb(searchDoc *request.SearchDocumentRequest, space *entity.Space, searchReq *vearchpb.SearchRequest) error {
	hasRank := true
	if searchDoc.Quick {
		hasRank = false
	}
	searchReq.HasRank = hasRank
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
						return fmt.Errorf("query param fields are not exist in the table")
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
			return fmt.Errorf("unmarshal err:[%s] , space.Index.IndexParams:[%s]", err.Error(), string(space.Index.Params))
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
			return fmt.Errorf("query param sort field not space field")
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

	err = parseQuery(searchDoc.Vectors, searchDoc.Filters, searchReq, space)
	if err != nil {
		return err
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
