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

const (
	ConditionOperatorIN    int32 = 1
	ConditionOperatorNOTIN int32 = 2
)

const (
	FilterOperatorAnd int32 = 0
	FilterOperatorOr  int32 = 1
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

type Term struct {
	Value    json.RawMessage
	Operator int32
}

func parseFilter(filters *request.Filter, space *entity.Space) ([]*vearchpb.RangeFilter, []*vearchpb.TermFilter, int32, error) {
	rfs := make([]*vearchpb.RangeFilter, 0)
	tfs := make([]*vearchpb.TermFilter, 0)
	var operator = FilterOperatorAnd

	var err error

	proMap := space.SpaceProperties
	if proMap == nil {
		proMap, err = entity.UnmarshalPropertyJSON(space.Fields)
		if err != nil {
			return nil, nil, operator, err
		}
	}

	if filters != nil {
		if filters.Operator == "AND" {
			operator = FilterOperatorAnd
		} else if filters.Operator == "OR" {
			operator = FilterOperatorOr
		} else {
			return nil, nil, operator, vearchpb.NewError(vearchpb.ErrorEnum_FILTER_OPERATOR_TYPE_ERR, nil)
		}
		rangeConditionMap := make(map[string][]*request.Condition)
		termConditionMap := make(map[string][]*Term)
		for _, condition := range filters.Conditions {
			if condition.Operator == "<" || condition.Operator == "<=" ||
				condition.Operator == ">" || condition.Operator == ">=" ||
				condition.Operator == "=" || condition.Operator == "<>" {
				rangeConditionMap[condition.Field] = append(rangeConditionMap[condition.Field], &condition)
			} else if condition.Operator == "IN" {
				tmp := make([]string, 0)
				err := json.Unmarshal(condition.Value, &tmp)
				if err != nil {
					log.Error(err)
					return nil, nil, operator, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, err)
				}

				tm := &Term{
					Value:    condition.Value,
					Operator: ConditionOperatorIN,
				}
				termConditionMap[condition.Field] = append(termConditionMap[condition.Field], tm)
			} else if condition.Operator == "NOT IN" {
				tmp := make([]string, 0)
				err := json.Unmarshal(condition.Value, &tmp)
				if err != nil {
					log.Error(err)
					return nil, nil, operator, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, err)
				}

				tm := &Term{
					Value:    condition.Value,
					Operator: ConditionOperatorNOTIN,
				}
				termConditionMap[condition.Field] = append(termConditionMap[condition.Field], tm)
			} else {
				return nil, nil, operator, vearchpb.NewError(vearchpb.ErrorEnum_FILTER_CONDITION_OPERATOR_TYPE_ERR, nil)
			}
		}
		filter, err := parseRange(filters.Operator, rangeConditionMap, proMap)
		if err != nil {
			return nil, nil, operator, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("parseRange err %s", err.Error()))
		}
		if len(filter) != 0 {
			rfs = append(rfs, filter...)
		}
		tmFilter, err := parseTerm(termConditionMap, proMap)
		if err != nil {
			return nil, nil, operator, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("parseTerm err %s", err.Error()))
		}
		if len(tmFilter) != 0 {
			tfs = append(tfs, tmFilter...)
		}
	}

	return rfs, tfs, operator, nil
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

	rfs, tfs, operator, err := parseFilter(filters, space)
	if err != nil {
		return err
	}
	if len(rfs) > 0 {
		req.RangeFilters = rfs
	}
	if len(tfs) > 0 {
		req.TermFilters = tfs
	}
	req.Operator = operator

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

func addRangeFilter(min any, max any, rangeFilter *vearchpb.RangeFilter, rangeFilters []*vearchpb.RangeFilter) ([]*vearchpb.RangeFilter, error) {
	minByte, err := cbbytes.ValueToByte(min)
	if err != nil {
		return nil, err
	}

	maxByte, err := cbbytes.ValueToByte(max)
	if err != nil {
		return nil, err
	}

	if minByte == nil || maxByte == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("range filter param is null or have not gte lte"))
	}

	newRangeFilter := &vearchpb.RangeFilter{
		Field:        rangeFilter.Field,
		LowerValue:   minByte,
		UpperValue:   maxByte,
		IncludeLower: rangeFilter.IncludeLower,
		IncludeUpper: rangeFilter.IncludeUpper,
		IsUnion:      rangeFilter.IsUnion,
	}

	rangeFilters = append(rangeFilters, newRangeFilter)
	return rangeFilters, nil
}

func parseRangeForOr(rangeCondition []*request.Condition, docField *entity.SpaceProperties, field string) ([]*vearchpb.RangeFilter, error) {
	var (
		leftMaxInclusive, rightMinInclusive bool  = false, false
		err                                 error = nil
	)

	rangeFilters := make([]*vearchpb.RangeFilter, 0)
	rangeFilter := &vearchpb.RangeFilter{
		Field:        field,
		IncludeLower: true,
		IncludeUpper: true,
	}

	switch docField.FieldType {
	case vearchpb.FieldType_INT:
		var leftMin, leftMax int32 = math.MinInt32, math.MinInt32
		var rightMin, rightMax int32 = math.MaxInt32, math.MaxInt32
		var curNum int32
		var equals []int32

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("INT %s Unmarshal err %s", string(rc.Value), err.Error()))
			}

			switch rc.Operator {
			case ">=":
				if curNum <= rightMin {
					rightMin = curNum
					rightMinInclusive = true
				}
			case ">":
				if curNum < rightMin {
					rightMin = curNum
					rightMinInclusive = false
				}
			case "=":
				equals = append(equals, curNum)
			case "<=":
				if curNum >= leftMax {
					leftMax = curNum
					leftMaxInclusive = true
				}
			case "<":
				if curNum > leftMax {
					leftMax = curNum
					leftMaxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		rangeFilter.IsUnion = ConditionOperatorIN
		for _, eqval := range equals {
			if eqval < leftMax || eqval > rightMin {
				continue
			} else if eqval == leftMax {
				leftMaxInclusive = true
			} else if eqval == rightMin {
				rightMinInclusive = true
			} else {
				if rangeFilters, err = addRangeFilter(eqval, eqval, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if (leftMax > rightMin) || (leftMax == rightMin && (leftMaxInclusive || rightMinInclusive)) {
			if rangeFilters, err = addRangeFilter(leftMin, rightMax, rangeFilter, rangeFilters); err != nil {
				return nil, err
			}
		} else {
			if leftMin < leftMax || leftMaxInclusive {
				rangeFilter.IncludeLower = true
				rangeFilter.IncludeUpper = leftMaxInclusive

				if rangeFilters, err = addRangeFilter(leftMin, leftMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
			if rightMin < rightMax || rightMinInclusive {
				rangeFilter.IncludeLower = rightMinInclusive
				rangeFilter.IncludeUpper = true

				if rangeFilters, err = addRangeFilter(rightMin, rightMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}
	case vearchpb.FieldType_LONG:
		var leftMin, leftMax int64 = math.MinInt64, math.MinInt64
		var rightMin, rightMax int64 = math.MaxInt64, math.MaxInt64
		var curNum int64
		var equals []int64

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("LONG %s Unmarshal err %s", string(rc.Value), err.Error()))
			}

			switch rc.Operator {
			case ">=":
				if curNum <= rightMin {
					rightMin = curNum
					rightMinInclusive = true
				}
			case ">":
				if curNum < rightMin {
					rightMin = curNum
					rightMinInclusive = false
				}
			case "=":
				equals = append(equals, curNum)
			case "<=":
				if curNum >= leftMax {
					leftMax = curNum
					leftMaxInclusive = true
				}
			case "<":
				if curNum > leftMax {
					leftMax = curNum
					leftMaxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		rangeFilter.IsUnion = ConditionOperatorIN
		for _, eqval := range equals {
			if eqval < leftMax || eqval > rightMin {
				continue
			} else if eqval == leftMax {
				leftMaxInclusive = true
			} else if eqval == rightMin {
				rightMinInclusive = true
			} else {
				if rangeFilters, err = addRangeFilter(eqval, eqval, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if (leftMax > rightMin) || (leftMax == rightMin && (leftMaxInclusive || rightMinInclusive)) {
			if rangeFilters, err = addRangeFilter(leftMin, rightMax, rangeFilter, rangeFilters); err != nil {
				return nil, err
			}
		} else {
			if leftMin < leftMax || leftMaxInclusive {
				rangeFilter.IncludeLower = true
				rangeFilter.IncludeUpper = leftMaxInclusive

				if rangeFilters, err = addRangeFilter(leftMin, leftMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
			if rightMin < rightMax || rightMinInclusive {
				rangeFilter.IncludeLower = rightMinInclusive
				rangeFilter.IncludeUpper = true

				if rangeFilters, err = addRangeFilter(rightMin, rightMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}
	case vearchpb.FieldType_FLOAT:
		var leftMin, leftMax float32 = -math.MaxFloat32, -math.MaxFloat32
		var rightMin, rightMax float32 = math.MaxFloat32, math.MaxFloat32
		var curNum float32
		var equals []float32

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("FLOAT %s Unmarshal err %s", string(rc.Value), err.Error()))
			}

			switch rc.Operator {
			case ">=":
				if curNum <= rightMin {
					rightMin = curNum
					rightMinInclusive = true
				}
			case ">":
				if curNum < rightMin {
					rightMin = curNum
					rightMinInclusive = false
				}
			case "=":
				equals = append(equals, curNum)
			case "<=":
				if curNum >= leftMax {
					leftMax = curNum
					leftMaxInclusive = true
				}
			case "<":
				if curNum > leftMax {
					leftMax = curNum
					leftMaxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		rangeFilter.IsUnion = ConditionOperatorIN
		for _, eqval := range equals {
			if eqval < leftMax || eqval > rightMin {
				continue
			} else if eqval == leftMax {
				leftMaxInclusive = true
			} else if eqval == rightMin {
				rightMinInclusive = true
			} else {
				if rangeFilters, err = addRangeFilter(eqval, eqval, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if (leftMax > rightMin) || (leftMax == rightMin && (leftMaxInclusive || rightMinInclusive)) {
			if rangeFilters, err = addRangeFilter(leftMin, rightMax, rangeFilter, rangeFilters); err != nil {
				return nil, err
			}
		} else {
			if leftMin < leftMax || leftMaxInclusive {
				rangeFilter.IncludeLower = true
				rangeFilter.IncludeUpper = leftMaxInclusive

				if rangeFilters, err = addRangeFilter(leftMin, leftMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
			if rightMin < rightMax || rightMinInclusive {
				rangeFilter.IncludeLower = rightMinInclusive
				rangeFilter.IncludeUpper = true

				if rangeFilters, err = addRangeFilter(rightMin, rightMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}
	case vearchpb.FieldType_DOUBLE:
		var leftMin, leftMax float64 = -math.MaxFloat64, -math.MaxFloat64
		var rightMin, rightMax float64 = math.MaxFloat64, math.MaxFloat64
		var curNum float64
		var equals []float64

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("FLOAT64 %s Unmarshal err %s", string(rc.Value), err.Error()))
			}

			switch rc.Operator {
			case ">=":
				if curNum <= rightMin {
					rightMin = curNum
					rightMinInclusive = true
				}
			case ">":
				if curNum < rightMin {
					rightMin = curNum
					rightMinInclusive = false
				}
			case "=":
				equals = append(equals, curNum)
			case "<=":
				if curNum >= leftMax {
					leftMax = curNum
					leftMaxInclusive = true
				}
			case "<":
				if curNum > leftMax {
					leftMax = curNum
					leftMaxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		rangeFilter.IsUnion = ConditionOperatorIN
		for _, eqval := range equals {
			if eqval < leftMax || eqval > rightMin {
				continue
			} else if eqval == leftMax {
				leftMaxInclusive = true
			} else if eqval == rightMin {
				rightMinInclusive = true
			} else {
				if rangeFilters, err = addRangeFilter(eqval, eqval, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if (leftMax > rightMin) || (leftMax == rightMin && (leftMaxInclusive || rightMinInclusive)) {
			if rangeFilters, err = addRangeFilter(leftMin, rightMax, rangeFilter, rangeFilters); err != nil {
				return nil, err
			}
		} else {
			if leftMin < leftMax || leftMaxInclusive {
				rangeFilter.IncludeLower = true
				rangeFilter.IncludeUpper = leftMaxInclusive

				if rangeFilters, err = addRangeFilter(leftMin, leftMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
			if rightMin < rightMax || rightMinInclusive {
				rangeFilter.IncludeLower = rightMinInclusive
				rangeFilter.IncludeUpper = true

				if rangeFilters, err = addRangeFilter(rightMin, rightMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}
	case vearchpb.FieldType_DATE:
		var leftMin, leftMax int64 = math.MinInt64, math.MinInt64
		var rightMin, rightMax int64 = math.MaxInt64, math.MaxInt64
		var curNum int64
		var equals []int64

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				var dateStr string
				new_err := json.Unmarshal(rc.Value, &dateStr)
				if new_err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", string(rc.Value), err.Error()))
				}
				f, err := cast.ToTimeE(dateStr)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", string(rc.Value), err.Error()))
				}
				curNum = f.UnixNano()
			} else {
				curNum = curNum * 1e9
			}

			switch rc.Operator {
			case ">=":
				if curNum <= rightMin {
					rightMin = curNum
					rightMinInclusive = true
				}
			case ">":
				if curNum < rightMin {
					rightMin = curNum
					rightMinInclusive = false
				}
			case "=":
				equals = append(equals, curNum)
			case "<=":
				if curNum >= leftMax {
					leftMax = curNum
					leftMaxInclusive = true
				}
			case "<":
				if curNum > leftMax {
					leftMax = curNum
					leftMaxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		rangeFilter.IsUnion = ConditionOperatorIN
		for _, eqval := range equals {
			if eqval < leftMax || eqval > rightMin {
				continue
			} else if eqval == leftMax {
				leftMaxInclusive = true
			} else if eqval == rightMin {
				rightMinInclusive = true
			} else {
				if rangeFilters, err = addRangeFilter(eqval, eqval, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if (leftMax > rightMin) || (leftMax == rightMin && (leftMaxInclusive || rightMinInclusive)) {
			if rangeFilters, err = addRangeFilter(leftMin, rightMax, rangeFilter, rangeFilters); err != nil {
				return nil, err
			}
		} else {
			if leftMin < leftMax || leftMaxInclusive {
				rangeFilter.IncludeLower = true
				rangeFilter.IncludeUpper = leftMaxInclusive

				if rangeFilters, err = addRangeFilter(leftMin, leftMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
			if rightMin < rightMax || rightMinInclusive {
				rangeFilter.IncludeLower = rightMinInclusive
				rangeFilter.IncludeUpper = true

				if rangeFilters, err = addRangeFilter(rightMin, rightMax, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}
	}

	return rangeFilters, nil
}

func parseRangeForAnd(rangeCondition []*request.Condition, docField *entity.SpaceProperties, field string) ([]*vearchpb.RangeFilter, error) {
	var (
		min, max                   any
		minInclusive, maxInclusive bool = true, true
		err                        error
	)

	rangeFilters := make([]*vearchpb.RangeFilter, 0)
	rangeFilter := &vearchpb.RangeFilter{
		Field:        field,
		IncludeLower: true,
		IncludeUpper: true,
	}

	switch docField.FieldType {
	case vearchpb.FieldType_INT:
		var minNum, maxNum int32 = math.MinInt32, math.MaxInt32
		var curNum int32

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("INT %s Unmarshal err %s", string(rc.Value), err.Error()))
			}
			switch rc.Operator {
			case ">=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
			case ">":
				if curNum >= minNum {
					minNum = curNum
					minInclusive = false
				}
			case "=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<=":
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<":
				if curNum <= maxNum {
					maxNum = curNum
					maxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if len(rangeFilters) != 0 && minNum == math.MinInt32 && maxNum == math.MaxInt32 && minInclusive && maxInclusive {
			return rangeFilters, err
		}
		min, max = minNum, maxNum
	case vearchpb.FieldType_LONG:
		var minNum, maxNum int64 = math.MinInt64, math.MaxInt64
		var curNum int64

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("LONG %s Unmarshal err %s", string(rc.Value), err.Error()))
			}
			switch rc.Operator {
			case ">=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
			case ">":
				if curNum >= minNum {
					minNum = curNum
					minInclusive = false
				}
			case "=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<=":
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<":
				if curNum <= maxNum {
					maxNum = curNum
					maxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if len(rangeFilters) != 0 && minNum == math.MinInt64 && maxNum == math.MaxInt64 && minInclusive && maxInclusive {
			return rangeFilters, err
		}
		min, max = minNum, maxNum
	case vearchpb.FieldType_FLOAT:
		var minNum, maxNum float32 = -math.MaxFloat32, math.MaxFloat32
		var curNum float32

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("FLOAT %s Unmarshal err %s", string(rc.Value), err.Error()))
			}
			switch rc.Operator {
			case ">=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
			case ">":
				if curNum >= minNum {
					minNum = curNum
					minInclusive = false
				}
			case "=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<=":
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<":
				if curNum <= maxNum {
					maxNum = curNum
					maxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if len(rangeFilters) != 0 && minNum == -math.MaxFloat32 && maxNum == math.MaxFloat32 && minInclusive && maxInclusive {
			return rangeFilters, err
		}
		min, max = minNum, maxNum
	case vearchpb.FieldType_DOUBLE:
		var minNum, maxNum float64 = -math.MaxFloat64, math.MaxFloat64
		var curNum float64

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("FLOAT64 %s Unmarshal err %s", string(rc.Value), err.Error()))
			}
			switch rc.Operator {
			case ">=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
			case ">":
				if curNum >= minNum {
					minNum = curNum
					minInclusive = false
				}
			case "=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<=":
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<":
				if curNum <= maxNum {
					maxNum = curNum
					maxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if len(rangeFilters) != 0 && minNum == -math.MaxFloat64 && maxNum == math.MaxFloat64 && minInclusive && maxInclusive {
			return rangeFilters, err
		}
		min, max = minNum, maxNum
	case vearchpb.FieldType_DATE:
		var minNum, maxNum int64 = math.MinInt64, math.MaxInt64
		var curNum int64

		for _, rc := range rangeCondition {
			err := vjson.Unmarshal(rc.Value, &curNum)
			if err != nil {
				var dateStr string
				new_err := json.Unmarshal(rc.Value, &dateStr)
				if new_err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", string(rc.Value), err.Error()))
				}
				f, err := cast.ToTimeE(dateStr)
				if err != nil {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("date %s Unmarshal err %s", string(rc.Value), err.Error()))
				}
				curNum = f.UnixNano()
			} else {
				curNum = curNum * 1e9
			}

			switch rc.Operator {
			case ">=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
			case ">":
				if curNum >= minNum {
					minNum = curNum
					minInclusive = false
				}
			case "=":
				if curNum > minNum {
					minNum = curNum
					minInclusive = true
				}
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<=":
				if curNum < maxNum {
					maxNum = curNum
					maxInclusive = true
				}
			case "<":
				if curNum <= maxNum {
					maxNum = curNum
					maxInclusive = false
				}
			case "<>":
				rangeFilter.IsUnion = ConditionOperatorNOTIN

				if rangeFilters, err = addRangeFilter(curNum, curNum, rangeFilter, rangeFilters); err != nil {
					return nil, err
				}
			}
		}

		if len(rangeFilters) != 0 && minNum == math.MinInt64 && maxNum == math.MaxInt64 && minInclusive && maxInclusive {
			return rangeFilters, err
		}
		min, max = minNum, maxNum
	}

	rangeFilter.IsUnion = ConditionOperatorIN
	rangeFilter.IncludeLower = minInclusive
	rangeFilter.IncludeUpper = maxInclusive

	if rangeFilters, err = addRangeFilter(min, max, rangeFilter, rangeFilters); err != nil {
		return nil, err
	}

	return rangeFilters, nil
}

func parseRange(operator string, rangeConditionMap map[string][]*request.Condition, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.RangeFilter, error) {

	rangeFilters := make([]*vearchpb.RangeFilter, 0)

	for field, rcs := range rangeConditionMap {
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

		if operator == "AND" {
			rangeFilter, err := parseRangeForAnd(rcs, docField, field)
			if err != nil {
				return nil, err
			}
			rangeFilters = append(rangeFilters, rangeFilter...)
		} else if operator == "OR" {
			rangeFilter, err := parseRangeForOr(rcs, docField, field)
			if err != nil {
				return nil, err
			}
			rangeFilters = append(rangeFilters, rangeFilter...)
		}
	}

	return rangeFilters, nil
}

func parseTerm(tm map[string][]*Term, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.TermFilter, error) {
	termFilters := make([]*vearchpb.TermFilter, 0)

	for field, rvs := range tm {
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

		for _, rv := range rvs {
			buf := bytes.Buffer{}
			var v any
			err := vjson.Unmarshal(rv.Value, &v)
			if err != nil {
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal [%s] err %s", string(rv.Value), err.Error()))
			}

			if ia, ok := v.([]any); ok {
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
				IsUnion: rv.Operator,
			}
			termFilters = append(termFilters, &termFilter)
		}
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
		queryReq.Fields = []string{entity.IdField}
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
				if property.Type != "vector" {
					queryReq.Fields = append(queryReq.Fields, fieldName)
				} else {
					vectorFieldArr = append(vectorFieldArr, fieldName)
				}
			}
			queryReq.Fields = append(queryReq.Fields, entity.IdField)
		} else {
			for _, field := range queryReq.Fields {
				if field != entity.IdField {
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
		if f == entity.IdField {
			hasID = true
		}
	}

	if !hasID {
		queryReq.Fields = append(queryReq.Fields, entity.IdField)
	}

	queryFieldMap := make(map[string]string)
	for _, field := range queryReq.Fields {
		queryFieldMap[field] = field
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
		rfs, tfs, operator, err := parseFilter(searchDoc.Filters, space)
		if err != nil {
			return err
		}
		if len(rfs) > 0 {
			queryReq.RangeFilters = rfs
		}
		if len(tfs) > 0 {
			queryReq.TermFilters = tfs
		}
		queryReq.Operator = operator
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

	if searchDoc.IndexParams != nil {
		searchReq.IndexParams = string(searchDoc.IndexParams)
	}

	searchReq.TopN = searchDoc.Limit
	if searchReq.TopN == 0 {
		searchReq.TopN = DefaultSize
	}

	if searchReq.Head.Params != nil && searchReq.Head.Params["queryOnlyId"] != "" {
		searchReq.Fields = []string{entity.IdField}
	} else {
		spaceProKeyMap := space.SpaceProperties
		if spaceProKeyMap == nil {
			spaceProKeyMap, _ = entity.UnmarshalPropertyJSON(space.Fields)
		}
		vectorFieldArr := make([]string, 0)
		if searchReq.Fields == nil || len(searchReq.Fields) == 0 {
			searchReq.Fields = make([]string, 0)
			for fieldName, property := range spaceProKeyMap {
				if property.Type != "vector" {
					searchReq.Fields = append(searchReq.Fields, fieldName)
				} else {
					vectorFieldArr = append(vectorFieldArr, fieldName)
				}
			}
			searchReq.Fields = append(searchReq.Fields, entity.IdField)
		} else {
			for _, field := range searchReq.Fields {
				if field != entity.IdField {
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
		if f == entity.IdField {
			hasID = true
		}
	}

	if !hasID {
		searchReq.Fields = append(searchReq.Fields, entity.IdField)
	}

	queryFieldMap := make(map[string]string)
	for _, field := range searchReq.Fields {
		queryFieldMap[field] = field
	}

	sortOrder, err := searchDoc.SortOrder()
	if err != nil {
		return err
	}

	metricType := ""
	indexParams := &entity.IndexParams{}

	if searchReq.IndexParams != "" {
		err := vjson.Unmarshal([]byte(searchReq.IndexParams), indexParams)
		if err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal err:[%s] , searchReq.IndexParams:[%s]", err.Error(), string(searchReq.IndexParams)))
		}
		metricType = indexParams.MetricType
	}

	if metricType == "" && space != nil && space.Index != nil && len(space.Index.Params) > 0 {
		err := vjson.Unmarshal(space.Index.Params, indexParams)
		if err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal err:[%s] , space.Index.IndexParams:[%s]", err.Error(), string(space.Index.Params)))
		}
		metricType = indexParams.MetricType
	}

	if metricType == "L2" {
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
