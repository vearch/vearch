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
	bytes2 "bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/cbjson"
	"math"
	"strings"
	"time"
)

type queryBuilder struct {
	mapping *mapping.IndexMapping
}

type VectorQuery struct {
	Field       string          `json:"field"`
	FeatureData json.RawMessage `json:"feature"`
	Feature     []float32       `json:"-"`
	Symbol      string          `json:"symbol"`
	Value       *float64        `json:"value"`
	Boost       *float64        `json:"boost"`
	Format      *string         `json:"format,omitempty"`
	MinScore    *float64        `json:"min_score,omitempty"`
	MaxScore    *float64        `json:"max_score,omitempty"`
}

var defaultBoost = util.PFloat64(1)

var minOffset float64 = 0.0000001

func (query *VectorQuery) ToC() (*C.struct_VectorQuery, error) {

	code, err := cbbytes.FloatArrayByte(query.Feature)
	if err != nil {
		return nil, err
	}

	if query.MinScore == nil {
		query.MinScore = util.PFloat64(0)
	}
	if query.MaxScore == nil {
		query.MaxScore = util.PFloat64(math.MaxFloat32)
	}

	if query.Value != nil {

		switch strings.TrimSpace(query.Symbol) {
		case ">":
			query.MinScore = util.PFloat64(*query.Value + minOffset)
		case ">=":
			query.MinScore = util.PFloat64(*query.Value)
		case "<":
			query.MaxScore = util.PFloat64(*query.Value - minOffset)
		case "<=":
			query.MaxScore = util.PFloat64(*query.Value)
		default:
			return nil, fmt.Errorf("symbol value unknow:[%s]", query.Symbol)
		}
	}

	if query.Boost == nil {
		query.Boost = defaultBoost
	}

	return C.MakeVectorQuery(byteArrayStr(query.Field), byteArray(code), C.double(*query.MinScore), C.double(*query.MaxScore), C.double(*query.Boost), C.int(1)), nil

}

func (qb *queryBuilder) parseTerm(data []byte) (*C.struct_TermFilter, error) {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return nil, err
	}

	isUnion := 1

	if operator, found := tmp["operator"]; found {
		op := strings.ToLower(cast.ToString(operator))
		switch op {
		case "and":
			isUnion = 0
		case "or":
			isUnion = 1
		default:
			return nil, fmt.Errorf("err term filter by operator:[%s]", operator)
		}

		delete(tmp, "operator")
	}

	for field, rv := range tmp {

		fd := qb.mapping.GetField(field)

		if fd == nil {
			return nil, fmt.Errorf("field:[%d] not found in mapping", field)
		}

		if fd.Options()&pspb.FieldOption_Index != pspb.FieldOption_Index {
			return nil, fmt.Errorf("field:[%d] not open index", field)
		}

		buf := bytes2.Buffer{}
		if ia, ok := rv.([]interface{}); ok {
			for i, obj := range ia {
				buf.WriteString(cast.ToString(obj))
				if i != len(ia)-1 {
					buf.WriteRune('\001')
				}
			}
		} else {
			buf.WriteString(cast.ToString(rv))
		}
		return C.MakeTermFilter(byteArrayStr(field), byteArrayStr(buf.String()), C.char(isUnion)), nil
	}

	return nil, nil

}

func (qb *queryBuilder) parseRange(data []byte) (*C.struct_RangeFilter, error) {

	tmp := make(map[string]map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return nil, err
	}

	var (
		field                      string
		min, max                   interface{}
		rv                         map[string]interface{}
		minInclusive, maxInclusive bool
	)

	for field, rv = range tmp {

		if qb.mapping.GetField(field).Options()&pspb.FieldOption_Index != pspb.FieldOption_Index {
			return nil, fmt.Errorf("field:[%d] not open index", field)
		}

		var found bool

		var start, end interface{}

		if start, found = rv["from"]; !found {
			if start, found = rv["gt"]; !found {
				if start, found = rv["gte"]; found {
					minInclusive = true
				}
			} else {
				minInclusive = false
			}
		} else {
			if rv["include_lower"] == nil || !cast.ToBool(rv["include_lower"]) {
				minInclusive = false
			} else {
				minInclusive = true
			}
		}

		if end, found = rv["to"]; !found {
			if end, found = rv["lt"]; !found {
				if end, found = rv["lte"]; found {
					maxInclusive = true
				}
			} else {
				maxInclusive = false
			}
		} else {
			if rv["include_upper"] == nil || !cast.ToBool(rv["include_upper"]) {
				maxInclusive = false
			} else {
				maxInclusive = true
			}
		}

		docField := qb.mapping.GetField(field)

		if docField == nil {
			return nil, fmt.Errorf("can not define field:[%s]", field)
		}

		switch docField.FieldType() {
		case pspb.FieldType_INT:
			var minNum, maxNum int64

			if start != nil {
				if f, e := cast.ToInt64E(start); e != nil {
					return nil, e
				} else {
					minNum = f
				}
			} else {
				minNum = math.MinInt64
			}

			if end != nil {
				if f, e := cast.ToInt64E(end); e != nil {
					return nil, e
				} else {
					maxNum = f
				}
			} else {
				maxNum = math.MinInt64
			}

			min, max = minNum, maxNum

		case pspb.FieldType_FLOAT:
			var minNum, maxNum float64

			if start != nil {
				if f, e := cast.ToFloat64E(start); e != nil {
					return nil, e
				} else {
					minNum = f
				}
			} else {
				minNum = -math.MaxFloat64
			}

			if end != nil {
				if f, e := cast.ToFloat64E(end); e != nil {
					return nil, e
				} else {
					maxNum = f
				}
			} else {
				maxNum = math.MaxFloat64
			}

			min, max = minNum, maxNum

		case pspb.FieldType_DATE:

			//TODO ANSJ we need a interface to date util
			var minDate, maxDate time.Time

			if start != nil {
				if f, e := cast.ToInt64E(start); e != nil {
					if minDate, e = cast.ToTimeE(start); e != nil {
						return nil, e
					}
				} else {
					minDate = time.Unix(0, f*1e6)
				}
			}

			if end != nil {
				if f, e := cast.ToInt64E(end); e != nil {
					if maxDate, e = cast.ToTimeE(end); e != nil {
						return nil, e
					}
				} else {
					maxDate = time.Unix(0, f*1e6)
				}
			} else {
				maxDate = time.Unix(math.MaxInt64, 0)
			}

			min, max = minDate.UnixNano(), maxDate.UnixNano()

		}

		var minC int8 = 0
		if minInclusive {
			minC = 1
		}
		var maxC int8 = 0
		if maxInclusive {
			maxC = 1
		}

		var minByte, maxByte []byte

		minByte, err = cbbytes.ValueToByte(min)
		if err != nil {
			return nil, err
		}

		maxByte, err = cbbytes.ValueToByte(max)
		if err != nil {
			return nil, err
		}

		return C.MakeRangeFilter(byteArrayStr(field), byteArray(minByte), byteArray(maxByte), C.char(minC), C.char(maxC)), nil
	}

	return nil, nil

}

func (qb *queryBuilder) parseQuery(data []byte, req *C.struct_Request) error {

	if len(data) == 0 {
		return nil
	}

	temp := struct {
		And              []json.RawMessage `json:"and"`
		Sum              []json.RawMessage `json:"sum"`
		Filter           []json.RawMessage `json:"filter"`
		DirectSearchType int               `json:"direct_search_type"`
		OnlineLogLevel   string            `json:"online_log_level"`
	}{}

	err := cbjson.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("unmarshal err:[%s] , query:[%s]", err.Error(), string(data))
	}

	vqs := make([]*C.struct_VectorQuery, 0)
	rfs := make([]*C.struct_RangeFilter, 0)
	tfs := make([]*C.struct_TermFilter, 0)

	var reqNum int

	if len(temp.And) > 0 {
		if reqNum, vqs, err = qb.parseVectors(reqNum, vqs, temp.And); err != nil {
			return err
		}
	} else if len(temp.Sum) > 0 {
		req.multi_vector_rank = C.int(1)
		if reqNum, vqs, err = qb.parseVectors(reqNum, vqs, temp.Sum); err != nil {
			return err
		}
	}

	for _, filterBytes := range temp.Filter {
		tmp := make(map[string]json.RawMessage)
		err := cbjson.Unmarshal(filterBytes, &tmp)
		if err != nil {
			return err
		}
		if filterBytes, ok := tmp["range"]; ok {
			filter, err := qb.parseRange(filterBytes)
			if err != nil {
				return err
			}
			rfs = append(rfs, filter)
		} else if termBytes, ok := tmp["term"]; ok {
			filter, err := qb.parseTerm(termBytes)
			if err != nil {
				return err
			}
			tfs = append(tfs, filter)
		}
	}

	if len(vqs) > 0 {
		cvqs := C.MakeVectorQuerys(C.int(len(vqs)))
		for i, q := range vqs {
			C.SetVectorQuery(cvqs, C.int(i), q)
		}
		req.vec_fields = cvqs
		req.vec_fields_num = C.int(len(vqs))
	} else {
		req.vec_fields_num = C.int(0)
	}

	if len(tfs) > 0 {
		ctfs := C.MakeTermFilters(C.int(len(tfs)))
		for i, q := range tfs {
			C.SetTermFilter(ctfs, C.int(i), q)
		}
		req.term_filters = ctfs
		req.term_filters_num = C.int(len(tfs))
	} else {
		req.term_filters_num = C.int(0)
	}

	if len(rfs) > 0 {
		crfs := C.MakeRangeFilters(C.int(len(rfs)))
		for i, q := range rfs {
			C.SetRangeFilter(crfs, C.int(i), q)
		}
		req.range_filters = crfs
		req.range_filters_num = C.int(len(rfs))
	} else {
		req.range_filters_num = C.int(0)
	}

	if reqNum <= 0 {
		reqNum = 1
	}

	req.req_num = C.int(reqNum)

	if temp.DirectSearchType != 0 {
		req.direct_search_type = C.int(temp.DirectSearchType)
	}

	if temp.OnlineLogLevel != "" {
		req.online_log_level = byteArrayStr(temp.OnlineLogLevel)
	}

	return nil
}

func (qb *queryBuilder) parseVectors(reqNum int, vqs []*C.struct_VectorQuery, tmpArr []json.RawMessage) (int, []*C.struct_VectorQuery, error) {
	var err error

	for i := 0; i < len(tmpArr); i++ {
		vqTemp := &VectorQuery{}
		if err = json.Unmarshal(tmpArr[i], vqTemp); err != nil {
			return reqNum, vqs, err
		}

		docField := qb.mapping.GetField(vqTemp.Field)

		if docField == nil || docField.FieldType() != pspb.FieldType_VECTOR {
			return reqNum, vqs, fmt.Errorf("query has err for field:[%s] is not vector type", vqTemp.Field)
		}

		if vqTemp.Feature, err = rowDateToFloatArray(vqTemp.FeatureData, docField.FieldMappingI.(*mapping.VectortFieldMapping).Dimension); err != nil {
			return reqNum, vqs, err
		}

		queryNum := len(vqTemp.Feature) / docField.FieldMappingI.(*mapping.VectortFieldMapping).Dimension
		validate := len(vqTemp.Feature) % docField.FieldMappingI.(*mapping.VectortFieldMapping).Dimension

		if queryNum == 0 || validate != 0 {
			return reqNum, vqs, fmt.Errorf("query has err for field:[%s] dimension size mapping:[%d] query:[%d]", docField.Name, len(vqTemp.Feature), docField.FieldMappingI.(*mapping.VectortFieldMapping).Dimension)
		}

		if reqNum == 0 {
			reqNum = queryNum
		} else if reqNum != queryNum {
			return reqNum, vqs, fmt.Errorf("query has err for field:[%s] not same queryNum mapping:[%d] query:[%d] ", docField.Name, len(vqTemp.Feature), docField.FieldMappingI.(*mapping.VectortFieldMapping).Dimension)
		}

		if vqTemp.Format != nil && len(*vqTemp.Format) > 0 {
			switch *vqTemp.Format {
			case "normalization", "normal":
				if err := util.Normalization(vqTemp.Feature); err != nil {
					return reqNum, vqs, err
				}
			default:
				return reqNum, vqs, fmt.Errorf("unknow vector process format:[%s]", vqTemp.Format)
			}
		}

		vq, err := vqTemp.ToC()
		if err != nil {
			return reqNum, vqs, err
		}

		vqs = append(vqs, vq)
	}

	return reqNum, vqs, nil
}
