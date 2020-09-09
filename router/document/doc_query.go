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
	"github.com/spf13/cast"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/ps/engine/sortorder"
	"github.com/vearch/vearch/router/document/rutil"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/cbjson"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	URLQueryFrom            = "from"
	URLQuerySize            = "size"
	UrlQueryRouting         = "routing"
	UrlQueryTypedKey        = "typed_keys"
	UrlQueryVersion         = "version"
	UrlQueryRetryOnConflict = "retry_on_conflict"
	UrlQueryOpType          = "op_type"
	UrlQueryRefresh         = "refresh"
	UrlQueryURISort         = "sort"
	UrlQueryTimeout         = "timeout"
	ClientTypeValue         = "client_type"
	DefaultSzie             = 50
	URLQueryRefresh         = "refresh"
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
}

var defaultBoost = util.PFloat64(1)

var minOffset float64 = 0.0000001

func parseQuery(data []byte, req *vearchpb.SearchRequest, space *entity.Space) error {

	if len(data) == 0 {
		return nil
	}

	temp := struct {
		And            []json.RawMessage `json:"and"`
		Sum            []json.RawMessage `json:"sum"`
		Filter         []json.RawMessage `json:"filter"`
		OnlineLogLevel string            `json:"online_log_level"`
	}{}

	err := cbjson.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("unmarshal err:[%s] , query:[%s]", err.Error(), string(data))
	}

	vqs := make([]*vearchpb.VectorQuery, 0)
	rfs := make([]*vearchpb.RangeFilter, 0)
	tfs := make([]*vearchpb.TermFilter, 0)

	var reqNum int

	if len(temp.And) > 0 {
		if reqNum, vqs, err = parseVectors(reqNum, vqs, temp.And, space); err != nil {
			return err
		}
	} else if len(temp.Sum) > 0 {
		req.MultiVectorRank = 1
		if reqNum, vqs, err = parseVectors(reqNum, vqs, temp.Sum, space); err != nil {
			return err
		}
	}

	proMap := space.SpaceProperties
	if proMap == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		proMap = spacePro
	}
	for _, filterBytes := range temp.Filter {
		tmp := make(map[string]json.RawMessage)
		err := cbjson.Unmarshal(filterBytes, &tmp)
		if err != nil {
			return err
		}
		if filterBytes, ok := tmp["range"]; ok {
			if filterBytes != nil {
				filter, err := parseRange(filterBytes, proMap)
				if err != nil {
					return err
				}
				if filter != nil {
					rfs = append(rfs, filter)
				}
			}
		} else if termBytes, ok := tmp["term"]; ok {
			if termBytes != nil {
				filter, err := parseTerm(termBytes, proMap)
				if err != nil {
					return err
				}
				if filter != nil {
					tfs = append(tfs, filter)
				}
			}
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
	req.OnlineLogLevel = temp.OnlineLogLevel
	return nil
}

func parseQueryForIdFeature(searchQuery []byte, space *entity.Space, items []*vearchpb.Item) ([]byte, error) {
	var binary bool
	var featureBinaryMap map[string][]int32
	var featureFloat32Map map[string][]float32
	if space.Engine.RetrievalType == "BINARYIVF" {
		featureBinaryMap = ToContentMapBinaryFeature(space, items)
		binary = true
	} else {
		featureFloat32Map = ToContentMapFloatFeature(space, items)
		binary = false
	}
	if searchQuery != nil {
		queryMap := map[string]interface{}{}
		err := json.Unmarshal(searchQuery, &queryMap)

		if err != nil {
			return nil, fmt.Errorf("unmarshal err:[%s] , query:[%s]", err.Error())
		}

		if queryMap["sum"] == nil && queryMap["and"] == nil {
			if binary {
				feature, err := MakeQueryFeature(nil, featureBinaryMap)
				if err != nil {
					return nil, fmt.Errorf("assembly feature err:[%s] ", err.Error())
				}
				return feature, nil
			} else {
				feature, err := MakeQueryFeature(featureFloat32Map, nil)
				if err != nil {
					return nil, fmt.Errorf("assembly feature err:[%s] ", err.Error())
				}
				return feature, nil
			}
		} else {
			if queryMap["and"] != nil {
				value := queryMap["and"]
				andArray := value.([]interface{})
				for _, val := range andArray {
					fMap := val.(map[string]interface{})
					if fMap["field"] != nil {
						field := fMap["field"].(string)
						if binary {
							fMap["feature"] = featureBinaryMap[field]
						} else {
							fMap["feature"] = featureFloat32Map[field]
						}
					} else {
						return nil, fmt.Errorf("query param no feature field")
					}
				}
			}

			if queryMap["sum"] != nil {
				value := queryMap["sum"]
				sumArray := value.([]interface{})
				for _, val := range sumArray {
					fMap := val.(map[string]interface{})
					if fMap["field"] != nil {
						field := fMap["field"].(string)
						if binary {
							fMap["feature"] = featureBinaryMap[field]
						} else {
							fMap["feature"] = featureFloat32Map[field]
						}
					} else {
						return nil, fmt.Errorf("query param no feature field")
					}
				}
			}
			queryJson, _ := json.MarshalIndent(queryMap, "", "  ")
			return queryJson, nil
		}
	} else {
		if binary {
			feature, err := MakeQueryFeature(nil, featureBinaryMap)
			if err != nil {
				return nil, fmt.Errorf("assembly feature err:[%s] ", err.Error())
			}
			return feature, nil
		} else {
			feature, err := MakeQueryFeature(featureFloat32Map, nil)
			if err != nil {
				return nil, fmt.Errorf("assembly feature err:[%s] ", err.Error())
			}
			return feature, nil
		}
	}
}

func parseVectors(reqNum int, vqs []*vearchpb.VectorQuery, tmpArr []json.RawMessage, space *entity.Space) (int, []*vearchpb.VectorQuery, error) {
	var err error
	retrievalType := space.Engine.RetrievalType
	proMap := space.SpaceProperties
	if proMap == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
		proMap = spacePro
	}
	for i := 0; i < len(tmpArr); i++ {
		vqTemp := &VectorQuery{}
		if err = json.Unmarshal(tmpArr[i], vqTemp); err != nil {
			return reqNum, vqs, err
		}

		docField := proMap[vqTemp.Field]

		if docField == nil || docField.FieldType != entity.FieldType_VECTOR {
			return reqNum, vqs, fmt.Errorf("query has err for field:[%s] is not vector type", vqTemp.Field)
		}

		if vqTemp.FeatureData == nil || len(vqTemp.FeatureData) == 0 {
			return reqNum, vqs, fmt.Errorf("query has err for feature is null")
		}

		d := docField.Dimension
		queryNum := 0
		validate := 0
		if strings.Compare(retrievalType, "BINARYIVF") == 0 {
			if vqTemp.FeatureUint8, err = rutil.RowDateToUInt8Array(vqTemp.FeatureData, d/8); err != nil {
				return reqNum, vqs, err
			}
			queryNum = len(vqTemp.FeatureUint8) / (d / 8)
			validate = len(vqTemp.FeatureUint8) % (d / 8)
		} else {
			if vqTemp.Feature, err = rutil.RowDateToFloatArray(vqTemp.FeatureData, d); err != nil {
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

		if strings.Compare(retrievalType, "BINARYIVF") != 0 {
			if vqTemp.Format != nil && len(*vqTemp.Format) > 0 {
				switch *vqTemp.Format {
				case "normalization", "normal":
				case "no":
				default:
					return reqNum, vqs, fmt.Errorf("unknow vector process format:[%s]", vqTemp.Format)
				}
			}
		}

		vq, err := vqTemp.ToC(retrievalType)
		if err != nil {
			return reqNum, vqs, err
		}
		vqs = append(vqs, vq)
	}
	return reqNum, vqs, nil
}

func parseRange(data []byte, proMap map[string]*entity.SpaceProperties) (*vearchpb.RangeFilter, error) {

	tmp := make(map[string]map[string]interface{})
	d := json.NewDecoder(bytes.NewBuffer(data))
	d.UseNumber()
	err := d.Decode(&tmp)
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

		docField := proMap[field]

		if docField == nil {
			return nil, fmt.Errorf("can not define field:[%s]", field)
		}

		if docField.Option&entity.FieldOption_Index != entity.FieldOption_Index {
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

		switch docField.FieldType {
		case entity.FieldType_INT:
			var minNum, maxNum int32

			if start != nil {
				v := start.(json.Number).String()
				if v != "" {
					vInt32, err := strconv.ParseInt(v, 10, 32)
					if err == nil {
						minNum = int32(vInt32)
					} else {
						return nil, err
					}
				} else {
					minNum = math.MinInt32
				}
			} else {
				minNum = math.MinInt32
			}

			if end != nil {
				v := end.(json.Number).String()
				if v != "" {
					vInt32, err := strconv.ParseInt(v, 10, 32)
					if err == nil {
						maxNum = int32(vInt32)
					} else {
						return nil, err
					}
				} else {
					maxNum = math.MaxInt32
				}
			} else {
				maxNum = math.MaxInt32
			}

			min, max = minNum, maxNum

		case entity.FieldType_LONG:
			var minNum, maxNum int64

			if start != nil {
				if f, e := start.(json.Number).Int64(); e != nil {
					return nil, e
				} else {
					minNum = f
				}
			} else {
				minNum = math.MinInt64
			}

			if end != nil {
				if f, e := end.(json.Number).Int64(); e != nil {
					return nil, e
				} else {
					maxNum = f
				}
			} else {
				maxNum = math.MaxInt64
			}

			min, max = minNum, maxNum

		case entity.FieldType_FLOAT:
			var minNum, maxNum float64

			if start != nil {
				if f, e := start.(json.Number).Float64(); e != nil {
					return nil, e
				} else {
					minNum = f
				}
			} else {
				minNum = -math.MaxFloat64
			}

			if end != nil {
				if f, e := end.(json.Number).Float64(); e != nil {
					return nil, e
				} else {
					maxNum = f
				}
			} else {
				maxNum = math.MaxFloat64
			}

			min, max = minNum, maxNum

		case entity.FieldType_DATE:

			var minDate, maxDate time.Time

			if start != nil {
				if f, e := start.(json.Number).Int64(); e != nil {
					if minDate, e = cast.ToTimeE(start); e != nil {
						return nil, e
					}
				} else {
					minDate = time.Unix(0, f*1e6)
				}
			}

			if end != nil {
				if f, e := end.(json.Number).Int64(); e != nil {
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

		var minByte, maxByte []byte

		minByte, err = cbbytes.ValueToByte(min)
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
		return &rangeFilter, nil
	}

	return nil, nil

}

func parseTerm(data []byte, proMap map[string]*entity.SpaceProperties) (*vearchpb.TermFilter, error) {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return nil, err
	}

	var isUnion int32
	isUnion = 1

	if operator, found := tmp["operator"]; found {
		op := strings.ToLower(cast.ToString(operator))
		switch op {
		case "and":
			isUnion = 0
		case "or":
			isUnion = 1
		case "not":
			isUnion = 2
		default:
			return nil, fmt.Errorf("err term filter by operator:[%s]", operator)
		}

		delete(tmp, "operator")
	}

	for field, rv := range tmp {

		fd := proMap[field]

		if fd == nil {
			return nil, fmt.Errorf("field:[%d] not found in mapping", field)
		}

		if fd.Option&entity.FieldOption_Index != entity.FieldOption_Index {
			return nil, fmt.Errorf("field:[%d] not open index", field)
		}

		buf := bytes.Buffer{}
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

		rangeFilter := vearchpb.TermFilter{
			Field:   field,
			Value:   buf.Bytes(),
			IsUnion: isUnion,
		}
		return &rangeFilter, nil
	}

	return nil, nil

}

func (query *VectorQuery) ToC(retrievalType string) (*vearchpb.VectorQuery, error) {
	var codeByte []byte
	if strings.Compare(retrievalType, "BINARYIVF") == 0 {
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
		query.MinScore = util.PFloat64(-1)
	}
	if query.MaxScore == nil {
		query.MaxScore = util.PFloat64(-1)
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

	vectorQuery := &vearchpb.VectorQuery{
		Name:     query.Field,
		Value:    codeByte,
		MinScore: *query.MinScore,
		MaxScore: *query.MaxScore,
		Boost:    *query.Boost,
		HasBoost: 0,
	}
	return vectorQuery, nil
}

func searchUrlParamParse(searchReq *vearchpb.SearchRequest) {
	urlParamMap := searchReq.Head.Params
	if urlParamMap[URLQuerySize] != "" {
		size := cast.ToInt(urlParamMap[URLQuerySize])
		searchReq.TopN = int32(size)
	} else {
		if searchReq.TopN == 0 {
			searchReq.TopN = DefaultSzie
		}
	}
	clientType := urlParamMap[ClientTypeValue]
	searchReq.Head.ClientType = clientType

}

func searchParamToSearchPb(searchDoc *request.SearchDocumentRequest, searchReq *vearchpb.SearchRequest, space *entity.Space, idFeature bool) error {
	searchReq.HasRank = searchDoc.Quick
	searchReq.IsVectorValue = searchDoc.VectorValue
	searchReq.ParallelBasedOnQuery = searchDoc.Parallel
	searchReq.L2Sqrt = searchDoc.L2Sqrt
	searchReq.IvfFlat = searchDoc.IVFFlat
	searchReq.RetrievalParams = string(searchDoc.RetrievalParam)
	searchReq.Fields = searchDoc.Fields
	searchReq.IsBruteSearch = searchDoc.IsBruteSearch

	metricType := ""
	if searchDoc.RetrievalParam != nil {
		temp := struct {
			MetricType string `json:"metric_type,omitempty"`
		}{}

		err := cbjson.Unmarshal(searchDoc.RetrievalParam, &temp)
		if err != nil {
			return fmt.Errorf("unmarshal err:[%s] , query:[%s]", err.Error(), string(searchDoc.RetrievalParam))
		}
		metricType = temp.MetricType
	}
	if searchDoc.Size != nil {
		searchReq.TopN = int32(*searchDoc.Size)
	}

	if searchReq.Head.Params != nil && searchReq.Head.Params["queryOnlyId"] != "" {
		searchReq.Fields = []string{mapping.IdField}
	} else {
		if len(searchReq.Fields) == 0 && searchDoc.VectorValue {
			searchReq.Fields = make([]string, 0)
			spaceProKeyMap := space.SpaceProperties
			if spaceProKeyMap == nil {
				spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
				spaceProKeyMap = spacePro
			}
			for k, _ := range spaceProKeyMap {
				searchReq.Fields = append(searchDoc.Fields, k)
			}
			searchReq.Fields = append(searchReq.Fields, mapping.IdField)
		}
	}

	if searchReq.Fields == nil || len(searchReq.Fields) == 0 {
		searchReq.Fields = append(searchReq.Fields, mapping.IdField)
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
	queryFields := searchReq.Fields
	if queryFields != nil {
		for _, feild := range queryFields {
			queryFieldMap[feild] = feild
		}
	}

	sortOrder, err := searchDoc.SortOrder()
	if err != nil {
		return err
	}

	if metricType != "" && metricType == "L2" {
		sortOrder = sortorder.SortOrder{&sortorder.SortScore{Desc: false}}
	}
	spaceProMap := space.SpaceProperties
	if spaceProMap == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Properties)
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
		if sortField != "_score" && sortField != "_id" {
			sortDesc := sort.GetSortOrder()
			if sortDesc {
				sortFieldMap[sortField] = "true"
			} else {
				sortFieldMap[sortField] = "false"
			}
		}
	}

	searchReq.SortFields = sortFieldArr
	searchReq.SortFieldMap = sortFieldMap

	order := "desc"
	if sortOrder != nil && len(sortOrder) > 0 {
		sortBool := sortOrder[0].GetSortOrder()
		if !sortBool {
			order = "asc"
		}
	}

	if searchReq.Head.Params == nil {
		paramMap := make(map[string]string)
		paramMap["sort"] = order
		searchReq.Head.Params = paramMap
	} else {
		searchReq.Head.Params["sort"] = order
	}

	if !idFeature {
		parseErr := parseQuery(searchDoc.Query, searchReq, space)
		if parseErr != nil {
			return parseErr
		}
	}

	searchUrlParamParse(searchReq)
	return nil
}

func ToContentMapFloatFeature(space *entity.Space, items []*vearchpb.Item) map[string][]float32 {
	nameFeatureMap := make(map[string][]float32)
	for _, u := range items {
		floatFeatureMap, _, err := GetVectorFieldValue(u.Doc, space)
		if floatFeatureMap != nil && err == nil {
			for key, value := range floatFeatureMap {
				nameFeatureMap[key] = append(nameFeatureMap[key], value...)
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
