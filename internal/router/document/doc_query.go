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
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/entity/request"
	"github.com/vearch/vearch/internal/pkg/cbbytes"
	"github.com/vearch/vearch/internal/pkg/cbjson"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/ps/engine/mapping"
	"github.com/vearch/vearch/internal/ps/engine/sortorder"
	"github.com/vearch/vearch/internal/router/document/rutil"
)

const (
	URLQueryFrom            = "from"
	URLQuerySize            = "size"
	UrlQueryRouting         = "routing"
	UrlQueryTypedKey        = "typed_keys"
	UrlQueryVersion         = "version"
	UrlQueryRetryOnConflict = "retry_on_conflict"
	UrlQueryOpType          = "op_type"
	UrlQueryURISort         = "sort"
	UrlQueryTimeout         = "timeout"
	LoadBalance             = "load_balance"
	DefaultSize             = 50
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

func parseQuery(data []byte, req *vearchpb.SearchRequest, space *entity.Space) error {
	if len(data) == 0 {
		return nil
	}

	temp := struct {
		And            []json.RawMessage `json:"and"`
		Sum            []json.RawMessage `json:"sum"`
		Vector         []json.RawMessage `json:"vector"`
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
	} else if len(temp.Vector) > 0 {
		req.MultiVectorRank = 1
		if reqNum, vqs, err = parseVectors(reqNum, vqs, temp.Vector, space); err != nil {
			return err
		}
	}

	proMap := space.SpaceProperties
	if proMap == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
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
				if len(filter) != 0 {
					rfs = append(rfs, filter...)
				}
			}
		} else if termBytes, ok := tmp["term"]; ok {
			if termBytes != nil {
				filter, err := parseTerm(termBytes, proMap)
				if err != nil {
					return err
				}
				if len(filter) != 0 {
					tfs = append(tfs, filter...)
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

func parseQueryForIdFeature(searchQuery []byte, space *entity.Space, items []*vearchpb.Item, query_type string) ([]byte, error) {
	var binary bool
	var featureBinaryMap map[string][]int32
	var featureFloat32Map map[string][]float32
	if space.Index.IndexType == "BINARYIVF" {
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
			return nil, fmt.Errorf("unmarshal err:[%s] , query:[%s]", err.Error(), searchQuery)
		}
		vector_value, vector_exists := queryMap["vector"].([]interface{})
		if queryMap["sum"] == nil && queryMap["and"] == nil && (!vector_exists || len(vector_value) == 0) {
			if binary {
				feature, err := MakeQueryFeature(nil, featureBinaryMap, query_type)
				if err != nil {
					return nil, fmt.Errorf("assembly feature err:[%s] ", err.Error())
				}
				return feature, nil
			} else {
				feature, err := MakeQueryFeature(featureFloat32Map, nil, query_type)
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

			if queryMap["vector"] != nil {
				value := queryMap["vector"]
				vectorArray := value.([]interface{})
				for _, val := range vectorArray {
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
			feature, err := MakeQueryFeature(nil, featureBinaryMap, query_type)
			if err != nil {
				return nil, fmt.Errorf("assembly feature err:[%s] ", err.Error())
			}
			return feature, nil
		} else {
			feature, err := MakeQueryFeature(featureFloat32Map, nil, query_type)
			if err != nil {
				return nil, fmt.Errorf("assembly feature err:[%s] ", err.Error())
			}
			return feature, nil
		}
	}
}

func parseVectors(reqNum int, vqs []*vearchpb.VectorQuery, tmpArr []json.RawMessage, space *entity.Space) (int, []*vearchpb.VectorQuery, error) {
	var err error
	indexType := space.Index.IndexType
	proMap := space.SpaceProperties
	if proMap == nil {
		spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
		proMap = spacePro
	}
	for i := 0; i < len(tmpArr); i++ {
		vqTemp := &VectorQuery{}
		if err = json.Unmarshal(tmpArr[i], vqTemp); err != nil {
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
		if strings.Compare(indexType, "BINARYIVF") == 0 {
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

		if strings.Compare(indexType, "BINARYIVF") != 0 {
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

func parseRange(data []byte, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.RangeFilter, error) {
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

	rangeFilters := make([]*vearchpb.RangeFilter, 0)

	for field, rv = range tmp {
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
					if err != nil {
						return nil, err
					}
					minNum = int32(vInt32)
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
					if err != nil {
						return nil, err
					}
					maxNum = int32(vInt32)
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
			var minNum, maxNum float32

			if start != nil {
				if f, e := start.(json.Number).Float64(); e != nil {
					return nil, e
				} else {
					minNum = float32(f)
				}
			} else {
				minNum = -math.MaxFloat32
			}

			if end != nil {
				if f, e := end.(json.Number).Float64(); e != nil {
					return nil, e
				} else {
					maxNum = float32(f)
				}
			} else {
				maxNum = math.MaxFloat32
			}

			min, max = minNum, maxNum
		case entity.FieldType_DOUBLE:
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
		rangeFilters = append(rangeFilters, &rangeFilter)
	}

	return rangeFilters, nil
}

func parseTerm(data []byte, proMap map[string]*entity.SpaceProperties) ([]*vearchpb.TermFilter, error) {
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

	termFilters := make([]*vearchpb.TermFilter, 0)

	for field, rv := range tmp {
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
	if strings.Compare(indexType, "BINARYIVF") == 0 {
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

func searchUrlParamParse(searchReq *vearchpb.SearchRequest) {
	urlParamMap := searchReq.Head.Params
	if urlParamMap[URLQuerySize] != "" {
		size := cast.ToInt(urlParamMap[URLQuerySize])
		searchReq.TopN = int32(size)
	} else {
		if searchReq.TopN == 0 {
			searchReq.TopN = DefaultSize
		}
	}
	searchReq.Head.ClientType = urlParamMap[LoadBalance]
}

func searchParamToSearchPb(searchDoc *request.SearchDocumentRequest, searchReq *vearchpb.SearchRequest, space *entity.Space, idFeature bool) error {
	hasRank := true
	if searchDoc.Quick {
		hasRank = false
	}
	searchReq.HasRank = hasRank
	searchReq.IsVectorValue = searchDoc.VectorValue
	searchReq.ParallelBasedOnQuery = searchDoc.Parallel
	searchReq.L2Sqrt = searchDoc.L2Sqrt
	searchReq.IvfFlat = searchDoc.IVFFlat
	searchReq.IndexParams = string(searchDoc.IndexParams)
	searchReq.Fields = searchDoc.Fields
	searchReq.IsBruteSearch = searchDoc.IsBruteSearch

	metricType := ""
	if searchDoc.IndexParams != nil {
		temp := struct {
			MetricType string `json:"metric_type,omitempty"`
			Nprobe     int64  `json:"nprobe,omitempty"`
		}{}

		err := cbjson.Unmarshal(searchDoc.IndexParams, &temp)
		if err != nil {
			return fmt.Errorf("unmarshal err:[%s] , query:[%s]", err.Error(), string(searchDoc.IndexParams))
		}
		metricType = temp.MetricType
		if temp.Nprobe == 0 && searchDoc.Nprobe != 0 {
			jsonMap := make(map[string]interface{})
			err := json.Unmarshal(searchDoc.IndexParams, &jsonMap)
			if err != nil {
				return fmt.Errorf("query param RetrievalParam parse err")
			}
			jsonMap["nprobe"] = searchDoc.Nprobe
			retrievalByte, err := json.Marshal(jsonMap)
			if err != nil {
				return fmt.Errorf("query param RetrievalParam parse err")
			}
			searchReq.IndexParams = string(retrievalByte)
		}
	} else {
		if searchDoc.Nprobe != 0 {
			IndexParams := map[string]int{"nprobe": int(searchDoc.Nprobe)}

			jsonByte, err := sonic.Marshal(IndexParams)
			if err != nil {
				return fmt.Errorf("query param RetrievalParam parse err")
			}
			searchReq.IndexParams = string(jsonByte)
		}
	}
	if searchDoc.Size != nil {
		searchReq.TopN = int32(*searchDoc.Size)
	}

	if searchReq.Head.Params != nil && searchReq.Head.Params["queryOnlyId"] != "" {
		searchReq.Fields = []string{mapping.IdField}
	} else {
		spaceProKeyMap := space.SpaceProperties
		if spaceProKeyMap == nil {
			spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
			spaceProKeyMap = spacePro
		}
		vectorFieldArr := make([]string, 0)
		if searchReq.Fields == nil || len(searchReq.Fields) == 0 {
			searchReq.Fields = make([]string, 0)
			spaceProKeyMap := space.SpaceProperties
			if spaceProKeyMap == nil {
				spacePro, _ := entity.UnmarshalPropertyJSON(space.Fields)
				spaceProKeyMap = spacePro
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
		err := cbjson.Unmarshal(space.Index.IndexParams, indexParams)
		if err != nil {
			return fmt.Errorf("unmarshal err:[%s] , space.Index.IndexParams:[%s]", err.Error(), string(space.Index.IndexParams))
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

	order := "desc"
	if len(sortOrder) > 0 {
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

	searchReq.Head.Params["load_balance"] = searchDoc.LoadBalance

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
