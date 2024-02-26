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

package sortorder

import (
	"encoding/json"
	"errors"
	"reflect"
)

var defaultSort = SortOrder{&SortScore{Desc: true}}

func ParseSort(bytes []byte) (SortOrder, error) {
	if len(bytes) == 0 {
		return defaultSort, nil
	}
	arr := make([]interface{}, 0, 3)
	if err := json.Unmarshal(bytes, &arr); err != nil {
		return nil, err
	} else {
		return parseSortInterface(arr)
	}
}

func parseSort(s interface{}) (Sort, error) {
	val := reflect.ValueOf(s)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.String:
		if val.String() == "_score" {
			return &SortScore{Desc: true}, nil
		} else if val.String() == "_id" {
			return &SortField{Field: "_id", Desc: false}, nil
		} else {
			return &SortField{Field: val.String(), Desc: true}, nil
		}

	case reflect.Map:
		if typ.Key().Kind() == reflect.String {
			for _, key := range val.MapKeys() {
				fieldName := key.String()
				sortVal := val.MapIndex(key).Interface()
				sVal := reflect.ValueOf(sortVal)
				switch sVal.Type().Kind() {
				case reflect.String:
					if sVal.String() == "desc" {
						return &SortField{Field: fieldName, Desc: true}, nil
					} else if sVal.String() == "asc" {
						return &SortField{Field: fieldName, Desc: false}, nil
					} else {
						return nil, errors.New("invalid sort")
					}
				case reflect.Map:
					var sort SortField
					sort.Field = fieldName
					for _, subKey := range sVal.MapKeys() {
						switch subKey.String() {
						case "order":
							order, ok := sVal.MapIndex(subKey).Interface().(string)
							if !ok {
								return nil, errors.New("invalid sort")
							}
							if order == "desc" {
								sort.Desc = true
							} else if order == "asc" {
								sort.Desc = false
							}
						case "mode":
							mode, ok := sVal.MapIndex(subKey).Interface().(string)
							if !ok {
								return nil, errors.New("invalid sort")
							}
							if mode == "min" {
								sort.Mode = SortFieldMin
							} else if mode == "max" {
								sort.Mode = SortFieldMax
							} else {
								// fixme not support avg/sum
							}
						case "missing":
							missing, ok := sVal.MapIndex(subKey).Interface().(string)
							if !ok {
								return nil, errors.New("invalid sort")
							}
							if missing == "_last" {
								sort.Missing = SortFieldMissingLast
							} else if missing == "_first" {
								sort.Missing = SortFieldMissingFirst
							} else {
								return nil, errors.New("invalid sort")
							}
						case "unmapped_type":
							// todo
						}
					}
					return &sort, nil
				}
			}
		}
	case reflect.Ptr:
		ptrElem := val.Elem()
		if ptrElem.IsValid() && ptrElem.CanInterface() {
			return parseSort(ptrElem.Interface())
		}
	default:
		return nil, errors.New("invalid sort type " + typ.Kind().String())
	}
	return nil, errors.New("invalid sort")
}

func parseSortInterface(s interface{}) (SortOrder, error) {
	if s == nil {
		return nil, nil
	}
	var sortOrder SortOrder
	val := reflect.ValueOf(s)
	typ := val.Type()

	switch typ.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			if val.Index(i).CanInterface() {
				sortVal := val.Index(i).Interface()
				sort, err := parseSort(sortVal)
				if err != nil {
					return nil, err
				}
				sortOrder = append(sortOrder, sort)
			}
		}
	case reflect.String:
		sort, err := parseSort(s)
		if err != nil {
			return nil, err
		}
		sortOrder = append(sortOrder, sort)
	default:
		sort, err := parseSort(val)
		if err != nil {
			return nil, err
		}
		sortOrder = append(sortOrder, sort)
	}
	return sortOrder, nil
}
