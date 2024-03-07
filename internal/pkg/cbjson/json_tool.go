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

package cbjson

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/internal/pkg/log"
)

type JsonMap map[string]interface{}

type JsonArrMap []JsonMap
type JsonArr []interface{}
type JsonVal interface{}

func ToJsonString(obj interface{}) string {
	bytes, err := json.Marshal(obj)
	if err != nil {
		log.Error(err.Error())
	}
	return string(bytes)
}

func ByteToJsonMap(bytes []byte) (JsonMap, error) {
	maps := make(map[string]interface{})
	if err := Unmarshal(bytes, &maps); err != nil {
		return nil, err
	}
	return maps, nil
}

func (this JsonMap) GetJsonMap(key string) JsonMap {
	if obj, ok := this[key]; ok {
		return JsonMap(obj.(map[string]interface{}))
	} else {
		return nil
	}
}

func (this JsonMap) GetJsonArr(key string) JsonArr {
	if obj, ok := this[key]; ok {
		return JsonArr(obj.([]interface{}))
	} else {
		return nil
	}
}

func (this JsonMap) GetJsonArrMap(key string) JsonArrMap {
	jsonArrMap := JsonArrMap{}
	arr := this.GetJsonArr(key)
	for i := range arr {
		obj := arr[i]

		jsonMap := JsonMap(obj.(map[string]interface{}))
		jsonArrMap = append(jsonArrMap, jsonMap)
	}

	return jsonArrMap
}

func (this JsonMap) GetJsonVal(key string) JsonVal {
	if obj, ok := this[key]; ok {
		return JsonVal(obj)
	} else {
		return nil
	}
}

func (this JsonMap) GetJsonValIntE(key string) (int, error) {
	val := this.GetJsonVal(key)
	if val == nil {
		return 0, fmt.Errorf("JsonMap GetJsonValIntE key: %s not found.", key)
	}

	return cast.ToIntE(val)
}

func (this JsonMap) GetJsonValInt64(key string) int64 {
	val := this.GetJsonVal(key)
	if val == nil {
		return 0
	}

	return cast.ToInt64(val)
}

func (this JsonMap) GetJsonValString(key string) string {
	val, _ := this.GetJsonValStringE(key)
	return val
}

func (this JsonMap) GetJsonValStringE(key string) (string, error) {
	val := this.GetJsonVal(key)
	if val == nil {
		return "", fmt.Errorf("JsonMap GetJsonValString key: %s not found.", key)
	}

	return cast.ToStringE(val)
}

func (this JsonMap) GetJsonValStringOrDefault(key, def string) string {
	val := this.GetJsonVal(key)
	if val == nil {
		return def
	}

	return cast.ToString(val)
}

func (this JsonMap) GetJsonValBool(key string) bool {
	val, _ := this.GetJsonValBoolE(key)
	return val
}

func (this JsonMap) GetJsonValBoolE(key string) (bool, error) {
	val := this.GetJsonVal(key)
	if val == nil {
		return false, fmt.Errorf("JsonMap GetJsonValBoolE key: %s not found.", key)
	}

	return cast.ToBoolE(val)
}

func (this JsonMap) GetJsonValBytes(key string) ([]byte, error) {
	val := this.GetJsonVal(key)
	if val == nil {
		return []byte{}, fmt.Errorf("JsonMap GetJsonValBytes key: %s not found.", key)
	}

	data, err := Marshal(val)
	if err != nil {
		return []byte{}, fmt.Errorf("JsonMap GetJsonValBytes key: %s to bytes failed, err: %v", key, err)
	}

	return data, nil
}
