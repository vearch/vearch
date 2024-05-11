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

package vjson

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/pkg/log"
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

func (jm JsonMap) GetJsonMap(key string) JsonMap {
	if obj, ok := jm[key]; ok {
		return JsonMap(obj.(map[string]interface{}))
	} else {
		return nil
	}
}

func (jm JsonMap) GetJsonArr(key string) JsonArr {
	if obj, ok := jm[key]; ok {
		return JsonArr(obj.([]interface{}))
	} else {
		return nil
	}
}

func (jm JsonMap) GetJsonArrMap(key string) JsonArrMap {
	jsonArrMap := JsonArrMap{}
	arr := jm.GetJsonArr(key)
	for i := range arr {
		obj := arr[i]

		jsonMap := JsonMap(obj.(map[string]interface{}))
		jsonArrMap = append(jsonArrMap, jsonMap)
	}

	return jsonArrMap
}

func (jm JsonMap) GetJsonVal(key string) JsonVal {
	if obj, ok := jm[key]; ok {
		return JsonVal(obj)
	} else {
		return nil
	}
}

func (jm JsonMap) GetJsonValIntE(key string) (int, error) {
	val := jm.GetJsonVal(key)
	if val == nil {
		return 0, fmt.Errorf("jsonMap GetJsonValIntE key: %s not found", key)
	}

	return cast.ToIntE(val)
}

func (jm JsonMap) GetJsonValInt64(key string) int64 {
	val := jm.GetJsonVal(key)
	if val == nil {
		return 0
	}

	return cast.ToInt64(val)
}

func (jm JsonMap) GetJsonValString(key string) string {
	val, _ := jm.GetJsonValStringE(key)
	return val
}

func (jm JsonMap) GetJsonValStringE(key string) (string, error) {
	val := jm.GetJsonVal(key)
	if val == nil {
		return "", fmt.Errorf("jsonMap GetJsonValString key: %s not found", key)
	}

	return cast.ToStringE(val)
}

func (jm JsonMap) GetJsonValStringOrDefault(key, def string) string {
	val := jm.GetJsonVal(key)
	if val == nil {
		return def
	}

	return cast.ToString(val)
}

func (jm JsonMap) GetJsonValBool(key string) bool {
	val, _ := jm.GetJsonValBoolE(key)
	return val
}

func (jm JsonMap) GetJsonValBoolE(key string) (bool, error) {
	val := jm.GetJsonVal(key)
	if val == nil {
		return false, fmt.Errorf("jsonMap GetJsonValBoolE key: %s not found", key)
	}

	return cast.ToBoolE(val)
}

func (jm JsonMap) GetJsonValBytes(key string) ([]byte, error) {
	val := jm.GetJsonVal(key)
	if val == nil {
		return []byte{}, fmt.Errorf("jsonMap GetJsonValBytes key: %s not found", key)
	}

	data, err := Marshal(val)
	if err != nil {
		return []byte{}, fmt.Errorf("jsonMap GetJsonValBytes key: %s to bytes failed, err: %v", key, err)
	}

	return data, nil
}
