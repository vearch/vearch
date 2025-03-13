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
	"fmt"

	"github.com/spf13/cast"
)

type JsonMap map[string]any

func ByteToJsonMap(bytes []byte) (JsonMap, error) {
	maps := make(map[string]any)
	if err := Unmarshal(bytes, &maps); err != nil {
		return nil, err
	}
	return maps, nil
}

func (jm JsonMap) GetJsonVal(key string) any {
	if obj, ok := jm[key]; ok {
		return any(obj)
	} else {
		return nil
	}
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
