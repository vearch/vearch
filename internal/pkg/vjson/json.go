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

	"github.com/bytedance/sonic"
)

// Marshal marshal v into valid JSON
func Marshal(v interface{}) ([]byte, error) {
	if m, ok := v.(json.Marshaler); ok {
		return m.MarshalJSON()
	}

	return sonic.Marshal(v)
}

// Unmarshal unmarshal a JSON data to v
func Unmarshal(data []byte, v interface{}) error {
	if m, ok := v.(json.Unmarshaler); ok {
		return m.UnmarshalJSON(data)
	}

	return sonic.Unmarshal(data, v)
}
