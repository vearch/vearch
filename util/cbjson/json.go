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
	"bytes"
	"encoding/json"
	"io"
	"time"

	"github.com/json-iterator/go"
	"github.com/json-iterator/go/extra"
)

var jsonAdapter jsoniter.API

func init() {
	extra.RegisterTimeAsInt64Codec(time.Nanosecond)

	jsonAdapter = jsoniter.Config{
		SortMapKeys:             false,
		EscapeHTML:              true,
		ValidateJsonRawMessage:  true,
		MarshalFloatWith6Digits: true,
		UseNumber:               false,
	}.Froze()
}

// Marshal marshal v into valid JSON
func Marshal(v interface{}) ([]byte, error) {
	if m, ok := v.(json.Marshaler); ok {
		return m.MarshalJSON()
	}

	return jsonAdapter.Marshal(v)
}

// MarshalIndent is like Marshal but applies Indent to format the output
func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	if m, ok := v.(json.Marshaler); ok {
		b, err := m.MarshalJSON()
		if err != nil {
			return nil, err
		}

		var buf bytes.Buffer
		if err = json.Indent(&buf, b, prefix, indent); err == nil {
			return buf.Bytes(), nil
		}
		return nil, err
	}

	return jsonAdapter.MarshalIndent(v, prefix, indent)
}

func NewEncoder(w io.Writer) *jsoniter.Encoder {
	return jsonAdapter.NewEncoder(w)
}

// Unmarshal unmarshal a JSON data to v
func Unmarshal(data []byte, v interface{}) error {
	if m, ok := v.(json.Unmarshaler); ok {
		return m.UnmarshalJSON(data)
	}

	return jsonAdapter.Unmarshal(data, v)
}

// NewDecoder create decoder read from an input stream
func NewDecoder(r io.Reader) *jsoniter.Decoder {
	return jsonAdapter.NewDecoder(r)
}
