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

package response

import "github.com/vearch/vearch/util/cbjson"

type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

func (sr *Shards) ToContent() ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field("_shards")
	builder.ValueInterface(sr)
	builder.EndObject()

	return builder.Output()
}
