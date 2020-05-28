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

import (
	"github.com/vearch/vearch/util/cbjson"
	"strconv"
)

func NewErrDocResults(ids []string, err error) []*DocResult {
	result := make(DocResults, len(ids))
	for i, id := range ids {
		result[i] = NewErrDocResult(id, err)
	}
	return result
}

type DocResults []*DocResult

func (this *DocResults) ToContent(dbName, spaceName string, idIsLong bool) ([]byte, error) {

	var builder = cbjson.ContentBuilderFactory()

	builder.BeginArray()

	for i, u := range *this {
		if i != 0 {
			builder.More()
		}
		builder.BeginObject()

		builder.Field("_index")
		builder.ValueString(dbName)

		builder.More()
		builder.Field("_type")
		builder.ValueString(spaceName)

		builder.More()
		builder.Field("_id")
		if idIsLong {
			idInt64, err := strconv.ParseInt(u.Id, 10, 64)
			if err == nil {
				builder.ValueNumeric(idInt64)
			}
		} else {
			builder.ValueString(u.Id)
		}

		builder.More()
		builder.Field("found")
		builder.ValueBool(u.Found)

		if u.Found {
			/*builder.More()
			builder.Field("_version")
			builder.ValueNumeric(u.Version)*/

			builder.More()
			builder.Field("_source")
			builder.ValueInterface(u.Source)
		}

		builder.EndObject()
	}

	builder.EndArray()

	return builder.Output()
}
