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
)

type WriteResponse []*DocResult

func (this WriteResponse) Merge(src WriteResponse) {
	this = append(this, src...)
}

func (this *WriteResponse) ToContentBluk(nameCache NameCache, idIsLong bool) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	if len(*this) > 1 {
		builder.BeginArray()
	}
	for i, u := range *this {
		if i != 0 {
			builder.More()
		}

		names := nameCache[[2]int64{int64(u.DB), int64(u.Space)}]

		docResult := DocResultWrite{
			DbName:    names[0],
			SpaceName: names[1],
			DocResult: u,
		}
		content, err := docResult.ToContent(idIsLong)
		if err != nil {
			return nil, err
		}
		builder.ValueRaw(string(content))
	}

	if len(*this) > 1 {
		builder.EndArray()
	}

	return builder.Output()
}

func (this *WriteResponse) ToContent(dbName, spaceName string, idIsLong bool) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	if len(*this) > 1 {
		builder.BeginArray()
	}
	for i, u := range *this {
		if i != 0 {
			builder.More()
		}

		docResult := DocResultWrite{
			DbName:    dbName,
			SpaceName: spaceName,
			DocResult: u,
		}
		content, err := docResult.ToContent(idIsLong)
		if err != nil {
			return nil, err
		}
		builder.ValueRaw(string(content))
	}

	if len(*this) > 1 {
		builder.EndArray()
	}

	return builder.Output()
}
