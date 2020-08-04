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
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/util/cbjson"
)

type BulkResponse struct {
	Took  uint64              `json:"took"`
	Items []*BulkItemResponse `json:"items"`
}

func (sr *BulkResponse) ToContent(took int64, idIsLong bool) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("took")
	builder.ValueNumeric(int64(took) / 1e6)

	hasError := false

	builder.More()
	builder.Field("items")
	builder.BeginArray()
	for i, item := range sr.Items {
		if item.HasError() {
			hasError = true
		}

		if i != 0 {
			builder.More()
		}
		content, err := item.ToContent(idIsLong)
		if err != nil {
			return nil, err
		}
		builder.ValueRaw(string(content))
	}
	builder.EndArray()

	builder.More()
	builder.Field("errors")
	builder.ValueBool(hasError)

	builder.EndObject()

	return builder.Output()
}

type BulkItemResponse struct {
	OpType    pspb.OpType     `json:"-"`
	ItemValue *DocResultWrite `json:"-"`
}

func (this *BulkItemResponse) ToContent(idIsLong bool) ([]byte, error) {
	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()
	builder.Field(this.KeyName())
	content, err := this.ItemValue.ToContent(idIsLong)
	if err != nil {
		return nil, err
	}
	builder.ValueRaw(string(content))
	builder.EndObject()

	return builder.Output()
}

func (this *BulkItemResponse) HasError() bool {
	if this.ItemValue == nil {
		return false
	}
	if this.ItemValue.DocResult == nil {
		return false
	}
	if this.ItemValue.DocResult.Failure == nil {
		return false
	}
	return true
}

func (this *BulkItemResponse) KeyName() string {
	switch this.OpType {
	case pspb.OpType_DELETE:
		return "delete"
	case pspb.OpType_MERGE:
		return "update"
	case pspb.OpType_REPLACE:
		return "index"
	case pspb.OpType_CREATE:
		return "create"
	default:
		return "unknow"
	}
}
