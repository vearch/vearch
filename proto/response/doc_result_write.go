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
	"fmt"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/util/cbjson"
	"net/http"
	"strconv"
)

type DocResultWrite struct {
	DocResult  *DocResult
	DbName     string
	SpaceName  string
	ReplicaNum int
}

func (this *DocResultWrite) ToContent(idIsLong bool) ([]byte, error) {
	if this.DocResult == nil {
		return nil, fmt.Errorf("DocResultWrite ToContent error: doc result not found.")
	}

	var builder = cbjson.ContentBuilderFactory()

	builder.BeginObject()

	builder.Field("_index")
	builder.ValueString(this.DbName)

	builder.More()
	builder.Field("_type")
	builder.ValueString(this.SpaceName)

	builder.More()
	builder.Field("_id")
	if idIsLong {
		idInt64, err := strconv.ParseInt(this.DocResult.Id, 10, 64)
		if err == nil {
			builder.ValueNumeric(idInt64)
		}
	} else {
		builder.ValueString(this.DocResult.Id)
	}

	if this.DocResult.Failure != nil {
		builder.More()
		builder.Field("status")
		builder.ValueNumeric(this.DocResult.Failure.Status)

		builder.More()
		builder.Field("error")
		builder.ValueInterface(WriteError{Shard: cast.ToString(this.ReplicaNum), ErrorType: this.DocResult.Failure.Type, ErrorReason: this.DocResult.Failure.Reason})
	} else {
		builder.More()
		builder.Field("status")
		builder.ValueNumeric(cast.ToInt64(this.SuccessStatus()))

		/*builder.More()
		builder.Field("_version")
		builder.ValueNumeric(this.DocResult.Version)*/

		builder.More()
		builder.Field("_shards")

		if this.DocResult.Failure == nil {
			okShards := Shards{Total: this.ReplicaNum, Successful: 1}
			builder.ValueInterface(okShards)
		} else {
			failShards := Shards{Total: this.ReplicaNum, Successful: 1}
			builder.ValueInterface(failShards)
		}

		builder.More()
		builder.Field("result")
		builder.ValueString(this.WriteResult())

		builder.More()
		builder.Field("_seq_no")
		builder.ValueUNumeric(1)

		builder.More()
		builder.Field("_primary_term")
		builder.ValueUNumeric(1)
	}

	builder.EndObject()

	return builder.Output()
}

func (doc *DocResultWrite) WriteResult() string {
	switch doc.DocResult.Type {
	case pspb.OpType_DELETE:
		return "deleted"
	case pspb.OpType_MERGE:
		return "updated"
	case pspb.OpType_REPLACE:
		if doc.DocResult.Replace {
			return "updated"
		}
		return "created"
	case pspb.OpType_CREATE:
		return "created"
	default:
		return "unknow"
	}
}

func (doc *DocResultWrite) SuccessStatus() int {
	switch doc.DocResult.Type {
	case pspb.OpType_DELETE:
		return http.StatusOK
	case pspb.OpType_MERGE:
		return http.StatusOK
	case pspb.OpType_REPLACE:
		if doc.DocResult.Replace {
			return http.StatusOK
		}
		return http.StatusCreated
	case pspb.OpType_CREATE:
		return http.StatusCreated
	default:
		return http.StatusOK
	}
}

type WriteError struct {
	ErrorIndex     string `json:"index"`
	ErrorIndexUUID string `json:"index_uuid"`
	Shard          string `json:"shard"`
	ErrorType      string `json:"type"`
	ErrorReason    string `json:"reason"`
}
