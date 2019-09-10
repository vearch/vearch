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

package request

import (
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/util/cbjson"
)

func NewObjRequest(reqCtx *RequestContext, pid entity.PartitionID, value interface{}) (*ObjRequest, error) {
	var (
		bytes []byte
		e     error
	)
	if code, ok := value.(pspb.Codec); ok {
		bytes, e = code.Marshal()
	} else {
		bytes, e = cbjson.Marshal(value)
	}

	if e != nil {
		return nil, e
	}
	return &ObjRequest{
		RequestContext: reqCtx,
		Value:          bytes,
		PartitionID:    pid,
	}, nil
}

type ObjRequest struct {
	*RequestContext
	PartitionID entity.PartitionID `json:"partition,omitempty"`
	Value       []byte             `json:"value,omitempty"`
}

func (req *ObjRequest) Context() *RequestContext {
	return req.RequestContext
}

func (req *ObjRequest) GetPartitionID() uint32 {
	return req.PartitionID
}

func (req *ObjRequest) SetPartitionID(pid entity.PartitionID) {
	req.PartitionID = pid
}

func (req *ObjRequest) CloneValue(pid entity.PartitionID, value interface{}) (Request, error) {
	bytes, e := cbjson.Marshal(value)
	if e != nil {
		return nil, e
	}
	return &ObjRequest{
		RequestContext: req.RequestContext,
		Value:          bytes,
		PartitionID:    pid,
	}, nil
}

func (req *ObjRequest) Clone(pid entity.PartitionID) Request {
	return &ObjRequest{
		RequestContext: req.RequestContext,
		Value:          req.Value,
		PartitionID:    pid,
	}
}

//this function will decoding or encoding value
func (req *ObjRequest) Decode(i interface{}) error {

	if code, ok := i.(pspb.Codec); ok {
		return code.Unmarshal(req.Value)
	}

	return cbjson.Unmarshal(req.Value, i)
}
