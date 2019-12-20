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
	"context"
	"github.com/vearch/vearch/proto/entity"
)

type SearchRequest struct {
	*RequestContext
	*SearchDocumentRequest
	PartitionID entity.PartitionID `json:"partition,omitempty"`
}

func (req *SearchRequest) Context() *RequestContext {
	return req.RequestContext
}

func (req *SearchRequest) GetPartitionID() uint32 {
	return req.PartitionID
}

func (req *SearchRequest) SetPartitionID(pid entity.PartitionID) {
	req.PartitionID = pid
}

func (req *SearchRequest) Clone(pid entity.PartitionID, db, space string) *SearchRequest {
	return &SearchRequest{
		RequestContext:        req.RequestContext,
		SearchDocumentRequest: req.SearchDocumentRequest,
		PartitionID:           pid,
	}
}

func NewSearchRequest(ctx context.Context, msgId string) *SearchRequest {
	return &SearchRequest{
		RequestContext: &RequestContext{
			MessageId: msgId,
			ctx:       ctx,
		},
		SearchDocumentRequest: &SearchDocumentRequest{},
	}
}
