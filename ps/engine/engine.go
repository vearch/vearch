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

package engine

import (
	"context"
	"github.com/tiglabs/raft/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/proto/pspb"
)


// Reader is the read interface to an engine's data.
type Reader interface {
	GetDoc(ctx context.Context, docID string) *response.DocResult

	GetDocs(ctx context.Context, docIDs []string) []*response.DocResult

	Search(ctx context.Context, req *request.SearchRequest) *response.SearchResponse

	MSearch(ctx context.Context, request *request.SearchRequest) response.SearchResponses

	//you can use ctx to cancel the stream , when this function returned will close resultChan
	StreamSearch(ctx context.Context, req *request.SearchRequest, resultChan chan *response.DocResult) error

	ReadSN(ctx context.Context) (int64, error)

	DocCount(ctx context.Context) (uint64, error)

	Capacity(ctx context.Context) (int64, error)
}

// RTReader is a real time reader, only used by ps or engine for update document
type RTReader interface {
	RTReadDoc(ctx context.Context, docID string) *response.DocResult
}

// Writer is the write interface to an engine's data.
type Writer interface {
	// use do by single cmd , support create update replace or delete
	Write(ctx context.Context, docCmd *pspb.DocCmd) *response.DocResult

	// if you not set id , suggest use it to create
	Create(ctx context.Context, docCmd *pspb.DocCmd) *response.DocResult

	//this update will merge documents
	Update(ctx context.Context, docCmd *pspb.DocCmd) *response.DocResult

	//delete documents
	Delete(ctx context.Context, docCmd *pspb.DocCmd) *response.DocResult

	// flush memory to segment, new reader will read the newest data
	Flush(ctx context.Context, sn int64) error

	// commit is renew a memory block, return a chan to client, client get the chan to wait the old memory flush to segment
	Commit(ctx context.Context, sn int64) (chan error, error)
}

// Engine is the interface that wraps the core operations of a document store.
type Engine interface {
	Reader() Reader
	RTReader() RTReader
	Writer() Writer
	//return three value, field to pspb.Field , new Schema info , error
	MapDocument(doc *pspb.DocCmd) ([]*pspb.Field, map[string]pspb.FieldType, error)
	NewSnapshot() (proto.Snapshot, error)
	ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error
	Optimize() error
	Close()

	UpdateMapping(space *entity.Space) error
	GetMapping() *mapping.IndexMapping

	GetSpace() *entity.Space
	GetPartitionID() entity.PartitionID
}
