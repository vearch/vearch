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

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/gamma"
	"github.com/vearch/vearch/ps/engine/mapping"
)

// Reader is the read interface to an engine's data.
type Reader interface {
	GetDoc(ctx context.Context, doc *vearchpb.Document) error

	ReadSN(ctx context.Context) (int64, error)

	DocCount(ctx context.Context) (uint64, error)

	Capacity(ctx context.Context) (int64, error)

	Search(ctx context.Context, request *vearchpb.SearchRequest, resp *vearchpb.SearchResponse) error
}

// Writer is the write interface to an engine's data.
type Writer interface {
	// use do by single cmd , support create update replace or delete
	Write(ctx context.Context, docCmd *vearchpb.DocCmd) error

	//this update will merge documents
	// Update(ctx context.Context, docCmd *vearchpb.DocCmd) error

	// flush memory to segment, new reader will read the newest data
	Flush(ctx context.Context, sn int64) error

	// commit is renew a memory block, return a chan to client, client get the chan to wait the old memory flush to segment
	Commit(ctx context.Context, sn int64) (chan error, error)
}

type EngineStatus struct {
	IndexStatus   int32
	DocNum        int32
	MaxDocid      int32
	MinIndexedNum int32
}

// Engine is the interface that wraps the core operations of a document store.
type Engine interface {
	Reader() Reader
	Writer() Writer
	//return three value, field to vearchpb.Field , new Schema info , error
	NewSnapshot() (proto.Snapshot, error)
	ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error
	Optimize() error
	IndexInfo() (int, int)
	EngineStatus(status *EngineStatus) error
	Close()
	HasClosed() bool

	UpdateMapping(space *entity.Space) error
	GetMapping() *mapping.IndexMapping

	GetSpace() *entity.Space
	GetPartitionID() entity.PartitionID

	SetEngineCfg(config *gamma.Config) error
	GetEngineCfg(config *gamma.Config) error
}
