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

package raftstore

import (
	"context"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
)

func (s *Store) GetDocument(ctx context.Context, readLeader bool, docID string) (*response.DocResult, error) {
	if err := s.checkReadable(readLeader); err != nil {
		return nil, err
	}
	return s.Engine.Reader().GetDoc(ctx, docID), nil
}

func (s *Store) GetRTDocument(ctx context.Context, readLeader bool, docID string) (*response.DocResult, error) {
	if err := s.checkReadable(readLeader); err != nil {
		return nil, err
	}
	return s.Engine.RTReader().RTReadDoc(ctx, docID), nil
}

func (s *Store) GetDocuments(ctx context.Context, readLeader bool, docIds []string) (response.DocResults, error) {
	if err := s.checkReadable(readLeader); err != nil {
		return nil, err
	}
	return s.Engine.Reader().GetDocs(ctx, docIds), nil
}

func (s *Store) Search(ctx context.Context, readLeader bool, query *request.SearchRequest) (result *response.SearchResponse, err error) {
	if err := s.checkSearchable(readLeader); err != nil {
		return nil, err
	}
	return s.Engine.Reader().Search(ctx, query), nil
}

func (s *Store) MSearch(ctx context.Context, readLeader bool, query *request.SearchRequest) (result response.SearchResponses, err error) {
	if err := s.checkSearchable(readLeader); err != nil {
		return nil, err
	}
	return s.Engine.Reader().MSearch(ctx, query), nil
}

func (s *Store) StreamSearch(ctx context.Context, readLeader bool, query *request.SearchRequest, resultChan chan *response.DocResult) error {
	if err := s.checkSearchable(readLeader); err != nil {
		return err
	}
	return s.Engine.Reader().StreamSearch(ctx, query, resultChan)
}

//check this store can read
func (s *Store) checkReadable(readLeader bool) error {
	status := s.Partition.GetStatus()

	if status == entity.PA_CLOSED {
		return pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_CLOSED)
	}

	if status == entity.PA_INVALID {
		return pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_INVALID)
	}

	if readLeader && status != entity.PA_READWRITE {
		log.Error("checkReadable status: %d , err: %v", status, pkg.CodeErr(pkg.ERRCODE_PARTITION_NOT_LEADER).Error())
		return pkg.CodeErr(pkg.ERRCODE_PARTITION_NOT_LEADER)
	}

	return nil

}

//check this store can read
func (s *Store) checkSearchable(readLeader bool) error {

	status := s.Partition.GetStatus()
	switch status {
	case entity.PA_CLOSED:
		return pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_CLOSED)
	case entity.PA_INVALID:
		return pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_INVALID)
	case entity.PA_CANNOT_SEARCH:
		return pkg.CodeErr(pkg.ERRCODE_PARTITION_CANNOT_SEARCH)
	}

	if readLeader && status != entity.PA_READWRITE {
		log.Error("checkReadable status: %d , err: %v", status, pkg.CodeErr(pkg.ERRCODE_PARTITION_NOT_LEADER).Error())
		return pkg.CodeErr(pkg.ERRCODE_PARTITION_NOT_LEADER)
	}

	return nil

}
