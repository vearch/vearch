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
	"fmt"
	"time"

	pkg "github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/proto/pspb/raftpb"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/vearchlog"
)

type RaftApplyResponse struct {
	Results []*response.DocResult
	Result  *response.DocResult
	FlushC  chan error
	Err     error
}

func (r *RaftApplyResponse) SetErr(err error) *RaftApplyResponse {
	r.Err = err
	return r
}

func (s *Store) UpdateSpace(ctx context.Context, space *entity.Space) error {
	if err := s.checkWritable(); err != nil {
		return err
	}

	bytes, err := cbjson.Marshal(space)
	if err != nil {
		return err
	}

	// Raft Commit
	raftCmd := raftpb.CreateRaftCommand()
	raftCmd.Type = raftpb.CmdType_UPDATESPACE
	if raftCmd.UpdateSpace == nil {
		raftCmd.UpdateSpace = new(pspb.UpdateSpace)
	}
	raftCmd.UpdateSpace.Version = space.Version
	raftCmd.UpdateSpace.Space = bytes
	defer func() {
		if e := raftCmd.Close(); e != nil {
			log.Error("raft cmd close err : %s", e.Error())
		}
	}()

	data, err := raftCmd.Marshal()

	if err != nil {
		return err
	}

	future := s.RaftServer.Submit(uint64(s.Partition.Id), data)

	response, err := future.Response()
	if err != nil {
		return err
	}

	if response.(*RaftApplyResponse).Err != nil {
		return response.(*RaftApplyResponse).Err
	}

	return nil
}

func (s *Store) DeleteByQuery(ctx context.Context, readLeader bool, query *request.SearchRequest) (delCount int, err error) {
	if err := s.checkWritable(); err != nil {
		return delCount, err
	}

	query.Size = util.PInt(10000)
	query.Fields = []string{"_id"}

	for {
		result := s.Engine.Reader().Search(ctx, query)

		if len(result.Status.Errors) > 0 {
			for _, v := range result.Status.Errors {
				return delCount, v
			}
		}

		if len(result.Hits) == 0 {
			return delCount, nil
		}

		raftCmd := raftpb.CreateRaftCommand()

		raftCmd.Type = raftpb.CmdType_WRITE
		raftCmd.WriteCommand = &pspb.DocCmd{
			Type:    pspb.OpType_DELETE,
			Version: -1,
		}
		for _, hit := range result.Hits {

			raftCmd.WriteCommand.DocId = hit.Id
			data, err := raftCmd.Marshal()
			if err != nil {
				return delCount, err
			}

			future := s.RaftServer.Submit(uint64(s.Partition.Id), data)

			resp, err := future.Response()
			if err != nil {
				return delCount, err
			}

			if resp.(*RaftApplyResponse).Err != nil {
				return delCount, resp.(*RaftApplyResponse).Err
			}

			if resp.(*RaftApplyResponse).Result.Failure != nil && resp.(*RaftApplyResponse).Result.Failure.Reason != "" {
				return delCount, fmt.Errorf(resp.(*RaftApplyResponse).Result.Failure.Reason)
			}
			delCount++
		}

		if err := s.Engine.Writer().Flush(ctx, s.Sn); err != nil {
			return delCount, err
		}
	}

}

func (s *Store) Write(ctx context.Context, request *pspb.DocCmd) (result *response.DocResult, err error) {
	if err = s.checkWritable(); err != nil {
		return nil, err
	}
	raftCmd := raftpb.CreateRaftCommand()
	raftCmd.Type = raftpb.CmdType_WRITE
	raftCmd.WriteCommand = request
	//TODO: pspb.Replace not use check version
	//if (request.Type == pspb.OpType_MERGE || request.Type == pspb.OpType_DELETE) && request.Version == 0 {
	/*if request.Type == pspb.OpType_MERGE || request.Type == pspb.OpType_DELETE {
		doc, err := s.GetRTDocument(ctx, true, request.DocId)
		if err != nil {
			return nil, fmt.Errorf("get document error:%v", err)
		}

		if doc != nil && doc.Failure != nil {
			return nil, fmt.Errorf("get document failed:%v", doc.Failure)
		}

		if doc.Found {
			//raftCmd.WriteCommand.Version = doc.Version
		} else if request.Type == pspb.OpType_MERGE || request.Type == pspb.OpType_DELETE {
			return nil, pkg.CodeErr(pkg.ERRCODE_DOCUMENT_NOT_EXIST)
		}
	}*/
	data, err := raftCmd.Marshal()
	if err != nil {
		return nil, err
	}

	if e := raftCmd.Close(); e != nil {
		log.Error("raft cmd close err : %s", e.Error())
	}
	startTime := time.Now()
	future := s.RaftServer.Submit(uint64(s.Partition.Id), data)
	resp, err := future.Response()
	if err != nil {
		return nil, err
	}

	if resp.(*RaftApplyResponse).Err != nil {
		return nil, resp.(*RaftApplyResponse).Err
	}
	endTime := time.Now()

	resultResp := resp.(*RaftApplyResponse).Result
	if request.Type != pspb.OpType_DELETE && resultResp.CostTime != nil {
		resultResp.CostTime.PsSWStartTime = startTime
		resultResp.CostTime.PsSWEndTime = endTime
	}

	return resultResp, nil
}

func (s *Store) Flush(ctx context.Context) error {

	if err := s.checkWritable(); err != nil {
		return err
	}
	raftCmd := raftpb.CreateRaftCommand()
	raftCmd.Type = raftpb.CmdType_FLUSH

	data, err := raftCmd.Marshal()
	if err != nil {
		return err
	}

	if e := raftCmd.Close(); e != nil {
		log.Error("raft cmd close err : %s", e.Error())
	}

	future := s.RaftServer.Submit(uint64(s.Partition.Id), data)

	response, err := future.Response()
	if err != nil {
		return err
	}

	if response.(*RaftApplyResponse).Err != nil {
		return response.(*RaftApplyResponse).Err
	}

	err = <-response.(*RaftApplyResponse).FlushC
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) checkWritable() error {
	switch s.Partition.GetStatus() {
	case entity.PA_INVALID:
		return vearchlog.LogErrAndReturn(pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_INVALID))
	case entity.PA_CLOSED:
		return vearchlog.LogErrAndReturn(pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_CLOSED))
	case entity.PA_READONLY:
		return vearchlog.LogErrAndReturn(pkg.CodeErr(pkg.ERRCODE_PARTITION_NOT_LEADER))
	case entity.PA_READWRITE:
		return nil
	default:
		return vearchlog.LogErrAndReturn(pkg.CodeErr(pkg.ERRCODE_INTERNAL_ERROR))
	}
}
