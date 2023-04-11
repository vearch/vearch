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

	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/ps/psutil"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/vearchlog"
        "sync/atomic"
)

var commitId int64

type RaftApplyResponse struct {
	FlushC chan error
	Err    error
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
	raftCmd := vearchpb.CreateRaftCommand()
	raftCmd.Type = vearchpb.CmdType_UPDATESPACE
	if raftCmd.UpdateSpace == nil {
		raftCmd.UpdateSpace = new(vearchpb.UpdateSpace)

	}
	raftCmd.UpdateSpace.Version = space.Version
	raftCmd.UpdateSpace.Space = bytes

	tmp_space := &entity.Space{}
	cerr := cbjson.Unmarshal(raftCmd.UpdateSpace.Space, tmp_space)
	if cerr != nil {
        return cerr
	}

	cerr = s.Engine.UpdateMapping(tmp_space)
	if cerr != nil {
        return cerr
	}

	// save partition meta file
	cerr = psutil.SavePartitionMeta(s.GetPartition().Path, s.GetPartition().Id, tmp_space)
	if cerr != nil {
		return cerr
	}

	s.SetSpace(tmp_space)

	return nil

}

func (s *Store) Write(ctx context.Context, request *vearchpb.DocCmd, query *vearchpb.SearchRequest, response *vearchpb.SearchResponse) (err error) {
        if err = s.checkWritable(); err != nil {
        	return err
        }
	raftCmd := vearchpb.CreateRaftCommand()
	if query != nil {
		raftCmd.Type = vearchpb.CmdType_SEARCHDEL
		raftCmd.SearchDelReq = query
		raftCmd.SearchDelResp = response
		docCmd := &vearchpb.DocCmd{Type: vearchpb.OpType_SEARCHDELETE}
		raftCmd.WriteCommand = docCmd
	} else {
		raftCmd.Type = vearchpb.CmdType_WRITE
		raftCmd.WriteCommand = request
	}


	switch raftCmd.Type {
	case vearchpb.CmdType_WRITE:
		err = s.Engine.Writer().Write(s.Ctx, raftCmd.WriteCommand, nil, nil)
	case vearchpb.CmdType_SEARCHDEL:
		err = s.Engine.Writer().Write(s.Ctx, raftCmd.WriteCommand, raftCmd.SearchDelReq, raftCmd.SearchDelResp)
	default:
		log.Error("unsupported command[%s]", raftCmd.Type)
		// resp.SetErr(fmt.Errorf("unsupported command[%s]", raftCmd.Type))
	}
	atomic.AddInt64(&s.CurrentSn, 1)
	return nil
}

//raft submit do
func (s *Store) RaftSubmit(data []byte) (err error) {

	future := s.RaftServer.Submit(uint64(s.Partition.Id), data)
	resp, err := future.Response()
	if err != nil {
		return err
	}
	if resp.(*RaftApplyResponse).Err != nil {
		return resp.(*RaftApplyResponse).Err
	}
	return nil
}

func (s *Store) Flush(ctx context.Context) error {

	if err := s.checkWritable(); err != nil {
		return err
	}
	raftCmd := vearchpb.CreateRaftCommand()
	raftCmd.Type = vearchpb.CmdType_FLUSH

    // NEED_FIX
       
       var current = atomic.LoadInt64(&s.CurrentSn)
    _, err := s.Engine.Writer().Commit(s.Ctx, current)

	if err != nil {
		return err
	}

	return nil
}

func (s *Store) checkWritable() error {
	switch s.Partition.GetStatus() {
	case entity.PA_INVALID:
		return vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, nil))
	case entity.PA_CLOSED:
		return vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil))
	case entity.PA_READWRITE:
		return nil
	default:
		return nil
	}
}
