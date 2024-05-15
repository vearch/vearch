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

	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/runtime/os"
	"github.com/vearch/vearch/v3/internal/pkg/vearchlog"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

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

	bytes, err := vjson.Marshal(space)
	if err != nil {
		return err
	}

	// Raft Commit
	raftCmd := &vearchpb.RaftCommand{
		Type: vearchpb.CmdType_UPDATESPACE,
		UpdateSpace: &vearchpb.UpdateSpace{
			Version: space.Version,
			Space:   bytes,
		},
	}

	data, err := vjson.Marshal(raftCmd)

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

func (s *Store) Write(ctx context.Context, request *vearchpb.DocCmd) (err error) {
	if err = s.checkWritable(); err != nil {
		return err
	}

	if request.Type == vearchpb.OpType_BULK {
		if s.Partition.ResourceExhausted {
			err = vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_RESOURCE_EXHAUSTED, nil)
			return err
		}
		s.Partition.AddNum += 1
		if s.Partition.AddNum%10000 == 0 {
			s.Partition.ResourceExhausted, err = os.CheckResource(s.RaftPath)
			if s.Partition.ResourceExhausted {
				return err
			}
		}
	}

	raftCmd := &vearchpb.RaftCommand{
		Type:         vearchpb.CmdType_WRITE,
		WriteCommand: request,
	}

	data, err := vjson.Marshal(raftCmd)
	if err != nil {
		return err
	}

	// sumbit raft
	err = s.RaftSubmit(data)
	if err != nil {
		return err
	}

	return nil
}

// raft submit do
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
	var err error
	if err := s.checkWritable(); err != nil {
		return err
	}

	s.Partition.ResourceExhausted, err = os.CheckResource(s.RaftPath)
	if err != nil {
		log.Warn(err.Error())
	}
	raftCmd := &vearchpb.RaftCommand{
		Type: vearchpb.CmdType_FLUSH,
	}
	data, err := vjson.Marshal(raftCmd)
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

	err = <-response.(*RaftApplyResponse).FlushC
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
	case entity.PA_READONLY:
		return vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_LEADER, nil))
	case entity.PA_READWRITE:
		return nil
	default:
		return vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, nil))
	}
}
