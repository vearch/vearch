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

package client

import (
	"context"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/util/metrics/mserver"
	"strings"
	"time"
)

type adminSender struct {
	*sender
	addr string
}

func (this *adminSender) CreatePartition(space *entity.Space, partitionId uint32) error {
	request, err := request.NewObjRequest(this.Ctx, partitionId, struct {
		Space       *entity.Space
		PartitionId uint32
	}{Space: space, PartitionId: partitionId})
	if err != nil {
		return err
	}

	if _, status, err := Execute(this.addr, CreatePartitionHandler, request); err != nil {
		return err
	} else if status != pkg.ERRCODE_SUCCESS {
		return pkg.CodeErr(status)
	}

	return nil
}

func (this *adminSender) UpdatePartition(space *entity.Space, pid entity.PartitionID) error {
	reqs, err := request.NewObjRequest(this.Ctx, pid, space)
	if err != nil {
		return err
	}
	_, _, err = Execute(this.addr, UpdatePartitionHandler, reqs)
	return err
}

func (this *adminSender) DeletePartition(partitionId uint32) error {
	reqs, err := request.NewObjRequest(this.Ctx, partitionId, partitionId)
	if err != nil {
		return err
	}
	_, _, e := Execute(this.addr, DeletePartitionHandler, reqs)
	return e
}

func (this *adminSender) ServerStats() *mserver.ServerStats {
	request := &request.ObjRequest{
		RequestContext: this.Ctx,
	}
	resp, status, err := Execute(this.addr, StatsHandler, request)

	if err != nil {
		return mserver.NewErrServerStatus(strings.Split(this.addr, ":")[0], err)
	}

	serverStats := resp.(*mserver.ServerStats)
	serverStats.Status = status
	return serverStats
}

func (this *adminSender) IsLive() bool {
	objRequest := &request.ObjRequest{
		RequestContext: this.Ctx,
	}

	//default time out for live
	timeout, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	objRequest.SetContext(timeout)

	_, _, err := Execute(this.addr, IsLiveHandler, objRequest)
	if err != nil {
		return false
	}

	return true
}


//get partition info about
func (this *adminSender) PartitionInfo(pid entity.PartitionID) (value *entity.PartitionInfo, err error) {
	objRequest := &request.ObjRequest{
		RequestContext: this.Ctx,
		PartitionID:    pid,
	}

	//default time out for live
	timeout, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()
	objRequest.SetContext(timeout)

	result, _, err := Execute(this.addr, PartitionInfoHandler, objRequest)
	if err != nil {
		return
	}

	value = &entity.PartitionInfo{}
	err = result.(*response.ObjResponse).Decode(&value)
	if err != nil {
		return
	}

	return value, nil
}
