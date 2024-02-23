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
	"encoding/json"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/metrics/mserver"
)

func operatePartition(method, addr string, space *entity.Space, pid uint32) error {
	bytes, e := sonic.Marshal(space)
	if e != nil {
		return e
	}
	args := &vearchpb.PartitionData{PartitionID: pid, Data: bytes}
	reply := new(vearchpb.PartitionData)
	err := Execute(addr, method, args, reply)
	if err != nil {
		return err
	}
	if reply != nil && reply.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		return vearchpb.NewError(reply.Err.Code, nil)
	}
	return nil
}

func CreatePartition(addr string, space *entity.Space, pid uint32) error {
	return operatePartition(CreatePartitionHandler, addr, space, pid)
}

func UpdatePartition(addr string, space *entity.Space, pid entity.PartitionID) error {
	return operatePartition(UpdatePartitionHandler, addr, space, pid)
}

func GetEngineCfg(addr string, pid entity.PartitionID) (cfg *entity.EngineCfg, err error) {
	args := &vearchpb.PartitionData{PartitionID: pid, Type: vearchpb.OpType_GET}
	reply := new(vearchpb.PartitionData)
	err = Execute(addr, EngineCfgHandler, args, reply)
	if err != nil {
		return nil, err
	} else if reply != nil && reply.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		return nil, vearchpb.NewError(reply.Err.Code, nil)
	}
	if reply.Data != nil {
		cfg := &entity.EngineCfg{}
		err = cbjson.Unmarshal(reply.Data, cfg)
		if err != nil {
			return nil, err
		}
		data, _ := json.Marshal(cfg)
		log.Debug("get engine cfg [%+v]", string(data))
		return cfg, nil
	}

	return nil, nil
}

func UpdateEngineCfg(addr string, cacheCfg *entity.EngineCfg, pid entity.PartitionID) error {
	value, err := sonic.Marshal(cacheCfg)
	if err != nil {
		return err
	}

	args := &vearchpb.PartitionData{PartitionID: pid, Data: value, Type: vearchpb.OpType_CREATE}
	reply := new(vearchpb.PartitionData)
	err = Execute(addr, EngineCfgHandler, args, reply)
	if err != nil {
		return err
	} else if reply != nil && reply.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		return vearchpb.NewError(reply.Err.Code, nil)
	}
	return nil
}

func DeleteReplica(addr string, partitionId uint32) error {
	args := &vearchpb.PartitionData{PartitionID: partitionId}
	reply := new(vearchpb.PartitionData)
	err := Execute(addr, DeleteReplicaHandler, args, reply)
	if err != nil {
		return err
	} else if reply != nil && reply.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		return vearchpb.NewError(reply.Err.Code, nil)
	}
	return nil
}

func DeletePartition(addr string, pid uint32) error {
	args := &vearchpb.PartitionData{PartitionID: pid}
	reply := new(vearchpb.PartitionData)
	err := Execute(addr, DeletePartitionHandler, args, reply)
	if err != nil {
		return err
	} else if reply != nil && reply.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		return vearchpb.NewError(reply.Err.Code, nil)
	}
	return nil
}

func ServerStats(addr string) *mserver.ServerStats {
	args := new(vearchpb.PartitionData)
	reply := new(vearchpb.PartitionData)
	err := Execute(addr, StatsHandler, args, reply)
	if err != nil {
		return mserver.NewErrServerStatus(strings.Split(addr, ":")[0], err)
	} else if reply != nil && reply.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		err = vearchpb.NewError(reply.Err.Code, nil)
		return mserver.NewErrServerStatus(strings.Split(addr, ":")[0], err)
	}
	serverStats := new(mserver.ServerStats)
	err = cbjson.Unmarshal(reply.Data, serverStats)
	if err != nil {
		return mserver.NewErrServerStatus(strings.Split(addr, ":")[0], err)
	}
	if serverStats.Status == 0 {
		serverStats.Status = 200
	}
	return serverStats

}

func IsLive(addr string) bool {
	err := Execute(addr, IsLiveHandler, new(vearchpb.PartitionData), new(vearchpb.PartitionData))
	return err == nil
}

// PartitionInfo get partition info about partitionID
func PartitionInfo(addr string, pid entity.PartitionID, detail_info bool) (value *entity.PartitionInfo, err error) {
	infos, err := _partitionsInfo(addr, pid, detail_info)
	if err != nil {
		return nil, err
	}
	return infos[0], nil
}

// PartitionInfos get all partition info from server
func PartitionInfos(addr string) (value []*entity.PartitionInfo, err error) {
	return _partitionsInfo(addr, 0, false)
}

// internal method for partitionInfo and partitionInfos
func _partitionsInfo(addr string, pid entity.PartitionID, detail_info bool) (value []*entity.PartitionInfo, err error) {
	args := &vearchpb.PartitionData{PartitionID: pid}
	if detail_info {
		// TODO now use this to judge
		args.Type = vearchpb.OpType_GET
	}
	reply := new(vearchpb.PartitionData)
	err = Execute(addr, PartitionInfoHandler, args, reply)
	if err != nil {
		return nil, err
	} else if reply != nil && reply.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		return nil, vearchpb.NewError(reply.Err.Code, nil)
	}
	value = make([]*entity.PartitionInfo, 0, 1)
	err = cbjson.Unmarshal(reply.Data, &value)
	if err != nil {
		log.Error("Unmarshal partition info failed, err: [%v]", err)
		return
	}
	return value, nil
}

func ChangeMember(addr string, changeMember *entity.ChangeMember) error {
	value, err := sonic.Marshal(changeMember)
	if err != nil {
		return err
	}

	args := &vearchpb.PartitionData{PartitionID: changeMember.PartitionID, Data: value}
	reply := new(vearchpb.PartitionData)
	err = Execute(addr, ChangeMemberHandler, args, reply)
	if err != nil {
		return err
	} else if reply != nil && reply.Err.Code != vearchpb.ErrorEnum_SUCCESS {
		return vearchpb.NewError(reply.Err.Code, nil)
	}
	return nil
}
