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

package services

import (
	"context"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

type ServerService struct {
	client *client.Client
}

func NewServerService(client *client.Client) *ServerService {
	return &ServerService{client: client}
}

// RegisterServer find nodeId partitions
func (s *ServerService) RegisterServer(ctx context.Context, ip string, nodeID entity.NodeID) (*entity.Server, error) {
	server := &entity.Server{Ip: ip}

	mc := s.client.Master()
	spaces, err := mc.QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		return nil, err
	}

	spaceConfigs, err := mc.QuerySpaceConfigsByKey(ctx, entity.PrefixSpaceConfig)
	if err != nil {
		return nil, err
	}

	configMap := make(map[entity.SpaceID]*entity.SpaceConfig)
	for _, config := range spaceConfigs {
		configMap[config.Id] = config
	}

	for _, s := range spaces {
		if config, exists := configMap[s.Id]; exists {
			s.RefreshInterval = *config.RefreshInterval
		}
		for _, p := range s.Partitions {
			for _, id := range p.Replicas {
				if nodeID == id {
					server.PartitionIds = append(server.PartitionIds, p.Id)
					server.Spaces = append(server.Spaces, s)
					break
				}
			}
		}
	}

	return server, nil
}

// recover fail node
func (s *ServerService) RecoverFailServer(ctx context.Context, spaceService *SpaceService, memberService *MemberService, rs *entity.RecoverFailServer) (e error) {
	// panic process
	defer errutil.CatchError(&e)
	mc := s.client.Master()
	// get fail server info
	targetFailServer := mc.QueryServerByIPAddr(ctx, rs.FailNodeAddr)
	log.Debug("targetFailServer is %s", targetFailServer)
	// get new server info
	newServer := mc.QueryServerByIPAddr(ctx, rs.NewNodeAddr)
	log.Debug("newServer is %s", newServer)
	if newServer.ID <= 0 || targetFailServer.ID <= 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_SERVER_ERROR, fmt.Errorf("newServer or targetFailServer is nil"))
	}

	for _, pid := range targetFailServer.Node.PartitionIds {
		cm := &entity.ChangeMember{}
		cm.Method = proto.ConfAddNode
		cm.NodeID = newServer.ID
		cm.PartitionID = pid
		if e = memberService.ChangeMember(ctx, spaceService, cm); e != nil {
			e = fmt.Errorf("ChangePartitionMember failed [%+v], err is %s ", cm, e.Error())
			log.Error(e)
			return e
		}
		log.Info("ChangePartitionMember add [%+v] success,", cm)
		cm.Method = proto.ConfRemoveNode
		cm.NodeID = targetFailServer.ID
		cm.PartitionID = pid
		if e = memberService.ChangeMember(ctx, spaceService, cm); e != nil {
			e = fmt.Errorf("ChangePartitionMember failed [%+v], err is %s ", cm, e.Error())
			log.Error(e)
			return e
		}
		log.Info("ChangePartitionMember remove [%+v]", cm)
	}
	// if success, remove from failServer
	mc.TryRemoveFailServer(ctx, targetFailServer.Node)

	return nil
}

func (s *ServerService) Stats(ctx context.Context) ([]*mserver.ServerStats, error) {
	mc := s.client.Master()
	servers, err := mc.QueryServers(ctx)
	if err != nil {
		return nil, err
	}

	statsChan := make(chan *mserver.ServerStats, len(servers))

	for _, s := range servers {
		go func(s *entity.Server) {
			defer func() {
				if r := recover(); r != nil {
					statsChan <- mserver.NewErrServerStatus(s.RpcAddr(), errors.New(cast.ToString(r)))
				}
			}()
			statsChan <- client.ServerStats(s.RpcAddr())
		}(s)
	}

	result := make([]*mserver.ServerStats, 0, len(servers))

	for {
		select {
		case s := <-statsChan:
			result = append(result, s)
		case <-ctx.Done():
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_TIMEOUT, nil)
		default:
			time.Sleep(time.Millisecond * 10)
			if len(result) >= len(servers) {
				close(statsChan)
				goto out
			}
		}
	}

out:

	return result, nil
}

func (s *ServerService) IsExistNode(ctx context.Context, id entity.NodeID, ip string) error {
	mc := s.client.Master()
	values, err := mc.Get(ctx, entity.ServerKey(id))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("look up key[%s] in etcd failed", entity.ServerKey(id)))
	}
	if values == nil {
		return nil
	}
	server := &entity.Server{}
	err = json.Unmarshal(values, server)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("parse key[%s] in etcd failed", entity.ServerKey(id)))
	}
	if server.Ip != ip {
		return errors.Errorf("node id[%d] has register on ip[%s]", server.ID, server.Ip)
	}
	return nil
}

func (s *ServerService) ResourceLimitService(ctx context.Context, dbService *DBService, resourceLimit *entity.ResourceLimit) (err error) {
	spaces := make([]*entity.Space, 0)
	dbNames := make([]string, 0)
	if resourceLimit.DbName == nil && resourceLimit.SpaceName != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("if space_name is set, db_name must be set"))
	}
	if resourceLimit.DbName != nil {
		dbNames = append(dbNames, *resourceLimit.DbName)
	}

	if len(dbNames) == 0 {
		dbs, err := dbService.QueryDBs(ctx)
		if err != nil {
			return err
		}
		dbNames = make([]string, len(dbs))
		for i, db := range dbs {
			dbNames[i] = db.Name
		}
	}

	mc := s.client.Master()
	for _, dbName := range dbNames {
		dbID, err := mc.QueryDBName2ID(ctx, dbName)
		if err != nil {
			return err
		}
		if resourceLimit.SpaceName != nil {
			if space, err := mc.QuerySpaceByName(ctx, dbID, *resourceLimit.SpaceName); err != nil {
				return err
			} else {
				spaces = append(spaces, space)
			}
		} else {
			if dbSpaces, err := mc.QuerySpaces(ctx, dbID); err != nil {
				return err
			} else {
				spaces = append(spaces, dbSpaces...)
			}
		}
	}

	log.Debug("ResourceLimitService dbNames: %v, len(spaces): %d", dbNames, len(spaces))
	check := false
	for _, space := range spaces {
		for _, partition := range space.Partitions {
			for _, nodeID := range partition.Replicas {
				if server, err := mc.QueryServer(ctx, nodeID); err != nil {
					return err
				} else {
					check = true
					err = client.ResourceLimit(server.RpcAddr(), resourceLimit, partition.Id)
					if err != nil {
						log.Error("ResourceLimit err: %s", err.Error())
						continue
					}
				}
			}
		}
	}
	if !check {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("target spaces is empty, no need to check resource limit"))
	}

	return nil
}
