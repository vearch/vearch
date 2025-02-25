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

	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
)

type ConfigService struct {
	client *client.Client
}

func NewConfigService(client *client.Client) *ConfigService {
	return &ConfigService{client: client}
}

func (s *ConfigService) GetEngineCfg(ctx context.Context, dbName, spaceName string) (cfg *entity.EngineConfig, err error) {
	defer errutil.CatchError(&err)
	mc := s.client.Master()
	// get space info
	dbId, err := mc.QueryDBName2Id(ctx, dbName)
	if err != nil {
		errutil.ThrowError(err)
	}

	space, err := mc.QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		log.Error("query space %s/%s err: %s", dbName, spaceName, err.Error())
		return nil, err
	}
	cfg, err = s.getEngineConfig(ctx, space)

	if err == nil {
		return cfg, nil
	}

	// invoke all space nodeID
	if space != nil && space.Partitions != nil {
		for _, partition := range space.Partitions {
			// get all replicas nodeID
			if partition.Replicas != nil {
				for _, nodeID := range partition.Replicas {
					server, err := mc.QueryServer(ctx, nodeID)
					errutil.ThrowError(err)
					// send rpc query
					log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
					cfg, err = client.GetEngineCfg(server.RpcAddr(), partition.Id)
					errutil.ThrowError(err)
					if err == nil {
						return cfg, nil
					}
				}
			}
		}
	}
	return nil, fmt.Errorf("cannot get engine config")
}

func (s *ConfigService) getEngineConfig(ctx context.Context, space *entity.Space) (cfg *entity.EngineConfig, err error) {
	defer errutil.CatchError(&err)
	mc := s.client.Master()
	marshal, err := mc.Get(ctx, entity.SpaceConfigKey(space.DBId, space.Id))
	if err != nil {
		return nil, err
	}

	cfg = &entity.EngineConfig{}
	if err = json.Unmarshal(marshal, cfg); err != nil {
		return nil, err
	}

	return cfg, err
}

func (s *ConfigService) UpdateEngineConfig(ctx context.Context, space *entity.Space, cfg *entity.EngineConfig) error {
	old_cfg, err := s.getEngineConfig(ctx, space)
	if err != nil {
		log.Error("get engine config err: %s", err.Error())
	}

	new_cfg := cfg
	if old_cfg != nil {
		new_cfg = old_cfg
		if cfg.EngineCacheSize != nil {
			new_cfg.EngineCacheSize = cfg.EngineCacheSize
		}
		if cfg.LongSearchTime != nil {
			new_cfg.LongSearchTime = cfg.LongSearchTime
		}
		if cfg.Path != nil {
			new_cfg.Path = cfg.Path
		}
	}
	marshal, err := json.Marshal(new_cfg)
	if err != nil {
		return err
	}
	mc := s.client.Master()
	if err = mc.Update(ctx, entity.SpaceConfigKey(space.DBId, space.Id), marshal); err != nil {
		return err
	}

	return nil
}

func (s *ConfigService) ModifyEngineCfg(ctx context.Context, dbName, spaceName string, cfg *entity.EngineConfig) (err error) {
	defer errutil.CatchError(&err)
	mc := s.client.Master()
	// get space info
	dbId, err := mc.QueryDBName2Id(ctx, dbName)
	if err != nil {
		errutil.ThrowError(err)
	}

	space, err := mc.QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		errutil.ThrowError(err)
	}
	// invoke all space nodeID
	if space != nil && space.Partitions != nil {
		for _, partition := range space.Partitions {
			// get all replicas nodeID
			if partition.Replicas != nil {
				for _, nodeID := range partition.Replicas {
					server, err := mc.QueryServer(ctx, nodeID)
					errutil.ThrowError(err)
					// send rpc query
					log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
					err = client.UpdateEngineCfg(server.RpcAddr(), cfg, partition.Id)
					errutil.ThrowError(err)
				}
			}
		}
	}

	err = s.UpdateEngineConfig(ctx, space, cfg)
	if err != nil {
		log.Error("update engine config err: %s", err.Error())
		return err
	}
	return nil
}
