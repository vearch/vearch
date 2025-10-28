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
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
)

type ConfigService struct {
	client *client.Client
}

func NewConfigService(client *client.Client) *ConfigService {
	return &ConfigService{client: client}
}

func (s *ConfigService) GetSpaceConfigByName(ctx context.Context, dbName, spaceName string) (cfg *entity.SpaceConfig, err error) {
	mc := s.client.Master()
	// get space info
	dbId, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		log.Error("query db %s err: %s", dbName, err.Error())
		return nil, err
	}

	space, err := mc.QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		log.Error("query space %s/%s err: %s", dbName, spaceName, err.Error())
		return nil, err
	}
	cfg, err = s.getSpaceConfig(ctx, space)

	if err == nil {
		return cfg, nil
	}

	// invoke all space nodeID
	if space != nil && space.Partitions != nil {
		for _, partition := range space.Partitions {
			// get all replicas nodeID
			if partition.Replicas == nil {
				continue
			}
			for _, nodeID := range partition.Replicas {
				server, err := mc.QueryServer(ctx, nodeID)
				if err != nil {
					log.Error("query server %d err: %s", nodeID, err.Error())
					continue
				}
				// send rpc query
				log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
				cfg, err = client.GetEngineCfg(server.RpcAddr(), partition.Id)
				if err != nil {
					log.Error("get engine config err: %s", err.Error())
					continue
				}
				return cfg, nil
			}
		}
	}
	return nil, fmt.Errorf("cannot get engine config")
}

func (s *ConfigService) getSpaceConfig(ctx context.Context, space *entity.Space) (cfg *entity.SpaceConfig, err error) {
	mc := s.client.Master()
	marshal, err := mc.Get(ctx, entity.SpaceConfigKey(space.DBId, space.Id))
	if err != nil {
		return nil, err
	}

	cfg = &entity.SpaceConfig{}
	if err = json.Unmarshal(marshal, cfg); err != nil {
		return nil, err
	}

	return cfg, err
}

// UpdateSpaceConfig updates the space config of the given space in meta.
func (s *ConfigService) UpdateSpaceConfig(ctx context.Context, space *entity.Space, cfg *entity.SpaceConfig) error {
	old_cfg, err := s.getSpaceConfig(ctx, space)
	if err != nil {
		log.Error("get space config err: %s", err.Error())
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
		if cfg.RefreshInterval != nil {
			new_cfg.RefreshInterval = cfg.RefreshInterval
		}
		if cfg.EnableIdCache != nil {
			new_cfg.EnableIdCache = cfg.EnableIdCache
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

// ModifySpaceConfig modifies the space config of the given space to servers and meta.
func (s *ConfigService) ModifySpaceConfig(ctx context.Context, dbName, spaceName string, cfg *entity.SpaceConfig) (err error) {
	mc := s.client.Master()
	// get space info
	dbId, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		log.Error("query db %s err: %s", dbName, err.Error())
		return err
	}

	space, err := mc.QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		log.Error("query space %s/%s err: %s", dbName, spaceName, err.Error())
		return err
	}
	// invoke all space nodeID
	if space != nil && space.Partitions != nil {
		for _, partition := range space.Partitions {
			// get all replicas nodeID
			if partition.Replicas != nil {
				for _, nodeID := range partition.Replicas {
					server, err := mc.QueryServer(ctx, nodeID)
					if err != nil {
						log.Error("query server %d err: %s", nodeID, err.Error())
						continue
					}
					// send rpc query
					log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
					err = client.UpdateEngineCfg(server.RpcAddr(), cfg, partition.Id)
					if err != nil {
						log.Error("update engine config err: %s", err.Error())
						continue
					}
				}
			}
		}
	}
	// Maintain compatibility with older versions
	cfg.DBId = dbId
	cfg.Id = space.Id
	err = s.UpdateSpaceConfig(ctx, space, cfg)
	if err != nil {
		log.Error("update space config err: %s", err.Error())
		return err
	}
	return nil
}

func (s *ConfigService) GetRequestLimitCfg(ctx context.Context) (*entity.RouterLimitCfg, error) {
	mc := s.client.Master()
	marshal, err := mc.Get(ctx, entity.RouterConfigKey("request_limit_config"))
	if err != nil {
		return nil, err
	}

	cfg := &entity.RouterLimitCfg{}
	if err = json.Unmarshal(marshal, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (s *ConfigService) ModifyRequestLimitCfg(ctx context.Context, cfg *entity.RouterLimitCfg) (err error) {
	old_cfg, err := s.GetRequestLimitCfg(ctx)
	if err != nil {
		log.Error("get router request limit config err: %s", err.Error())
	}
	new_cfg := cfg

	if old_cfg != nil {
		new_cfg = old_cfg

		if cfg.RequestLimitEnabled != new_cfg.RequestLimitEnabled {
			new_cfg.RequestLimitEnabled = cfg.RequestLimitEnabled
		}
		if cfg.RequestLimitEnabled && cfg.TotalReadLimit > 0 {
			new_cfg.TotalReadLimit = cfg.TotalReadLimit
		}
		if cfg.RequestLimitEnabled && cfg.TotalWriteLimit > 0 {
			new_cfg.TotalWriteLimit = cfg.TotalWriteLimit
		}
	}

	marshal, err := json.Marshal(new_cfg)
	if err != nil {
		return err
	}
	mc := s.client.Master()
	if err = mc.Update(ctx, entity.RouterConfigKey("request_limit_config"), marshal); err != nil {
		return err
	}

	return nil
}
