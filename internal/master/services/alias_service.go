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

	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type AliasService struct {
	client *client.Client
}

func NewAliasService(client *client.Client) *AliasService {
	return &AliasService{client: client}
}

func (s *AliasService) CreateAlias(ctx context.Context, alias *entity.Alias) (err error) {
	// validate name
	if err = alias.Validate(); err != nil {
		return err
	}
	mc := s.client.Master()
	mutex := mc.NewLock(ctx, entity.LockAliasKey(alias.Name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for create alias err %s", err)
		}
	}()
	err = mc.STM(context.Background(), func(stm concurrency.STM) error {
		aliasKey := entity.AliasKey(alias.Name)

		value := stm.Get(aliasKey)
		if value != "" {
			return vearchpb.NewError(vearchpb.ErrorEnum_ALIAS_EXIST, nil)
		}
		marshal, err := json.Marshal(alias)
		if err != nil {
			return err
		}
		stm.Put(aliasKey, string(marshal))
		return nil
	})
	return err
}

func (s *AliasService) DeleteAlias(ctx context.Context, aliasName string) (err error) {
	alias, err := s.QueryAlias(ctx, aliasName)
	if err != nil {
		return err
	}
	// it will lock cluster
	mc := s.client.Master()
	mutex := mc.NewLock(ctx, entity.LockAliasKey(aliasName), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for delete alias err %s", err)
		}
	}()

	err = mc.STM(context.Background(),
		func(stm concurrency.STM) error {
			stm.Del(entity.AliasKey(alias.Name))
			return nil
		})

	if err != nil {
		return err
	}

	return nil
}

func (s *AliasService) UpdateAlias(ctx context.Context, alias *entity.Alias) (err error) {
	mc := s.client.Master()
	bs, err := mc.Get(ctx, entity.AliasKey(alias.Name))
	if err != nil {
		return err
	}
	if bs == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_ALIAS_NOT_EXIST, nil)
	}

	// it will lock cluster
	mutex := mc.NewLock(ctx, entity.LockAliasKey(alias.Name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for update alias err %s", err)
		}
	}()
	err = mc.STM(context.Background(), func(stm concurrency.STM) error {
		marshal, err := json.Marshal(alias)
		if err != nil {
			return err
		}
		stm.Put(entity.AliasKey(alias.Name), string(marshal))
		return nil
	})
	return nil
}

func (s *AliasService) QueryAllAlias(ctx context.Context) ([]*entity.Alias, error) {
	mc := s.client.Master()
	_, values, err := mc.PrefixScan(ctx, entity.PrefixAlias)
	if err != nil {
		return nil, err
	}
	allAlias := make([]*entity.Alias, 0, len(values))
	for _, value := range values {
		if value == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_ALIAS_NOT_EXIST, nil)
		}
		alias := &entity.Alias{}
		err = json.Unmarshal(value, alias)
		if err != nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get alias:%s value:%s, err:%s", alias.Name, string(value), err.Error()))
		}
		allAlias = append(allAlias, alias)
	}

	return allAlias, err
}

func (s *AliasService) QueryAlias(ctx context.Context, aliasName string) (alias *entity.Alias, err error) {
	alias = &entity.Alias{Name: aliasName}

	mc := s.client.Master()
	bs, err := mc.Get(ctx, entity.AliasKey(aliasName))

	if err != nil {
		return nil, err
	}

	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_ALIAS_NOT_EXIST, nil)
	}

	err = json.Unmarshal(bs, alias)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get alias:%s value:%s, err:%s", alias.Name, string(bs), err.Error()))
	}
	return alias, nil
}
