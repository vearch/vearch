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

type RoleService struct {
	client *client.Client
}

func NewRoleService(client *client.Client) *RoleService {
	return &RoleService{client: client}
}

func (s *RoleService) CreateRole(ctx context.Context, role *entity.Role) (err error) {
	// validate
	if err = role.Validate(); err != nil {
		return err
	}
	mc := s.client.Master()
	mutex := mc.NewLock(ctx, entity.LockRoleKey(role.Name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for create role err %s", err)
		}
	}()
	err = mc.STM(context.Background(), func(stm concurrency.STM) error {
		roleKey := entity.RoleKey(role.Name)

		value := stm.Get(roleKey)
		if value != "" {
			return vearchpb.NewError(vearchpb.ErrorEnum_ROLE_EXIST, nil)
		}
		marshal, err := json.Marshal(role)
		if err != nil {
			return err
		}
		stm.Put(roleKey, string(marshal))
		return nil
	})
	return err
}

func (s *RoleService) DeleteRole(ctx context.Context, roleName string) (err error) {
	role, err := s.QueryRole(ctx, roleName)
	if err != nil {
		return err
	}
	mc := s.client.Master()
	// it will lock cluster
	mutex := mc.NewLock(ctx, entity.LockRoleKey(roleName), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for delete role err %s", err)
		}
	}()

	err = mc.STM(context.Background(),
		func(stm concurrency.STM) error {
			stm.Del(entity.RoleKey(role.Name))
			return nil
		})

	if err != nil {
		return err
	}

	return nil
}

func (s *RoleService) QueryRole(ctx context.Context, role_name string) (role *entity.Role, err error) {
	role = &entity.Role{Name: role_name}
	mc := s.client.Master()

	if value, exists := entity.RoleMap[role_name]; exists {
		role = &value
		return role, nil
	}

	bs, err := mc.Get(ctx, entity.RoleKey(role_name))

	if err != nil {
		return nil, err
	}

	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_ROLE_NOT_EXIST, nil)
	}

	err = json.Unmarshal(bs, role)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get role:%s, err:%s", role.Name, err.Error()))
	}

	return role, nil
}

func (s *RoleService) QueryAllRole(ctx context.Context) ([]*entity.Role, error) {
	mc := s.client.Master()
	_, values, err := mc.PrefixScan(ctx, entity.PrefixRole)
	if err != nil {
		return nil, err
	}
	roles := make([]*entity.Role, 0, len(values))
	for _, value := range values {
		if value == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_ROLE_NOT_EXIST, nil)
		}
		role := &entity.Role{}
		err = json.Unmarshal(value, role)
		if err != nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get role:%s, err:%s", role.Name, err.Error()))
		}
		roles = append(roles, role)
	}

	return roles, err
}

func (s *RoleService) QueryUserWithPassword(ctx context.Context, user_name string, check_role bool) (userRole *entity.UserRole, err error) {
	user := &entity.User{Name: user_name}

	mc := s.client.Master()
	bs, err := mc.Get(ctx, entity.UserKey(user_name))

	if err != nil {
		return nil, err
	}

	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_USER_NOT_EXIST, nil)
	}

	err = json.Unmarshal(bs, user)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get user:%s, err:%s", user.Name, err.Error()))
	}

	userRole = &entity.UserRole{Name: user.Name, Password: user.Password}
	if check_role {
		if role, err := s.QueryRole(ctx, *user.RoleName); err != nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get user:%s role:%s, err:%s", user.Name, *user.RoleName, err.Error()))
		} else {
			userRole.Role = *role
		}
	}

	return userRole, nil
}

func (s *RoleService) ChangeRolePrivilege(ctx context.Context, role *entity.Role) (new_role *entity.Role, err error) {
	//validate
	if err = role.Validate(); err != nil {
		return nil, err
	}

	mc := s.client.Master()
	bs, err := mc.Get(ctx, entity.RoleKey(role.Name))
	if err != nil {
		return nil, err
	}
	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_ROLE_NOT_EXIST, nil)
	}
	old_role := &entity.Role{}
	err = json.Unmarshal(bs, old_role)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get role privilege:%s err:%s", old_role.Name, err.Error()))
	}

	// it will lock cluster
	mutex := mc.NewLock(ctx, entity.LockRoleKey(role.Name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return nil, err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for update role privilege err %s", err)
		}
	}()
	err = mc.STM(context.Background(), func(stm concurrency.STM) error {
		if len(old_role.Privileges) == 0 {
			old_role.Privileges = make(map[entity.Resource]entity.Privilege)
		}
		for resource, privilege := range role.Privileges {
			if role.Operator == entity.Grant {
				old_role.Privileges[resource] = privilege
			}
			if role.Operator == entity.Revoke {
				delete(old_role.Privileges, resource)
			}
		}
		marshal, err := json.Marshal(old_role)
		if err != nil {
			return err
		}
		stm.Put(entity.RoleKey(old_role.Name), string(marshal))
		return nil
	})
	return old_role, nil
}
