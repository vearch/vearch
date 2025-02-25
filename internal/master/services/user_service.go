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
	"strings"
	"time"

	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type UserService struct {
	client *client.Client
}

func NewUserService(client *client.Client) *UserService {
	return &UserService{client: client}
}

func (s *UserService) CreateUser(ctx context.Context, role *RoleService, user *entity.User, check_root bool) (err error) {
	//validate name
	if err = user.Validate(check_root); err != nil {
		return err
	}

	if user.RoleName != nil {
		if _, err := role.QueryRole(ctx, *user.RoleName); err != nil {
			return err
		}
	} else {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("role name is empty"))
	}
	if user.Password == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("password is empty"))
	}

	mc := s.client.Master()
	mutex := mc.NewLock(ctx, entity.LockUserKey(user.Name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for create user err %s", err)
		}
	}()
	err = mc.STM(context.Background(), func(stm concurrency.STM) error {
		userKey := entity.UserKey(user.Name)

		value := stm.Get(userKey)
		if value != "" {
			return vearchpb.NewError(vearchpb.ErrorEnum_USER_EXIST, nil)
		}
		marshal, err := json.Marshal(user)
		if err != nil {
			return err
		}
		stm.Put(userKey, string(marshal))
		return nil
	})
	return err
}

func (s *UserService) DeleteUser(ctx context.Context, role *RoleService, userName string) error {
	if strings.EqualFold(userName, entity.RootName) {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("can't delete root user"))
	}
	user, err := s.QueryUser(ctx, role, userName, false)
	if user == nil {
		return err
	}
	// it will lock cluster
	mc := s.client.Master()
	mutex := mc.NewLock(ctx, entity.LockUserKey(userName), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for delete user err %s", err)
		}
	}()

	err = mc.STM(context.Background(),
		func(stm concurrency.STM) error {
			stm.Del(entity.UserKey(user.Name))
			return nil
		})

	if err != nil {
		return err
	}

	return nil
}

func (s *UserService) QueryUser(ctx context.Context, role *RoleService, userName string, checkRole bool) (userRole *entity.UserRole, err error) {
	user := &entity.User{Name: userName}

	mc := s.client.Master()
	bs, err := mc.Get(ctx, entity.UserKey(userName))

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

	userRole = &entity.UserRole{Name: user.Name}
	if checkRole {
		if role, err := role.QueryRole(ctx, *user.RoleName); err != nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get user:%s role:%s, err:%s", user.Name, *user.RoleName, err.Error()))
		} else {
			userRole.Role = *role
		}
	}

	return userRole, nil
}

func (s *UserService) QueryAllUser(ctx context.Context, role *RoleService) ([]*entity.UserRole, error) {
	mc := s.client.Master()
	_, values, err := mc.PrefixScan(ctx, entity.PrefixUser)
	if err != nil {
		return nil, err
	}
	users := make([]*entity.UserRole, 0, len(values))
	for _, value := range values {
		if value == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_USER_NOT_EXIST, nil)
		}
		user := &entity.User{}
		err = json.Unmarshal(value, user)
		if err != nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get user:%s, err:%s", user.Name, err.Error()))
		}
		userRole := &entity.UserRole{Name: user.Name}
		if role, err := role.QueryRole(ctx, *user.RoleName); err != nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get user:%s role:%s, err:%s", user.Name, *user.RoleName, err.Error()))
		} else {
			userRole.Role = *role
		}

		users = append(users, userRole)
	}

	return users, err
}

func (s *UserService) UpdateUser(ctx context.Context, role *RoleService, user *entity.User, auth_user string) (err error) {
	mc := s.client.Master()
	bs, err := mc.Get(ctx, entity.UserKey(user.Name))
	if err != nil {
		return err
	}
	if bs == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_USER_NOT_EXIST, nil)
	}

	old_user := &entity.User{}
	err = json.Unmarshal(bs, old_user)
	if err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("updata user:%s err:%s", user.Name, err.Error()))
	}

	if user.RoleName != nil {
		if user.Password != nil || user.OldPassword != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("don't update role or password at same time"))
		}
		if _, err := role.QueryRole(ctx, *user.RoleName); err != nil {
			return err
		}
		if old_user.Password != nil {
			user.Password = old_user.Password
		}
	} else {
		if auth_user == entity.RootName && user.Name != entity.RootName {
			if user.Password == nil {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("empty password"))
			}
			if old_user.Password != nil && *user.Password == *old_user.Password {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("password is same with old password"))
			}
		} else {
			if user.Password == nil || user.OldPassword == nil {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("empty password or old password"))
			}
			if old_user.Password != nil && *user.Password == *old_user.Password {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("password is same with old password"))
			}
			if old_user.Password != nil && *user.OldPassword != *old_user.Password {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("old password is invalid"))
			}
		}

		if old_user.RoleName != nil {
			user.RoleName = old_user.RoleName
		}
	}

	// it will lock cluster
	mutex := mc.NewLock(ctx, entity.LockUserKey(user.Name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for update alias err %s", err)
		}
	}()
	err = mc.STM(context.Background(), func(stm concurrency.STM) error {
		marshal, err := json.Marshal(user)
		if err != nil {
			return err
		}
		stm.Put(entity.UserKey(user.Name), string(marshal))
		return nil
	})
	return nil
}
