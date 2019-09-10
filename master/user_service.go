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

package master

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vearch/vearch/proto/entity"
)

func NewUser(name, password, allowedHost string, userDB map[string]struct{}, privi entity.UserPrivi) *entity.User {
	return &entity.User{
		Name:        name,
		Password:    password,
		AllowedHost: allowedHost,
		UserDB:      userDB,
		Privi:       privi,
	}
}

func (ms *masterService) updateUser(user *entity.User) error {
	userVal, err := json.Marshal(user)
	if err != nil {
		return err
	}
	err = ms.Master().Update(context.Background(), entity.UserKey(user.Name), userVal)
	return err
}

func (ms *masterService) createUser(ctx context.Context, name, password, allowdHost, dblist, privilist string) (*entity.User, error) {
	user, _ := ms.queryUser(ctx, name)
	if user != nil {
		return nil, fmt.Errorf("duplicate user")
	}

	dbs := strings.Split(dblist, "|")
	userDB := make(map[string]struct{})

	for _, db := range dbs {
		userDB[db] = struct{}{}
	}

	privis := strings.Split(strings.ToLower(privilist), "|")
	var userPrivi entity.UserPrivi

	for _, privi := range privis {
		v, ok := entity.PriviMap[privi]
		if !ok {
			return nil, fmt.Errorf("invalid privi %s", privi)
		}
		userPrivi |= v
	}

	user = NewUser(name, password, allowdHost, userDB, userPrivi)

	userVal, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}
	err = ms.Master().Create(context.Background(), entity.UserKey(user.Name), userVal)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func (ms *masterService) queryUser(ctx context.Context, name string) (*entity.User, error) {
	return ms.Master().QueryUser(ctx, name)
}

func (ms *masterService) grantUserPriv(ctx context.Context, userName, privilist string) (*entity.User, error) {
	user, err := ms.queryUser(ctx, userName)
	if err != nil {
		return nil, err
	}

	privis := strings.Split(strings.ToLower(privilist), "|")

	for _, privi := range privis {
		v, ok := entity.PriviMap[privi]
		if !ok {
			return nil, fmt.Errorf("invalid privi %s", privi)
		}
		user.Privi |= v
	}

	if err := ms.updateUser(user); err != nil {
		return nil, err
	}
	return user, nil
}

func (ms *masterService) grantUserDB(ctx context.Context, userName, dblist string) (*entity.User, error) {
	user, err := ms.queryUser(ctx, userName)
	if user == nil {
		return nil, err
	}

	if user.UserDB == nil {
		user.UserDB = make(map[string]struct{})
	}

	dbs := strings.Split(dblist, "|")

	for _, db := range dbs {
		user.UserDB[db] = struct{}{}
	}

	if err := ms.updateUser(user); err != nil {
		return nil, err
	}
	return user, nil
}

func (ms *masterService) revokeUserPriv(ctx context.Context, userName, privilist string) (*entity.User, error) {
	if userName == "root" {
		return nil, fmt.Errorf("root privilege can not be revoked")
	}

	user, err := ms.queryUser(ctx, userName)
	if err != nil {
		return nil, err
	}

	privis := strings.Split(strings.ToLower(privilist), "|")
	for _, privi := range privis {
		v, ok := entity.PriviMap[privi]
		if !ok {
			return nil, fmt.Errorf("invalid privi %s", privi)
		}
		user.Privi &= ^v
	}

	if err := ms.updateUser(user); err != nil {
		return nil, err
	}
	return user, nil
}

func (ms *masterService) revokeUserDB(ctx context.Context, userName, dblist string) (*entity.User, error) {
	user, err := ms.queryUser(ctx, userName)
	if err != nil {
		return nil, err
	}

	if len(user.UserDB) == 0 {
		return user, nil
	}

	dbs := strings.Split(dblist, "|")
	for _, db := range dbs {
		delete(user.UserDB, db)
	}

	if err := ms.updateUser(user); err != nil {
		return nil, err
	}
	return user, nil
}

func (ms *masterService) updateUserPass(ctx context.Context, userName, passwd string) (*entity.User, error) {
	user, err := ms.queryUser(ctx, userName)
	if err != nil {
		return nil, fmt.Errorf("user %v not exist", userName)
	}
	user.Password = passwd

	if err := ms.updateUser(user); err != nil {
		return nil, err
	}
	return user, nil
}

func (ms *masterService) deleteUser(ctx context.Context, name string) (*entity.User, error) {
	user, err := ms.queryUser(ctx, name)
	if err != nil {
		return nil, err
	}
	if err = ms.Master().Delete(context.Background(), entity.UserKey(user.Name)); err != nil {
		return nil, err
	}
	return user, nil
}

func (ms *masterService) addUserDB(ctx context.Context, userName, dbName string) (*entity.User, error) {
	user, err := ms.queryUser(ctx, userName)
	if err != nil {
		return nil, err
	}

	user.UserDB[dbName] = struct{}{}

	if err := ms.updateUser(user); err != nil {
		return nil, err
	}

	return user, nil
}

func (ms *masterService) listUser() ([]*entity.User, error) {
	_, vals, err := ms.Master().PrefixScan(context.Background(), entity.PrefixUser)
	if err != nil {
		return nil, err
	}

	var users []*entity.User
	for _, userData := range vals {
		user := &entity.User{}
		err := json.Unmarshal(userData, user)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}
