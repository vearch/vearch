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
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/master/services"
)

// masterService is used for master administrator purpose. It should not used by router or partition server program
type masterService struct {
	*client.Client
	dbService        *services.DBService
	spaceService     *services.SpaceService
	aliasService     *services.AliasService
	serverService    *services.ServerService
	userService      *services.UserService
	roleService      *services.RoleService
	partitionService *services.PartitionService
	memberService    *services.MemberService
	configService    *services.ConfigService
	backupService    *services.BackupService
}

func newMasterService(client *client.Client) (*masterService, error) {
	return &masterService{
		Client:           client,
		dbService:        services.NewDBService(client),
		spaceService:     services.NewSpaceService(client),
		aliasService:     services.NewAliasService(client),
		serverService:    services.NewServerService(client),
		userService:      services.NewUserService(client),
		roleService:      services.NewRoleService(client),
		partitionService: services.NewPartitionService(client),
		memberService:    services.NewMemberService(client),
		configService:    services.NewConfigService(client),
		backupService:    services.NewBackupService(client),
	}, nil
}

func (ms *masterService) DB() *services.DBService {
	return ms.dbService
}

func (ms *masterService) Space() *services.SpaceService {
	return ms.spaceService
}

func (ms *masterService) Alias() *services.AliasService {
	return ms.aliasService
}

func (ms *masterService) Server() *services.ServerService {
	return ms.serverService
}

func (ms *masterService) User() *services.UserService {
	return ms.userService
}

func (ms *masterService) Role() *services.RoleService {
	return ms.roleService
}

func (ms *masterService) Partition() *services.PartitionService {
	return ms.partitionService
}

func (ms *masterService) Member() *services.MemberService {
	return ms.memberService
}

func (ms *masterService) Config() *services.ConfigService {
	return ms.configService
}

func (ms *masterService) Backup() *services.BackupService {
	return ms.backupService
}
