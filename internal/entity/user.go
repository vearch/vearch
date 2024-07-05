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

package entity

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

type PrivilegeType uint64

type Privilege string

const (
	None      Privilege = "None"
	WriteOnly Privilege = "WriteOnly"
	ReadOnly  Privilege = "ReadOnly"
	WriteRead Privilege = "WriteRead"
)

var PriviMap = map[Privilege]string{
	None:      "None",
	WriteOnly: "WriteOnly",
	ReadOnly:  "ReadOnly",
	WriteRead: "WriteRead",
}

type Resource string

const (
	ResourceAll       Resource = "ResourceAll"
	ResourceCluster   Resource = "ResourceCluster"
	ResourceServer    Resource = "ResourceServer"
	ResourcePartition Resource = "ResourcePartition"
	ResourceDB        Resource = "ResourceDB"
	ResourceSpace     Resource = "ResourceSpace"
	ResourceDocument  Resource = "ResourceDocument"
	ResourceIndex     Resource = "ResourceIndex"
	ResourceAlias     Resource = "ResourceAlias"
	ResourceUser      Resource = "ResourceUser"
	ResourceRole      Resource = "ResourceRole"
	ResourceConfig    Resource = "ResourceConfig"
	ResourceCache     Resource = "ResourceCache"
)

var ResourceMap = map[Resource]string{
	ResourceAll:       "ResourceAll",
	ResourceCluster:   "ResourceCluster",
	ResourceServer:    "ResourceServer",
	ResourcePartition: "ResourcePartition",
	ResourceDB:        "ResourceDB",
	ResourceSpace:     "ResourceSpace",
	ResourceDocument:  "ResourceDocument",
	ResourceIndex:     "ResourceIndex",
	ResourceAlias:     "ResourceAlias",
	ResourceUser:      "ResourceUser",
	ResourceRole:      "ResourceRole",
	ResourceConfig:    "ResourceConfig",
	ResourceCache:     "ResourceCache",
}

type OperatorType string

const (
	Grant  OperatorType = "Grant"
	Revoke OperatorType = "Revoke"
)

type Role struct {
	Name       string                 `json:"name,omitempty"`
	Operator   OperatorType           `json:"operator,omitempty"`
	Privileges map[Resource]Privilege `json:"privileges,omitempty"`
}

var RootPrivilege = map[Resource]Privilege{
	"ResourceAll": WriteRead,
}

var ClusterPrivilege = map[Resource]Privilege{
	"ResourceCluster":   WriteRead,
	"ResourceServer":    WriteRead,
	"ResourcePartition": WriteRead,
	"ResourceDB":        WriteRead,
	"ResourceSpace":     WriteRead,
	"ResourceDocument":  WriteRead,
	"ResourceIndex":     WriteRead,
	"ResourceAlias":     WriteRead,
	"ResourceConfig":    WriteRead,
	"ResourceUser":      WriteRead,
	"ResourceRole":      WriteRead,
}

var SpaceAdminPrivilege = map[Resource]Privilege{
	"ResourceSpace":    WriteRead,
	"ResourceDocument": WriteRead,
	"ResourceIndex":    WriteRead,
	"ResourceAlias":    ReadOnly,
}

var ReadDBSpaceEditDocumentPrivilege = map[Resource]Privilege{
	"ResourceCluster":  ReadOnly,
	"ResourceDB":       ReadOnly,
	"ResourceSpace":    ReadOnly,
	"ResourceDocument": WriteRead,
	"ResourceIndex":    WriteRead,
	"ResourceAlias":    ReadOnly,
}

var ReadSpaceEditDocumentPrivilege = map[Resource]Privilege{
	"ResourceSpace":    ReadOnly,
	"ResourceDocument": WriteRead,
	"ResourceIndex":    WriteRead,
	"ResourceAlias":    ReadOnly,
}

var DocumentAdminPrivilege = map[Resource]Privilege{
	"ResourceDocument": WriteRead,
	"ResourceIndex":    WriteRead,
}

var DocumentReadPrivilege = map[Resource]Privilege{
	"ResourceDocument": ReadOnly,
	"ResourceIndex":    ReadOnly,
}

var RootRole = Role{Name: "root", Privileges: RootPrivilege}
var ClusterAdminRole = Role{Name: "defaultClusterAdmin", Privileges: ClusterPrivilege}
var SpaceAdminRole = Role{Name: "defaultSpaceAdmin", Privileges: SpaceAdminPrivilege}
var DocumentAdminRole = Role{Name: "defaultDocumentAdmin", Privileges: DocumentAdminPrivilege}
var ReadDBSpaceEditDocumentRole = Role{Name: "defaultReadDBSpaceEditDocument", Privileges: ReadDBSpaceEditDocumentPrivilege}
var ReadSpaceEditDocumentRole = Role{Name: "defaultReadSpaceEditDocument", Privileges: ReadSpaceEditDocumentPrivilege}

var RoleMap = map[string]Role{
	"root":                           RootRole,
	"defaultClusterAdmin":            ClusterAdminRole,
	"defaultSpaceAdmin":              SpaceAdminRole,
	"defaultDocumentAdmin":           DocumentAdminRole,
	"defaultReadDBSpaceEditDocument": ReadDBSpaceEditDocumentRole,
	"defaultReadSpaceEditDocument":   ReadSpaceEditDocumentRole,
}

type NameType string

const (
	RoleNameType NameType = "Role"
	UserNameType NameType = "User"
)

func ValidateName(name string, name_type NameType, check_root bool) error {
	// validate name
	rs := []rune(name)
	if len(rs) == 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("%s name can not be empty string", name_type))
	}
	if unicode.IsNumber(rs[0]) {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("%s name : %s can not start with num", name_type, name))
	}
	if rs[0] == '_' {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("%s name : %s can not start with _", name_type, name))
	}
	for _, r := range rs {
		switch r {
		case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '\\', '+', '-', '!', '*', '/', '(', ')', ':', '^', '[', ']', '"', '{', '}', '~', '%', '&', '\'', '<', '>', '?':
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("character '%c' can not in %s name[%s]", r, name_type, name))
		}
	}
	if check_root {
		if strings.EqualFold(name, "root") {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("%s name can't be root", name_type))
		}
	}

	return nil
}

func ParseResources(endpoint string, method string) (resource Resource, privilege Privilege) {
	switch method {
	case "GET":
		privilege = ReadOnly
	default:
		privilege = WriteOnly
	}

	resource = ResourceAll
	if strings.HasPrefix(endpoint, "/cluster") {
		resource = ResourceCluster
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/servers") {
		resource = ResourceServer
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/partitions") {
		resource = ResourcePartition
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/dbs") {
		resource = ResourceDB
		if strings.Contains(endpoint, "/spaces") {
			resource = ResourceSpace
		}
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/backup") {
		resource = ResourceSpace
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/document") {
		resource = ResourceDocument
		if strings.Contains(endpoint, "query") || strings.Contains(endpoint, "search") {
			privilege = ReadOnly
		} else {
			privilege = WriteOnly
		}
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/index") {
		resource = ResourceIndex
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/alias") {
		resource = ResourceAlias
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/config") {
		resource = ResourceConfig
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/users") {
		resource = ResourceUser
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/roles") {
		resource = ResourceRole
		return resource, privilege
	}

	if strings.HasPrefix(endpoint, "/cache") {
		resource = ResourceCache
		return resource, privilege
	}

	return resource, privilege
}

func (role *Role) Validate() error {
	if err := ValidateName(role.Name, RoleNameType, true); err != nil {
		return err
	}
	if role.Operator != "" && role.Operator != Grant && role.Operator != Revoke {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("role privilege operator type : %s, should be %s or %s", role.Operator, Grant, Revoke))
	}
	for resource, privilege := range role.Privileges {
		if _, exists := ResourceMap[resource]; !exists {
			keys := make([]Resource, 0, len(ResourceMap))
			for k := range ResourceMap {
				keys = append(keys, k)
			}
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("role resource: %s, should be %v", resource, keys))
		} else {
			if _, exists := PriviMap[privilege]; !exists {
				keys := make([]Privilege, 0, len(PriviMap))
				for k := range PriviMap {
					keys = append(keys, k)
				}
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("role privilege: %s, should be %v", privilege, keys))
			}
		}
	}
	return nil
}

const RootName = "root"

func (role *Role) HasPermissionForResources(endpoint string, method string) error {
	if role.Name == RootName {
		return nil
	}
	resource, privilege := ParseResources(endpoint, method)
	if value, ok := role.Privileges[resource]; ok {
		if privilege == value || value == WriteRead {
			return nil
		}
	} else {
		return vearchpb.NewError(vearchpb.ErrorEnum_AUTHENTICATION_FAILED, fmt.Errorf("role:%s don't have privilege for resource: %s", role.Name, resource))
	}
	return nil
}

type User struct {
	Name        string  `json:"name"`
	Password    *string `json:"password,omitempty"`
	OldPassword *string `json:"old_password,omitempty"`
	RoleName    *string `json:"role_name,omitempty"`
}

type UserRole struct {
	Name        string  `json:"name"`
	Password    *string `json:"password,omitempty"`
	OldPassword *string `json:"old_password,omitempty"`
	Role        Role    `json:"role,omitempty"`
}

func (user *User) Validate(check_root bool) error {
	// validate user name
	if err := ValidateName(user.Name, UserNameType, check_root); err != nil {
		return err
	}
	if user.Password != nil && *(user.Password) == "" {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("user password is empty"))
	}

	if check_root {
		if user.RoleName != nil && strings.EqualFold(*user.RoleName, RootName) {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("user role name can't be root"))
		}
	}

	return nil
}

func HasPrivi(userPrivi PrivilegeType, checkPrivi PrivilegeType) bool {
	return (userPrivi & checkPrivi) == checkPrivi
}

func LackPrivi(userPrivi PrivilegeType, checkPrivi PrivilegeType) PrivilegeType {
	return checkPrivi &^ userPrivi
}
