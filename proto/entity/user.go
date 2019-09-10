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

import "bytes"

type User struct {
	Name        string
	Password    string
	AllowedHost string
	Privi       UserPrivi
	UserDB      map[string]struct{}
	HeadKey     string
}

type UserPrivi uint64

const (
	PrivilegeNone     UserPrivi = 0
	PrivilegeSelect   UserPrivi = 1
	PrivilegeInsert   UserPrivi = 1 << 1
	PrivilegeUpdate   UserPrivi = 1 << 2
	PrivilegeDelete   UserPrivi = 1 << 3
	PrivilegeCreate   UserPrivi = 1 << 4
	PrivilegeAlter    UserPrivi = 1 << 5
	PrivilegeDrop     UserPrivi = 1 << 6
	PrivilegeTruncate UserPrivi = 1 << 7
	PrivilegeGrant    UserPrivi = 1 << 8
)

var PriviMap = map[string]UserPrivi{
	"select": PrivilegeSelect,
	"insert": PrivilegeInsert,
	"update": PrivilegeUpdate,
	"delete": PrivilegeDelete,

	"create":   PrivilegeCreate,
	"alter":    PrivilegeAlter,
	"drop":     PrivilegeDrop,
	"truncate": PrivilegeTruncate,

	"grant": PrivilegeGrant,
}

func HasPrivi(userPrivi UserPrivi, checkPrivi UserPrivi) bool {
	return (userPrivi & checkPrivi) == checkPrivi
}

func LackPrivi(userPrivi UserPrivi, checkPrivi UserPrivi) UserPrivi {
	return checkPrivi &^ userPrivi
}

func (userPrivi UserPrivi) String() string {
	var prefix string
	var b bytes.Buffer

	for privDesc, priv := range PriviMap {
		if HasPrivi(userPrivi, priv) {
			b.WriteString(prefix)
			b.WriteString(privDesc)
			prefix = "|"
		}
	}

	return b.String()
}
