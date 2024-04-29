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
	"unicode"

	"github.com/vearch/vearch/internal/proto/vearchpb"
)

// alias/dbName/spaceName
type Alias struct {
	Name      string `json:"name,omitempty"`
	DbName    string `json:"db_name,omitempty"`
	SpaceName string `json:"space_name,omitempty"`
}

func (alias *Alias) Validate() error {
	// validate db name
	rs := []rune(alias.Name)
	if len(rs) == 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("alias name can not be empty string"))
	}
	if unicode.IsNumber(rs[0]) {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("alias name : %s can not start with num", alias.Name))
	}
	if rs[0] == '_' {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("alias name : %s can not start with _", alias.Name))
	}
	for _, r := range rs {
		switch r {
		case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '\\', '+', '-', '!', '*', '/', '(', ')', ':', '^', '[', ']', '"', '{', '}', '~', '%', '&', '\'', '<', '>', '?':
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("character '%c' can not in db name[%s]", r, alias.Name))
		}
	}
	return nil
}
