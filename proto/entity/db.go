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
)

//db/id/[dbId]:[dbName]
//db/name/[dbName]:[dbId]
//db/body/[dbId]:[dbBody]
type DB struct {
	Id   DBID     `json:"id,omitempty"`
	Name string   `json:"name,omitempty"`
	Ps   []string `json:"ps,omitempty"` //if set this , your db only use you config ps
}

func (db *DB) Validate() error {
	rs := []rune(db.Name)

	if len(rs) == 0 {
		return fmt.Errorf("name can not set empty string")
	}

	if unicode.IsNumber(rs[0]) {
		return fmt.Errorf("name : %s can not start with num", db.Name)
	}

	if rs[0] == '_' {
		return fmt.Errorf("name : %s can not start with _", db.Name)
	}

	for _, r := range rs {
		switch r {
		case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0, '\\', '+', '-', '!', '*', '/', '(', ')', ':', '^', '[', ']', '"', '{', '}', '~', '%', '&', '\'', '<', '>', '?':
			return fmt.Errorf("name : %s can not has char in name ", `'\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0 , '\\','+', '-', '!', '*', '/', '(', ')', ':' , '^','[',']','"','{','}','~','%','&','\'','<','>','?'`)
		}
	}
	return nil
}
