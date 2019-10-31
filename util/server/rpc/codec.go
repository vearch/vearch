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

package server

import (
	"bytes"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/util/log"
	"github.com/vmihailenco/msgpack"
)

type MsgpackCodec struct {
}

func (c *MsgpackCodec) Decode(data []byte, i interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			log.Error(cast.ToString(r))
		}
	}()
	return msgpack.NewDecoder(bytes.NewBuffer(data)).UseJSONTag(true).Decode(i)
}

func (c *MsgpackCodec) Encode(i interface{}) ([]byte, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error(cast.ToString(r))
		}
	}()
	var buf bytes.Buffer
	err := msgpack.NewEncoder(&buf).UseCompactEncoding(true).UseJSONTag(true).Encode(i)
	return buf.Bytes(), err

}
