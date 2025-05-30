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

	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vmihailenco/msgpack"
	"google.golang.org/protobuf/proto"
)

type MsgpackCodec struct {
}

func (c *MsgpackCodec) Decode(data []byte, i any) error {
	defer func() {
		if r := recover(); r != nil {
			log.Error("msgpack decode error: %v", r)
		}
	}()
	return msgpack.NewDecoder(bytes.NewBuffer(data)).UseJSONTag(true).Decode(i)
}

func (c *MsgpackCodec) Encode(i any) ([]byte, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("msgpack encode error: %v", r)
		}
	}()
	var buf bytes.Buffer
	err := msgpack.NewEncoder(&buf).UseCompactEncoding(true).UseJSONTag(true).Encode(i)
	return buf.Bytes(), err

}

// PBCodec uses protobuf marshaler and unmarshaler.
type PBCodec struct{}

// Encode encodes an object into slice of bytes.
func (c PBCodec) Encode(i any) ([]byte, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("pbcodec encode error: %v", r)
		}
	}()

	if m, ok := i.(proto.Message); ok {
		return proto.Marshal(m)
	}

	var buf bytes.Buffer
	err := msgpack.NewEncoder(&buf).UseCompactEncoding(true).UseJSONTag(true).Encode(i)
	return buf.Bytes(), err
}

// Decode decodes an object from slice of bytes.
func (c PBCodec) Decode(data []byte, i any) error {
	defer func() {
		if r := recover(); r != nil {
			log.Error("pbcodec decode error: %v", r)
		}
	}()

	if m, ok := i.(proto.Message); ok {
		return proto.Unmarshal(data, m)
	}

	return msgpack.NewDecoder(bytes.NewBuffer(data)).UseJSONTag(true).Decode(i)
}
