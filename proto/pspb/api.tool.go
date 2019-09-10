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

package pspb

import (
	"sync"
)

var pool = sync.Pool{
	New: func() interface{} {
		return &DocCmd{}
	},
}

func GetDocCmd() *DocCmd {
	cmd := pool.Get().(*DocCmd)
	clean(cmd)
	return cmd
}

func PutDocCmd(doc *DocCmd) {
	clean(doc)
	pool.Put(doc)
}

func clean(doc *DocCmd) {
	doc.Type = OpType_NOOP
	doc.DocId = ""
	doc.Version = 0
	doc.Slot = 0
	doc.Source = doc.Source[:0]
	doc.Fields = doc.Fields[:0]
	doc.PulloutVersion = false
}

func NewDocCmd(ot OpType, id string, slot uint32, source []byte, version int64) *DocCmd {
	doc := GetDocCmd()
	doc.Type = ot
	doc.DocId = id
	doc.Source = source
	doc.Slot = slot
	doc.Version = version
	return doc
}

func NewDocCreateWithSlot(id string, slot uint32, source []byte) *DocCmd {
	doc := GetDocCmd()
	doc.Type = OpType_CREATE
	doc.DocId = id
	doc.Source = source
	doc.Slot = slot
	return doc
}

// if version is zero  update version++
func NewDocMergeWithSlot(id string, slot uint32, source []byte, version int64) *DocCmd {
	doc := GetDocCmd()
	doc.Type = OpType_MERGE
	doc.DocId = id
	doc.Source = source
	doc.Slot = slot
	doc.Version = version
	return doc
}

// if version == -1 , over write and not check version , if version == 0 , update version++
func NewDocReplaceWithSlot(id string, slot uint32, source []byte, version int64) *DocCmd {
	doc := GetDocCmd()
	doc.Type = OpType_REPLACE
	doc.DocId = id
	doc.Source = source
	doc.Slot = slot
	doc.Version = version
	return doc
}

// f version == 0 , delete not check version
func NewDocDeleteWithSlot(id string, slot uint32, version int64) *DocCmd {
	doc := GetDocCmd()
	doc.Type = OpType_DELETE
	doc.DocId = id
	doc.Slot = slot
	doc.Version = version
	return doc
}
