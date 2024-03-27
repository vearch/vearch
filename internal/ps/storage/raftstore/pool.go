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

package raftstore

import (
	"sync"

	"github.com/vearch/vearch/internal/proto/vearchpb"
)

var (
	raftCmdPool = &sync.Pool{
		New: func() interface{} {
			return new(vearchpb.RaftCommand)
		},
	}
)

// CreateRaftCommand create a RaftCommand object
func CreateRaftCommand() *vearchpb.RaftCommand {
	cmd := raftCmdPool.Get().(*vearchpb.RaftCommand)
	cmd.UpdateSpace = nil
	cmd.WriteCommand = nil
	cmd.Type = 0
	return cmd
}

// Close reset and put to pool
func CloseRaftCommand(c *vearchpb.RaftCommand) {
	c.Type = 0
	c.WriteCommand = nil
	c.UpdateSpace = nil
	raftCmdPool.Put(c)
}
