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

package raftpb

import (
	"sync"
)

var (
	raftCmdPool = &sync.Pool{
		New: func() interface{} {
			return new(RaftCommand)
		},
	}

	snapDataPool = &sync.Pool{
		New: func() interface{} {
			return new(SnapData)
		},
	}
)

// CreateRaftCommand create a RaftCommand object
func CreateRaftCommand() *RaftCommand {
	cmd := raftCmdPool.Get().(*RaftCommand)
	cmd.UpdateSpace = nil
	cmd.WriteCommand = nil
	cmd.Type = 0
	return cmd
}

// Close reset and put to pool
func (c *RaftCommand) Close() error {
	c.Type = 0
	c.WriteCommand = nil
	c.UpdateSpace = nil
	raftCmdPool.Put(c)
	return nil
}
