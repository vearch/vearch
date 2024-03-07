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
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/vearch/vearch/internal/pkg/errutil"
	"github.com/vearch/vearch/internal/pkg/log"
)

// Snapshot implements the raft interface.
func (s *Store) Snapshot() (proto.Snapshot, error) {
	return s.GetEngine().NewSnapshot()
}

// ApplySnapshot implements the raft interface.
func (s *Store) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) (err error) {
	defer errutil.CatchError(&err)
	s.Engine.Close()
	log.Debug("close engine")
	i := 0
	// wait engine close
	for {
		if s.Engine.HasClosed() {
			break
		}
		time.Sleep(1 * time.Second)
		i++
		log.Debug("wait stop engine times:[%d]", i)
	}
	log.Debug("engine has stop, begin remove engine data.")
	// remove engine data dir
	err = s.RemoveDataPath()
	errutil.ThrowError(err)
	log.Debug("remove engine data path")
	// apply snapshot
	err = s.GetEngine().ApplySnapshot(peers, iter)
	if err == nil {
		log.Debug("store info is [%+v]", s)
	} else {
		errutil.ThrowError(err)
	}
	err = s.ReBuildEngine()
	log.Debug("rebuild engine after store info is [%+v]", s)
	errutil.ThrowError(err)
	return err
}
