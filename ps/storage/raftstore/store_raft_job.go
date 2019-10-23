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
	"fmt"
	"runtime/debug"
	"time"

	"github.com/spf13/cast"
	"github.com/tiglabs/log"
)

const (
	TruncateTicket = 5 * time.Minute
	TruncateCounts = 20000000

	FlushTicket  = 1 * time.Second
)

// truncate is raft log truncate.
func (s *Store) startTruncateJob(initLastFlushIndex int64) {
	truncateFunc := func(truncIndex int64) error {
		// get raft peers status
		snapPeers := s.RaftServer.GetPendingReplica(uint64(s.Partition.Id))
		if len(snapPeers) > 0 {
			return fmt.Errorf("peer %v is snapShot", snapPeers)
		}
		s.RaftServer.Truncate(uint64(s.Partition.Id), uint64(truncIndex))
		return nil
	}

	appTruncateIndex := initLastFlushIndex
	go func() {
		defer func() {
			if i := recover(); i != nil {
				log.Error(string(debug.Stack()))
				log.Error(cast.ToString(i))
			}
		}()
		for {
			time.Sleep(TruncateTicket)
			select {
			case <-s.Ctx.Done():
				return
			default:
			}
			// counts condition
			if s.Engine == nil {
				log.Error("store is empty so stop truncate job, dbID:[%d] space:[%d,%s] partitionID:[%d]", s.Space.DBId, s.Space.Id, s.Space.Name, s.Partition.Id)
				return
			}

			flushSn, err := s.Engine.Reader().ReadSN(s.Ctx)
			if err != nil {
				log.Error("truncate getsn: %s", err.Error())
				continue
			}
			if (flushSn - appTruncateIndex - TruncateCounts) >= 0 {
				newTrucIndex := flushSn - TruncateCounts
				if err = truncateFunc(newTrucIndex); err != nil {
					log.Warn("truncate: %s", err.Error())
					continue
				}
				appTruncateIndex = newTrucIndex
				continue
			}
		}
	}()
}

// start flush job
func (s *Store) startFlushJob() {
	go func() {
		defer func() {
			if i := recover(); i != nil {
				log.Error(string(debug.Stack()))
				log.Error(cast.ToString(i))
			}
		}()

		flushFunc := func() {
			if s.Sn == 0 {
				return
			}
			tempSn := s.Sn
			// counts condition
			if s.Engine == nil {
				log.Error("store is empty so stop flush job, dbID:[%d] space:[%d,%s] partitionID:[%d]", s.Space.DBId, s.Space.Id, s.Space.Name, s.Partition.Id)
				return
			}
			if tempSn-s.LastFlushSn < 20000 {
				return
			}

			if err := s.Engine.Writer().Flush(s.Ctx, tempSn); err != nil {
				log.Error(err.Error())
				return
			}
			s.LastFlushSn = tempSn
		}

		ticker := time.NewTicker(FlushTicket)
		for {
			select {
			case <-s.Ctx.Done():
				return
			case <-ticker.C:
				flushFunc()
			}
		}
	}()
}

