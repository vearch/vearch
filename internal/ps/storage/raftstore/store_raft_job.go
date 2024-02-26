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
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/util/log"
)

const (
	TruncateTicket             = 5 * time.Minute
	FlushTicket                = 1 * time.Second
	DefaultFlushTimeInterval   = 600 // 10 minutes
	DefaultFlushCountThreshold = 200000
)

var fti int32 // flush time interval
var fct int32 // flush count threshold

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

	if config.Conf().PS.RaftTruncateCount <= 0 {
		config.Conf().PS.RaftTruncateCount = 100000
	}

	log.Info("start truncate job! truncate count: %d", config.Conf().PS.RaftTruncateCount)
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
			if (flushSn - appTruncateIndex - config.Conf().PS.RaftTruncateCount) > 0 {
				newTrucIndex := flushSn - config.Conf().PS.RaftTruncateCount
				if err = truncateFunc(newTrucIndex); err != nil {
					log.Warn("truncate: %s", err.Error())
					continue
				}
				log.Info("truncate raft success! current sn: %d, last sn:%d", newTrucIndex, appTruncateIndex)
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

		fti = int32(config.Conf().PS.FlushTimeInterval)
		if fti <= 0 {
			fti = DefaultFlushTimeInterval
		}
		fct = int32(config.Conf().PS.FlushCountThreshold)
		if fct <= 0 {
			fct = DefaultFlushCountThreshold
		}

		// init last min indexed num and doc num
		var engineStatus engine.EngineStatus
		s.Engine.EngineStatus(&engineStatus)
		lastIndexNum := engineStatus.MinIndexedNum
		lastMaxDocid := engineStatus.MaxDocid

		log.Info("start flush job, flush time interval=%d, count threshold=%d, min index num=%d, max docid=%d", fti, fct, lastIndexNum, lastMaxDocid)
		flushFunc := func() {
			if s.Sn == 0 {
				return
			}
			// counts condition
			if s.Engine == nil {
				log.Error("store is empty so stop flush job, dbID:[%d] space:[%d,%s] partitionID:[%d]",
					s.Space.DBId, s.Space.Id, s.Space.Name, s.Partition.Id)
				return
			}

			var status engine.EngineStatus
			s.Engine.EngineStatus(&status)
			t := time.Now()
			tempSn := s.Sn
			if t.Sub(s.LastFlushTime).Seconds() > float64(fti) && (tempSn-s.LastFlushSn > int64(fct) || status.MinIndexedNum-lastIndexNum > fct || status.MaxDocid-lastMaxDocid > fct) {
				log.Info("begin to flush, current time: %s, sn: %d, min indexed num=%d, max docid=%d",
					t.Format(time.RFC3339), tempSn, status.MinIndexedNum, status.MaxDocid)
				if err := s.Engine.Writer().Flush(s.Ctx, tempSn); err != nil {
					log.Error(err.Error())
					return
				}
				s.LastFlushSn = tempSn
				s.LastFlushTime = t
				lastIndexNum = status.MinIndexedNum
				lastMaxDocid = status.MaxDocid
			}
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
