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
			log.Info("currrent sn=%d", s.CurrentSn)
			if s.CurrentSn == 0 {
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
			tempSn := s.CurrentSn
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
