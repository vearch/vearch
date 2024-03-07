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

package router

import (
	"time"

	"github.com/vearch/vearch/internal/config"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/pkg/log"
)

const KeepAliveTime = 10

// this job for heartbeat master 1m once
func (s *Server) StartHeartbeatJob(addr string) {
	go func() {
		var key string = config.Conf().Global.Name

		log.Debugf("register key: [%s], routerIP: [%s]", key, addr)
		keepaliveC, err := s.cli.Master().Store.KeepAlive(s.ctx, entity.RouterKey(key, addr), []byte(addr), time.Second*KeepAliveTime)
		if err != nil {
			log.Error("KeepAlive err: %s", err.Error())
			return
		}

		for {
			select {
			case <-s.ctx.Done():
				log.Error("keep alive ctx done!")
				return
			case ka, ok := <-keepaliveC:
				if !ok {
					log.Error("keep alive channel closed!")
					time.Sleep(2 * time.Second)
					keepaliveC, err = s.cli.Master().Store.KeepAlive(s.ctx, entity.RouterKey(key, addr), []byte(addr), time.Second*KeepAliveTime)
					if err != nil {
						log.Errorf("KeepAlive err: %s", err.Error())
					}
					continue
				}
				log.Debugf("Receive keepalive, leaseId: %d, ttl:%d", ka.ID, ka.TTL)
			}
		}
	}()
}
