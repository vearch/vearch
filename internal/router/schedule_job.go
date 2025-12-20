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

	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
)

const (
	KeepAliveTime     = 10
	MaxRetries        = 5
	InitialBackoff    = 1 * time.Second
	MaxBackoff        = 30 * time.Second
	BackoffMultiplier = 2.0
)

// this job for heartbeat master 1m once
func (s *Server) StartHeartbeatJob(addr string) {
	go func() {
		var key string = config.Conf().Global.Name
		retries := 0
		backoff := InitialBackoff

		log.Info("Starting heartbeat job, key: [%s], routerIP: [%s]", key, addr)
		keepaliveC, err := s.cli.Master().Store.KeepAlive(s.ctx, entity.RouterKey(key, addr), []byte(addr), time.Second*KeepAliveTime)
		if err != nil {
			log.Error("Initial KeepAlive failed: %s", err.Error())
			return
		}

		for {
			select {
			case <-s.ctx.Done():
				log.Info("Heartbeat job stopped by context")
				return
			case ka, ok := <-keepaliveC:
				if !ok {
					log.Warn("Keep alive channel closed, attempting to reconnect...")

					// Check max retries
					if retries >= MaxRetries {
						log.Error("Max retries (%d) reached, stopping heartbeat job", MaxRetries)
						return
					}

					// Exponential backoff
					time.Sleep(backoff)

					keepaliveC, err = s.cli.Master().Store.KeepAlive(s.ctx, entity.RouterKey(key, addr), []byte(addr), time.Second*KeepAliveTime)
					if err != nil {
						log.Error("KeepAlive reconnection failed (attempt %d/%d): %s", retries+1, MaxRetries, err.Error())
						retries++
						backoff = time.Duration(float64(backoff) * BackoffMultiplier)
						if backoff > MaxBackoff {
							backoff = MaxBackoff
						}
						continue
					}

					// Reconnection successful, reset counters
					log.Info("KeepAlive reconnected successfully")
					retries = 0
					backoff = InitialBackoff
					continue
				}

				// Normal keepalive response
				log.Debugf("Received keepalive, leaseId: %d, ttl:%d", ka.ID, ka.TTL)
				retries = 0 // Reset failure counter on successful keepalive
			}
		}
	}()
}
