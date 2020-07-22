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

package signals

import (
	"github.com/vearch/vearch/util/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type SignalHook struct {
	sigsC chan os.Signal
	hooks []func()
	stopC chan interface{}
}

func NewSignalHook() *SignalHook {
	s := &SignalHook{
		sigsC: make(chan os.Signal),
		hooks: make([]func(), 0),
		stopC: make(chan interface{}, 1),
	}
	// signal.Notify(s.sigsC, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)
	signal.Notify(s.sigsC, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

	return s
}

func (s *SignalHook) AddSignalHook(f func()) {
	s.hooks = append(s.hooks, f)
}

func (s *SignalHook) WaitSignals() os.Signal {
	log.Info("Wait Signals...")
	sig := <-s.sigsC
	log.Info("Signal received: %s", sig.String())
	return sig
}

func (s *SignalHook) AsyncInvokeHooks() {
	go func() {
		for _, f := range s.hooks {
			f()
		}
		s.stopC <- struct{}{}
	}()
}

func (s *SignalHook) WaitUntilTimeout(d time.Duration) {
	select {
	case <-s.stopC:
		log.Info("All signal hooks had finish, server graceful soft shutdown...")
		return
	case <-s.sigsC:
		log.Info("Second signal received, server hard shutdown...")
		return
	case <-time.After(d):
		log.Info("Signal time limit reached, server hard shutdown...")
	}
}
