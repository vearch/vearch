// Copyright 2016 The Cockroach Authors.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package debugutil

import (
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"

	"github.com/vearch/vearch/v3/internal/debugutil/pprofui"
	"github.com/vearch/vearch/v3/internal/pkg/log"
)

// Server serves the /debug/* family of tools.
type Server struct {
	mux *http.ServeMux
}

// NewServer sets up a debug server.
func NewServer() *Server {
	mux := http.NewServeMux()

	// Cribbed straight from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) {
		CPUProfileHandler(w, r)
	})
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ps := pprofui.NewServer(pprofui.NewMemStorage(1, 0), func(profile string, labels bool, do func()) {
		if profile != "profile" {
			do()
			return
		}

		if err := CPUProfileDo(CPUProfileOptions{WithLabels: labels}.Type(), func() error {
			var extra string
			if labels {
				extra = " (enabling profiler labels)"
			}
			log.Info("pprofui: recording %v%v", profile, extra)
			do()
			return nil
		}); err != nil {
			// NB: we don't have good error handling here. Could be changed if we find
			// this problematic. In practice, `do()` wraps the pprof handler which will
			// return an error if there's already a profile going on just the same.
			return
		}
	})
	mux.Handle("/debug/pprof/ui/", http.StripPrefix("/debug/pprof/ui", ps))

	mux.HandleFunc("/perf/profile", CPUProfile)
	mux.HandleFunc("/perf/heap", HeapProfile)

	return &Server{
		mux: mux,
	}
}

// ServeHTTP serves various tools under the /debug endpoint. It restricts access
// according to the `server.remote_debugging.mode` cluster variable.
func (ds *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler, _ := ds.mux.Handler(r)
	handler.ServeHTTP(w, r)
}

// http://127.0.0.1/debug/pprof/ui/
// http://127.0.0.1/debug/pprof/ui/profile
// http://127.0.0.1/debug/pprof/ui/heap

func StartUIPprofListener(port int) {
	pprofServer := NewServer()
	address := strings.Join([]string{"", strconv.Itoa(port)}, ":")
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Error("StartUIPprofListener start error: %v", err.Error())
		return
	}

	srvhttp := &http.Server{
		Handler: pprofServer.mux,
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("start pprof server error: %v", r)
			}
		}()

		err = srvhttp.Serve(listener)
		if err != nil {
			log.Error("srvhttp.Serve error: %v", err.Error())
		}
	}()
}
