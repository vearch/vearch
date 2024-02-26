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

package netutil

import (
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	rpprof "runtime/pprof"

	"github.com/vearch/vearch/internal/util/gogc"
	"github.com/vearch/vearch/internal/util/log"
)

func StartPprofService(addr string) (*http.ServeMux, error) {
	pprofMux := http.NewServeMux()
	pprofMux.HandleFunc("/debug/ping", PingPong)
	pprofMux.HandleFunc("/debug/pprof", DebugPprofHandler)
	pprofMux.HandleFunc("/debug/pprof/cmdline", DebugPprofCmdlineHandler)
	pprofMux.HandleFunc("/debug/pprof/profile", DebugPprofProfileHandler)
	pprofMux.HandleFunc("/debug/pprof/symbol", DebugPprofSymbolHandler)
	pprofMux.HandleFunc("/debug/pprof/trace", DebugPprofTraceHandler)
	pprofMux.HandleFunc("/debug/gc", GCHandler)
	pprofMux.HandleFunc("/debug/pprof/heap", DebugPprofHeapHandler)
	pprofMux.HandleFunc("/debug/pprof/mutex", DebugPprofMutexHandler)
	pprofMux.HandleFunc("/debug/pprof/goroutine", DebugPprofGoroutineHandler)
	pprofMux.HandleFunc("/debug/pprof/block", DebugPprofBlockHandler)
	pprofMux.HandleFunc("/debug/pprof/threadcreate", DebugPprofThreadcreateHandler)
	if err := http.ListenAndServe(addr, pprofMux); err != nil {
		return nil, err
	}
	return pprofMux, nil
}
func PingPong(w http.ResponseWriter, _ *http.Request) {
	w.Write([]byte("ok"))
	return
}

func DebugPprofHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("vearch-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Index(ww, r)
}

func DebugPprofCmdlineHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("vearch-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Cmdline(ww, r)
}

func DebugPprofProfileHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("vearch-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Profile(ww, r)
}

func DebugPprofSymbolHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("vearch-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Symbol(ww, r)
}

func DebugPprofTraceHandler(w http.ResponseWriter, r *http.Request) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("vearch-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)
	pprof.Trace(ww, r)
}

func GCHandler(w http.ResponseWriter, _ *http.Request) {
	gogc.PrintGCSummary(w)
	return
}

func DebugPprofHeapHandler(w http.ResponseWriter, r *http.Request) {
	debugPprofLookupHandler(w, r, "heap")
}

func DebugPprofGoroutineHandler(w http.ResponseWriter, r *http.Request) {
	debugPprofLookupHandler(w, r, "goroutine")
}

func DebugPprofThreadcreateHandler(w http.ResponseWriter, r *http.Request) {
	debugPprofLookupHandler(w, r, "threadcreate")
}

func DebugPprofBlockHandler(w http.ResponseWriter, r *http.Request) {
	runtime.SetBlockProfileRate(1000 * 1000)
	debugPprofLookupHandler(w, r, "block")
}

func DebugPprofMutexHandler(w http.ResponseWriter, r *http.Request) {
	runtime.SetMutexProfileFraction(1)
	debugPprofLookupHandler(w, r, "mutex")
}

func debugPprofLookupHandler(w http.ResponseWriter, r *http.Request, name string) {
	var output io.Writer = w
	if file := r.FormValue("file"); file != "" {
		f, err := os.Create(file)
		if err != nil {
			log.Error("vearch-debug: create file failed(%v), path=%v", err, file)
			return
		}
		defer f.Close()
		output = f
	}
	ww := NewResponseWriter(w, output)

	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="profile"`)
	p := rpprof.Lookup(name)
	if p == nil {
		w.Write([]byte("heap profile is not supported"))
		return
	}

	p.WriteTo(ww, 2)
}
