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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/julienschmidt/httprouter"
	"github.com/tiglabs/log"
	ratelimit2 "github.com/vearch/vearch/util/ratelimit"
	"golang.org/x/net/netutil"
)

const (
	RouterModeHttpRouter RouterMode = "httprouter"
	RouterModeGorilla    RouterMode = "gorilla"
	StartTime                       = "__start_time"
)

type RouterMode string

var (
	routerMode = RouterModeHttpRouter
)

type ServerConfig struct {
	Name         string
	Addr         string // ip:port
	Version      string
	ConnLimit    int
	CloseTimeout time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	RateLimit    ratelimit2.RateLimit
}

// Server is a http server
type Server struct {
	cfg       *ServerConfig
	server    *http.Server
	router    http.Handler
	rateLimit ratelimit2.RateLimit
	closed    int64
}

// NewUriServer creates the server with uri parsing
func NewServer(config *ServerConfig) *Server {
	s := &Server{
		cfg: config,
	}

	s.createRouter()
	s.server = &http.Server{
		Handler: s,
	}

	return s
}

func SetMode(mode RouterMode) {
	routerMode = mode
}

type UriParams interface {
	ByName(name string) string
	Put(name string, value string)
}

type UriParamsMap struct {
	values map[string]string
}

func (p *UriParamsMap) Put(name, value string) {
	p.values[name] = value
}

func (p *UriParamsMap) ByName(name string) string {
	if len(name) == 0 {
		return ""
	}
	v, ok := p.values[name]
	if !ok {
		return ""
	} else {
		return v
	}
}

func NewMockUriParams(values map[string]string) UriParams {
	return &UriParamsMap{
		values: values,
	}
}

type Handle func(http.ResponseWriter, *http.Request, UriParams)

type HandleContinued func(context.Context, http.ResponseWriter, *http.Request, UriParams) (context.Context, bool)

func validateHttpMethod(method string) bool {
	switch method {
	case http.MethodHead:
	case http.MethodGet:
	case http.MethodPost:
	case http.MethodPut:
	case http.MethodDelete:
	default:
		return false
	}

	return true
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.rateLimit != nil {
		if !s.rateLimit.Wait(1) {
			LimitHandle(w, r)
			return
		}
	}
	s.router.ServeHTTP(w, r)
}

func (s *Server) Handle(method string, path string, handle Handle) {
	if !validateHttpMethod(method) {
		panic(fmt.Sprintf("invalid http method[%s]", method))
	}

	s.doHandle(method, path, handle)
}

// process multiple handles
func (s *Server) Handles(method string, path string, handles []HandleContinued) {
	s.HandlesMethods([]string{method}, path, handles, nil)
}

func (s *Server) HandlesMethods(methods []string, path string, handles []HandleContinued, end HandleContinued) {
	for _, method := range methods {
		if !validateHttpMethod(method) {
			panic(fmt.Sprintf("invalid http method[%s]", method))
		}

		if handles == nil || len(handles) == 0 {
			log.Error("BUG: handles can not be empty!")
			return
		}

		s.doHandles(method, path, handles, end)
	}
}

// type FilterHandle func(http.ResponseWriter, *http.Request) bool

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		// server is already closed
		return
	}

	if s.server != nil {
		s.server.Close()
	}
}

// Graceful shutdown the server
func (s *Server) Shutdown() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		return
	}

	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.CloseTimeout)
		defer cancel()

		if err := s.server.Shutdown(ctx); err != nil {
			log.Error("fail to graceful shutdown http server. err[%v]", err)
		} else {
			log.Debug("finish to shutdown http server")
		}
	}
}

// isClosed checks whether server is closed or not.
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.closed) == 1
}

// Run runs the server.
func (s *Server) Run() error {
	l, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		log.Error("Fail to listen:[%v]. err:%v", s.cfg.Addr, err)
		return err
	}
	if s.cfg.ConnLimit > 0 {
		l = netutil.LimitListener(l, s.cfg.ConnLimit)
	}
	s.rateLimit = s.cfg.RateLimit

	if s.cfg.ReadTimeout > 0 {
		s.server.ReadTimeout = s.cfg.ReadTimeout
	}
	if s.cfg.WriteTimeout > 0 {
		s.server.WriteTimeout = s.cfg.WriteTimeout
	}
	if err = s.server.Serve(l); err != http.ErrServerClosed {
		log.Error("http.listenAndServe failed: %s", err)
	}
	return nil
}

func (s *Server) Name() string {
	return s.cfg.Name
}

func (s *Server) createRouter() {
	if routerMode == RouterModeGorilla {
		s.router = mux.NewRouter()
	} else if routerMode == RouterModeHttpRouter {
		s.router = httprouter.New()
	}
}

func (s *Server) doHandle(method, path string, handle Handle) {
	if routerMode == RouterModeGorilla {
		var h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handle(w, r, &UriParamsMap{
				values: mux.Vars(r),
			})
		})
		s.router.(*mux.Router).Handle(path, h).Methods(method)

	} else if routerMode == RouterModeHttpRouter {
		var h = func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
			uriParams := make(map[string]string)
			for _, param := range params {
				if _, ok := uriParams[param.Key]; !ok {
					uriParams[param.Key] = param.Value
				}
			}
			handle(w, r, &UriParamsMap{values: uriParams})
		}
		s.router.(*httprouter.Router).Handle(method, path, h)
	}
}

func (s *Server) doHandles(method, path string, handles []HandleContinued, end HandleContinued) {
	var flag bool
	if routerMode == RouterModeGorilla {
		var h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), StartTime, time.Now()))
			defer cancel()

			defer s.CatchPanicAndSendErrReply(w)

			uriParamsMap := &UriParamsMap{
				values: mux.Vars(r),
			}
			if end != nil {
				defer func(w http.ResponseWriter, r *http.Request, uriParamsMap *UriParamsMap) {
					end(ctx, w, r, uriParamsMap)
				}(w, r, uriParamsMap)
			}

			for _, handle := range handles {
				if ctx, flag = handle(ctx, w, r, uriParamsMap); !flag {
					break
				}
			}
		})
		s.router.(*mux.Router).Handle(path, h).Methods(method)

	} else if routerMode == RouterModeHttpRouter {
		var h = func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), StartTime, time.Now()))
			defer cancel()

			uriParams := make(map[string]string)

			for _, param := range params {
				if _, ok := uriParams[param.Key]; !ok {
					uriParams[param.Key] = param.Value
				}
			}

			uriParamsMap := &UriParamsMap{
				values: uriParams,
			}

			if end != nil {
				defer end(ctx, w, r, uriParamsMap)
			}

			for _, handle := range handles {
				if ctx, flag = handle(ctx, w, r, uriParamsMap); !flag {
					break
				}
			}
		}
		s.router.(*httprouter.Router).Handle(method, path, h)
	}
}

func (s *Server) CatchPanicAndSendErrReply(w http.ResponseWriter) {
	if r := recover(); r != nil {
		buf := make([]byte, 2048)
		stackSize := runtime.Stack(buf, false)
		buf = buf[:stackSize]
		err := fmt.Errorf("%v", r)
		log.Error("recover panic. %v, %s", r, buf)
		NewResponse(w).SetHttpStatus(http.StatusOK).SendJsonHttpReplyError(err)
	}
}

// Helper handlers

// Error replies to the request with the specified error message and HTTP code.
// It does not otherwise end the request; the caller should ensure no further
// writes are done to w.
// The error message should be plain text.
func Error(w http.ResponseWriter, error string, code int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	fmt.Fprintln(w, error)
}

type ResponseWriter struct {
	http.ResponseWriter
	writer io.Writer
}

func NewResponseWriter(w http.ResponseWriter, writer io.Writer) *ResponseWriter {
	return &ResponseWriter{ResponseWriter: w, writer: writer}
}

func (w *ResponseWriter) Write(b []byte) (int, error) {
	if w.writer == nil {
		return w.Write(b)
	} else {
		return w.writer.Write(b)
	}
}

// http protocal
type HttpReply struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg,omitempty"`
	Data interface{} `json:"data,omitempty"`
}

func LimitHandle(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		io.Copy(ioutil.Discard, r.Body)
	}
	Error(w, "server busy, please wait for a while and retry", http.StatusTooManyRequests)
}
