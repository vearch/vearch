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
	"context"
	"fmt"
	"net"
	"time"

	"github.com/gin-gonic/gin"
	limit "github.com/juju/ratelimit"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/internal/client"
	"github.com/vearch/vearch/internal/monitor"
	"github.com/vearch/vearch/internal/router/document"
	"github.com/vearch/vearch/internal/util"
	"github.com/vearch/vearch/internal/util/log"
	"github.com/vearch/vearch/internal/util/metrics/mserver"
	"github.com/vearch/vearch/internal/util/netutil"
	"google.golang.org/grpc"
)

type Server struct {
	ctx        context.Context
	cli        *client.Client
	httpServer *gin.Engine
	rpcServer  *grpc.Server
	cancelFunc context.CancelFunc
}

func NewServer(ctx context.Context) (*Server, error) {
	cli, err := client.NewClient(config.Conf())
	if err != nil {
		return nil, err
	}

	addr := config.LocalCastAddr

	// httpServerConfig := &netutil.ServerConfig{
	// 	Name:         "HttpServer",
	// 	Addr:         util.BuildAddr(addr, config.Conf().Router.Port),
	// 	ConnLimit:    config.Conf().Router.ConnLimit,
	// 	CloseTimeout: time.Duration(config.Conf().Router.CloseTimeout),
	// }
	// netutil.SetMode(netutil.RouterModeGorilla) //no need

	// httpServer := netutil.NewServer(httpServerConfig)
	// if !log.IsDebugEnabled() {
	// 	gin.SetMode(gin.ReleaseMode)
	// }
	gin.SetMode(gin.ReleaseMode)
	httpServer := gin.New()

	document.ExportDocumentHandler(httpServer, cli)

	var rpcServer *grpc.Server
	if config.Conf().Router.RpcPort > 0 {
		lis, err := net.Listen("tcp", util.BuildAddr(addr, config.Conf().Router.RpcPort))
		if err != nil {
			panic(fmt.Errorf("start rpc server failed to listen: %v", err))
		}
		rpcServer = grpc.NewServer()
		go func() {
			if err := rpcServer.Serve(lis); err != nil {
				panic(fmt.Errorf("start rpc server failed to start: %v", err))
			}
		}()
		document.ExportRpcHandler(rpcServer, cli)
	}

	routerCtx, routerCancel := context.WithCancel(ctx)
	// start router cache
	if err := cli.Master().FlushCacheJob(routerCtx); err != nil {
		log.Error("Error in Start cache Job,Err:%v", err)
		panic(err)
	}

	return &Server{
		httpServer: httpServer,
		ctx:        routerCtx,
		cli:        cli,
		cancelFunc: routerCancel,
		rpcServer:  rpcServer,
	}, nil
}

func (server *Server) Start() error {
	var routerIP string
	var err error
	// get local IP addr
	routerIP, err = netutil.GetLocalIP()
	if err != nil {
		panic(fmt.Sprintf("conn master failed, err: [%s]", err.Error()))
	}
	log.Debugf("Get router ip: [%s]", routerIP)
	mserver.SetIp(routerIP, false)
	if config.Conf().Router.RpcPort > 0 {
		server.StartHeartbeatJob(fmt.Sprintf("%s:%d", routerIP, config.Conf().Router.RpcPort))
	}

	if port := config.Conf().Router.MonitorPort; port > 0 {
		monitor.Register(nil, nil, config.Conf().Router.MonitorPort)
	}

	if err := server.httpServer.Run(cast.ToString(util.BuildAddr("0.0.0.0", config.Conf().Router.Port))); err != nil {
		return fmt.Errorf("fail to start http Server, %v", err)
	}
	log.Info("router exited!")

	return nil
}

func (server *Server) Shutdown() {
	server.cancelFunc()
	log.Info("router shutdown... start")
	if server.httpServer != nil {
		server.httpServer = nil
	}
	log.Info("router shutdown... end")
}

type Limiter struct {
	bucker *limit.Bucket
}

func (l *Limiter) Limit() bool {
	return !l.bucker.WaitMaxDuration(1, time.Second)
}
