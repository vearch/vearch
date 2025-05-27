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
	"encoding/base64"
	"fmt"
	"net"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/monitor"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	"github.com/vearch/vearch/v3/internal/pkg/netutil"
	"github.com/vearch/vearch/v3/internal/router/document"
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

	res, err := cli.Master().RegisterRouter(ctx, config.Conf().Global.Name, time.Duration(10*time.Second))
	if err != nil {
		return nil, err
	}

	log.Info("register router success, res: %s", res)

	addr := config.LocalCastAddr

	gin.SetMode(gin.ReleaseMode)
	httpServer := gin.New()
	httpServer.Use(func(c *gin.Context) {
		rid := c.GetHeader("X-Request-Id")
		if rid == "" {
			id, err := uuid.NewRandom()
			if err != nil {
				log.Error("generate request id failed, %v", err)
				c.Next()
				return
			}
			rid = id.String()
			rid = base64.StdEncoding.EncodeToString([]byte(rid))[:10]
			c.Request.Header.Add("X-Request-Id", rid)
		}

		// set request id to context
		c.Header("X-Request-Id", rid)
		c.Next()
	})
	if len(config.Conf().Router.AllowOrigins) > 0 {
		corsConfig := cors.DefaultConfig()
		corsConfig.AllowCredentials = true
		corsConfig.AllowOrigins = config.Conf().Router.AllowOrigins
		log.Info("use cors, AllowOrigins: %v", corsConfig.AllowOrigins)
		httpServer.Use(cors.New(corsConfig))
	}

	document.ExportDocumentHandler(httpServer, cli)

	var rpcServer *grpc.Server
	if config.Conf().Router.RpcPort > 0 {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, config.Conf().Router.RpcPort))
		if err != nil {
			panic(fmt.Errorf("start rpc server failed to listen: %v", err))
		}
		rpcServer = grpc.NewServer()
		go func() {
			if err := rpcServer.Serve(lis); err != nil {
				panic(fmt.Errorf("start rpc server failed to start: %v", err))
			}
		}()
		// document.ExportRpcHandler(rpcServer, cli)
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

	server.StartHeartbeatJob(routerIP)

	if port := config.Conf().Router.MonitorPort; port > 0 {
		monitor.Register(nil, nil, config.Conf().Router.MonitorPort)
	}

	if err := server.httpServer.Run(cast.ToString(fmt.Sprintf("0.0.0.0:%d", config.Conf().Router.Port))); err != nil {
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
