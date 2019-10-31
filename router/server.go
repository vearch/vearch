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
	"regexp"
	"strings"
	"time"

	"github.com/vearch/vearch/util/metrics/mserver"

	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/router/document"
	"github.com/vearch/vearch/util"
	_ "github.com/vearch/vearch/util/init"
	"github.com/vearch/vearch/util/netutil"
	"github.com/vearch/vearch/util/vearchlog"
)

const (
	DefaultConnMaxLimit = 10000
	DefaultCloseTimeout = 5 * time.Second
)

type Server struct {
	httpServer *netutil.Server
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewServer(ctx context.Context) (*Server, error) {
	// master service load cfg and init
	log.Regist(vearchlog.NewVearchLog(config.Conf().GetLogDir(config.Router), "Router", config.Conf().GetLevel(config.Router), true))
	cli, err := client.NewClient(config.Conf())
	if err != nil {
		return nil, err
	}

	addr := config.LocalCastAddr

	httpServerConfig := &netutil.ServerConfig{
		Name:         "HttpServer",
		Addr:         util.BuildAddr(addr, config.Conf().Router.Port),
		Version:      "v1",
		ConnLimit:    DefaultConnMaxLimit,
		CloseTimeout: DefaultCloseTimeout,
	}
	netutil.SetMode(netutil.RouterModeGorilla)
	httpServer := netutil.NewServer(httpServerConfig)
	document.ExportDocumentHandler(httpServer, cli, config.Conf().NewMonitor(config.Router))

	routerCtx, routerCancel := context.WithCancel(ctx)
	// start router cache
	if err := cli.Master().FlushCacheJob(routerCtx); err != nil {
		log.Error("Error in Start cache Job,Err:%v", err)
		panic(err)
	}

	return &Server{
		httpServer: httpServer,
		ctx:        routerCtx,
		cancelFunc: routerCancel,
	}, nil
}

func (server *Server) Start() error {
	//find ip for server
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			match, _ := regexp.MatchString(`^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+$`, addr.String())
			if !match {
				continue
			}
			slit := strings.Split(addr.String(), "/")
			mserver.SetIp(slit[0], false)
			break
		}
	}

	if err := server.httpServer.Run(); err != nil {
		return fmt.Errorf("Fail to start http Server, %v", err)
	}
	log.Info("router exited!")

	return nil
}

func (server *Server) Shutdown() {
	server.cancelFunc()
	log.Info("router shutdown... start")
	if server.httpServer != nil {
		server.httpServer.Shutdown()
		server.httpServer = nil
	}
	log.Info("router shutdown... end")
}
