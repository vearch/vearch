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

package master

import (
	"context"
	"fmt"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/util/monitoring"
	"github.com/vearch/vearch/util/vearchlog"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"go.etcd.io/etcd/embed"
)

type Server struct {
	etcCfg     *embed.Config
	client     *client.Client
	etcdServer *embed.Etcd
	ctx        context.Context
	monitor    monitoring.Monitor
}

func NewServer(ctx context.Context) (*Server, error) {
	log.Regist(vearchlog.NewVearchLog(config.Conf().GetLogDir(config.Master), "Master", config.Conf().GetLevel(config.Master), true))
	//Logically, this code should not be executed, because if the local master is not found, it will panic
	if config.Conf().Masters.Self() == nil {
		return nil, fmt.Errorf("master not init please your address or master name ")
	}

	cfg, err := config.Conf().GetEmbed()

	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return nil, err
	}

	return &Server{etcCfg: cfg, ctx: ctx, monitor: config.Conf().NewMonitor(config.Master)}, nil
}

func (s *Server) Start() (err error) {
	//start api server
	log.Debug("master start ...")

	//start etc server
	s.etcdServer, err = embed.StartEtcd(s.etcCfg)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	defer s.etcdServer.Close()

	select {
	case <-s.etcdServer.Server.ReadyNotify():
		log.Info("Server is ready!")
	case <-time.After(60 * time.Second):
		s.etcdServer.Server.Stop() // trigger a shutdown
		log.Error("Server took too long to start!")
		return fmt.Errorf("etcd start timeout")
	}

	s.client, err = client.NewClient(config.Conf())
	if err != nil {
		return err
	}
	service, err := newMasterService(s.client)
	if err != nil {
		return err
	}

	if !log.IsDebugEnabled() {
		gin.SetMode(gin.ReleaseMode)
	}

	// start http server
	engine := gin.Default()

	ExportToClusterHandler(engine, service)
	ExportToUserHandler(engine, service)

	go func() {
		if err := engine.Run(":" + cast.ToString(config.Conf().Masters.Self().ApiPort)); err != nil {
			panic(err)
		}
	}()

	s.StartCleanJon(s.ctx)

	return <-s.etcdServer.Err()
}

func (s *Server) Stop() {
	log.Info("master shutdown... start")
	s.etcdServer.Server.Stop()
	log.Info("master shutdown... end")
}
