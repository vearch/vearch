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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/monitor"
	"github.com/vearch/vearch/util/ginutil"
	"github.com/vearch/vearch/util/server/vearchhttp"
)

type monitorApi struct {
	router         *gin.Engine
	monitorService *monitorService
	dh             *vearchhttp.BaseHandler
}

func ExportToMonitorHandler(router *gin.Engine, monitorService *monitorService) {
	dh := vearchhttp.NewBaseHandler(30)

	c := &monitorApi{router: router, monitorService: monitorService, dh: dh}

	//cluster handler
	router.Handle(http.MethodGet, "/_cluster/health", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.health, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/_cluster/stats", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.stats, dh.TimeOutEndHandler)

	monitor.Register(monitorService.Client, monitorService.etcdServer, config.Conf().Masters.Self().MonitorPort)
	//monitorService.Register()
}

// got every partition servers system info
func (m *monitorApi) stats(c *gin.Context) {
	list, err := m.monitorService.statsService(c)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	ginutil.NewAutoMehtodName(c).SendJson(list)
}

// cluster health in partition level
func (m *monitorApi) health(c *gin.Context) {
	dbName := c.Query("db")
	spaceName := c.Query("space")
	detail := c.Query("detail")

	result, err := m.monitorService.partitionInfo(c, dbName, spaceName, detail)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c).SendJson(result)
}

func (m *monitorApi) auth(c *gin.Context) {
	if err := Auth(c); err != nil {
		defer m.dh.TimeOutEndHandler(c)
		c.Abort()
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	}
}
