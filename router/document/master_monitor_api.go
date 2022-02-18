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

package document

import "C"
import (
	"context"
	"net/http"

	"github.com/vearch/vearch/util/gorillautil"
	"github.com/vearch/vearch/util/netutil"
	"github.com/vearch/vearch/util/server/vearchhttp"
)

// export monitor
type MasterMonitorAPI struct {
	monitorService *monitorService
	dh             *vearchhttp.BaseHandler
}

// export monitor API by gorilla
func (handler *DocumentHandler) GorillaExportMonitor(monitorService *monitorService) error {

	dh := vearchhttp.NewBaseHandler(30)

	msMonitor := &MasterMonitorAPI{monitorService: monitorService, dh: dh}

	// cluster handler
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/_cluster/health", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, msMonitor.health}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/_cluster/stats", []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, msMonitor.stats}, nil)

	//if monitorService != nil {
	//	if config.Conf().Router.EtcdMonitorPort > 0 {
	//		monitor.Register(monitorService.Client, monitorService.etcdServer, config.Conf().Router.EtcdMonitorPort)
	//	}
	//}
	return nil
}

//got every partition servers system info
func (monitor *MasterMonitorAPI) stats(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	list, err := monitor.monitorService.statsService(ctx)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, false
	}
	gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(list)
	return ctx, true
}

//cluster health in partition level
func (monitor *MasterMonitorAPI) health(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	head := setRequestHead(params, r)

	dbName := head.Params["db"]
	spaceName := head.Params["space"]

	result, err := monitor.monitorService.partitionInfo(ctx, dbName, spaceName)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, false
	}
	gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(result)
	return ctx, true
}
