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
	"github.com/gin-gonic/gin"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/ginutil"
	"github.com/vearch/vearch/util/server/vearchhttp"
	"net/http"
)

func ExportToUserHandler(router *gin.Engine, masterService *masterService) {

	dh := vearchhttp.NewBaseHandler(30)

	u := &clusterApi{router: router, masterService: masterService, dh: dh}

	router.Handle(http.MethodPost, "/manage/user/create", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.createUser, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/manage/user/get", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.getUser, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/manage/user/passwd/update", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.userPasswdUpdate, dh.TimeOutEndHandler)
	router.Handle(http.MethodDelete, "/manage/user/delete", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.deleteUser, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/manage/user/list", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.listUser, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/manage/user/grant/db", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.grantUserDB, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/manage/user/revoke/db", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.revokeUserDB, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/manage/user/grant/priv", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.grantUserPriv, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/manage/user/revoke/priv", dh.PaincHandler, dh.TimeOutHandler, u.auth, u.revokeUserPriv, dh.TimeOutEndHandler)
}

func (api *clusterApi) createUser(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}
	userName := c.Request.FormValue(userName)
	password := c.Request.FormValue(userPassword)
	allowedHost := c.Request.FormValue(allowdHost)
	if allowedHost == "" {
		allowedHost = "%"
	}
	dblist := c.Request.FormValue(userDbList)
	userPrivis := c.Request.FormValue(privilege)

	user, err := api.masterService.createUser(ctx.(context.Context), userName, password, allowedHost, dblist, userPrivis)
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(user)
}

func (api *clusterApi) grantUserPriv(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}
	userName := c.Request.FormValue(userName)

	privilist := c.Request.FormValue(privilege)

	user, err := api.masterService.grantUserPriv(ctx.(context.Context), userName, privilist)
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(user)
}

func (api *clusterApi) grantUserDB(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}
	userName := c.Request.FormValue(userName)
	dblist := c.Request.FormValue(userDbList)

	user, err := api.masterService.grantUserDB(ctx.(context.Context), userName, dblist)
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(user)
}

func (api *clusterApi) revokeUserPriv(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}
	userName := c.Request.FormValue(userName)
	privilist := c.Request.FormValue(privilege)
	user, err := api.masterService.revokeUserPriv(ctx.(context.Context), userName, privilist)
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(user)
}

func (api *clusterApi) revokeUserDB(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}
	userName := c.Request.FormValue(userName)
	dblist := c.Request.FormValue(userDbList)

	user, err := api.masterService.revokeUserDB(ctx.(context.Context), userName, dblist)
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(user)
}

func (api *clusterApi) deleteUser(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}
	userName := c.Request.FormValue(userName)
	user, err := api.masterService.deleteUser(ctx.(context.Context), userName)
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(user)
}

func (api *clusterApi) getUser(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	user, err := api.masterService.queryUser(ctx.(context.Context), c.Query(userName))
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	user.HeadKey = util.AuthEncrypt(user.Name, user.Password)

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(user)
}

func (api *clusterApi) listUser(c *gin.Context) {
	users, err := api.masterService.listUser()
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(users)
}

func (api *clusterApi) userPasswdUpdate(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}
	userName := c.Request.FormValue(userName)
	passwd := c.Request.FormValue(userPassword)

	user, err := api.masterService.updateUserPass(ctx.(context.Context), userName, passwd)
	if err != nil {
		ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, api.monitor).SendJsonHttpReplySuccess(user)
}
