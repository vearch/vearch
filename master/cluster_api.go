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

import "C"
import (
	"context"
	"fmt"
	"github.com/vearch/vearch/util/monitoring"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/util/netutil"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/ginutil"
	"github.com/vearch/vearch/util/server/baudhttp"
	"github.com/tiglabs/log"
)

const (
	DB            = "db"
	dbName        = "db_name"
	sapceName     = "space_name"
	userName      = "user_name"
	userPassword  = "user_password"
	userDbList    = "user_db_list"
	allowdHost    = "allowed_host"
	privilege     = "privilege"
	headerAuthKey = "Authorization"
	PartitionId   = "partition_id"
)

type clusterApi struct {
	router        *gin.Engine
	masterService *masterService
	dh            *baudhttp.BaudHandler
	monitor       monitoring.Monitor
}

func ExportToClusterHandler(router *gin.Engine, masterService *masterService) {

	dh := baudhttp.NewBaudHandler(30)

	c := &clusterApi{router: router, masterService: masterService, dh: dh}

	router.Handle(http.MethodGet, "/", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.handleClusterInfo, dh.TimeOutEndHandler)

	//cluster handler
	router.Handle(http.MethodGet, "/_cluster/health", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.health, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/_cluster/stats", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.stats, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/clean_lock", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.cleanLock, dh.TimeOutEndHandler)

	//db,servers handler
	router.Handle(http.MethodGet, "/list/server", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.serverList, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/list/db", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.dbList, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/list/space", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.spaceList, dh.TimeOutEndHandler)

	//partition register
	router.Handle(http.MethodPost, "/register", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.register, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/register_partition", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.registerPartition, dh.TimeOutEndHandler)

	//db handler
	router.Handle(http.MethodPut, "/db/_create", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.createDB, dh.TimeOutEndHandler)
	router.Handle(http.MethodDelete, "/db/:"+DB, dh.PaincHandler, dh.TimeOutHandler, c.auth, c.deleteDB, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/db/:"+DB, dh.PaincHandler, dh.TimeOutHandler, c.auth, c.getDB, dh.TimeOutEndHandler)

	//space handler
	router.Handle(http.MethodPut, "/space/:"+dbName+"/_create", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.createSpace, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/space/:"+dbName+"/:"+sapceName, dh.PaincHandler, dh.TimeOutHandler, c.auth, c.getSpace, dh.TimeOutEndHandler)
	router.Handle(http.MethodDelete, "/space/:"+dbName+"/:"+sapceName, dh.PaincHandler, dh.TimeOutHandler, c.auth, c.deleteSpace, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/space/:"+dbName+"/:"+sapceName, dh.PaincHandler, dh.TimeOutHandler, c.auth, c.updateSpace, dh.TimeOutEndHandler)

	//partition handler
	router.Handle(http.MethodPost, "/partition/frozen/:"+PartitionId, dh.PaincHandler, dh.TimeOutHandler, c.auth, c.frozenPartition, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/partition/append", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.appendPartition, dh.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/partition/delete", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.deletePartition, dh.TimeOutEndHandler)

}

func (this *clusterApi) handleClusterInfo(c *gin.Context) {

	versionLayer := make(map[string]interface{})
	versionLayer["build_version"] = config.GetBuildVersion()
	versionLayer["build_time"] = config.GetBuildTime()
	versionLayer["commit_id"] = config.GetCommitID()

	layer := make(map[string]interface{})
	layer["name"] = config.Conf().Global.Name
	layer["cluster_name"] = config.Conf().Global.Name
	layer["cluster_uuid"] = ""
	layer["version"] = versionLayer
	layer["tagline"] = ""

	ginutil.NewAutoMehtodName(c, this.monitor).SendJson(layer)
}

//got every partition servers system info
func (this *clusterApi) stats(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)
	list, err := this.masterService.statsService(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}
	ginutil.NewAutoMehtodName(c, this.monitor).SendJson(list)
}

//cluster health in partition level
func (this *clusterApi) health(c *gin.Context) {

	ctx, _ := c.Get(baudhttp.Ctx)

	dbName := c.Query("db")
	spaceName := c.Query("space")

	result, err := this.masterService.partitionInfo(ctx.(context.Context), dbName, spaceName)
	if err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, this.monitor).SendJson(result)
}

//clean lock for admin , when space locked , waring make sure not create space ing
func (this *clusterApi) cleanLock(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)

	removed := make([]string, 0, 1)

	if keys, _, err := this.masterService.Master().Store.PrefixScan(ctx.(context.Context), entity.PrefixLock); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	} else {
		for _, key := range keys {
			if err := this.masterService.Master().Store.Delete(ctx.(context.Context), string(key)); err != nil {
				ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
				return
			}
			removed = append(removed, string(key))
		}
	}
	ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(removed)
}

//for ps startup to register self and get ip response
func (this *clusterApi) register(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)

	ip := c.Request.RemoteAddr[0:strings.LastIndex(c.Request.RemoteAddr, ":")]
	log.Debug("register from: %v", ip)
	clusterName := c.Query("clusterName")
	nodeId := entity.NodeID(cast.ToInt64(c.Query("nodeId")))

	if clusterName == "" || nodeId == 0 {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(fmt.Errorf("param err must has clusterName AND nodeId"))
		return
	}

	if clusterName != config.Conf().Global.Name {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(fmt.Errorf("cluster name not same ,please check"))
		return
	}

	server, err := this.masterService.registerServerService(ctx.(context.Context), ip, nodeId)
	if err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(server)
}

//wen partition leader got it will register self to this api
func (this *clusterApi) registerPartition(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)
	partition := &entity.Partition{}

	if err := c.ShouldBindJSON(partition); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	partition.UpdateTime = time.Now().UnixNano()

	if err := this.masterService.registerPartitionService(ctx.(context.Context), partition); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) createDB(c *gin.Context) {
	db := &entity.DB{}

	if err := c.Bind(db); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}
	log.Debug("create db: %s", db.Name)

	ctx, _ := c.Get(baudhttp.Ctx)

	if err := this.masterService.createDBService(ctx.(context.Context), db); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(db)
	}
}

func (this *clusterApi) deleteDB(c *gin.Context) {
	log.Debug("delete db, db: %s", c.Param(DB))
	db := c.Param(DB)
	ctx, _ := c.Get(baudhttp.Ctx)

	if err := this.masterService.deleteDBService(ctx.(context.Context), db); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) getDB(c *gin.Context) {
	db := c.Param(DB)
	ctx, _ := c.Get(baudhttp.Ctx)

	if db, err := this.masterService.queryDBService(ctx.(context.Context), db); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(db)
	}
}

//ps use this function to frozen partition return err is nil , means ok
//errors not frozen
func (this *clusterApi) frozenPartition(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)
	pid, err := cast.ToInt64E(c.Param(PartitionId))
	if err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(fmt.Errorf("err partition id[%s] err[%s]", c.Param(PartitionId), err.Error()))
		return
	}

	log.Info("Frozen partition, partitionID:[%d]", pid)

	if err := this.masterService.frozenPartition(ctx.(context.Context), entity.PartitionID(pid)); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) appendPartition(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)
	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}
	dbName := c.Request.FormValue(dbName)
	spaceName := c.Request.FormValue(sapceName)
	log.Info("append partition,dbName:[%s],  spaceName:[%s]", dbName, spaceName)

	if err := this.masterService.appendPartition(ctx.(context.Context), dbName, spaceName); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) deletePartition(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)
	if err := c.Request.ParseForm(); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	dbName := c.Request.FormValue(dbName)
	spaceName := c.Request.FormValue(sapceName)
	partitionID := c.Request.FormValue(PartitionId)
	log.Info("delete partition,dbName :[%s],  spaceName:[%s] partitionID:[%s]", dbName, spaceName, partitionID)

	pid, err := cast.ToUint32E(partitionID)
	if err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	if err := this.masterService.deletePartition(ctx.(context.Context), dbName, spaceName, entity.PartitionID(pid)); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) meltPartitionn(c *gin.Context) {

}

func (this *clusterApi) createSpace(c *gin.Context) {
	log.Debug("create space, db: %s", c.Param(dbName))

	dbName := c.Param(dbName)

	ctx, _ := c.Get(baudhttp.Ctx)

	space := &entity.Space{}

	if err := c.ShouldBindJSON(space); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Debug("create space, space: %s, err: %s", body, err.Error())
		log.Error("parse space settings err: %v", err)
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	if space.PartitionNum <= 0 {
		space.PartitionNum = 1
	}

	log.Debug("create space, db: %s", c.Param(dbName))
	if space.ReplicaNum <= 0 {
		space.ReplicaNum = 1
	}

	if space.DynamicSchema == "" {
		space.DynamicSchema = "true"
	}

	if space.StoreSource == nil {
		space.StoreSource = util.PBool(true)
	}

	if space.DynamicSchema == "" {
		space.DynamicSchema = "true"
	}

	if space.DefaultField == "" {
		space.DefaultField = mapping.DefaultField
	}

	if space.DocValuesDynamic == nil {
		space.DocValuesDynamic = util.PBool(true)
	}

	log.Debug("create space, db: %s", c.Param(dbName))
	if space.Engine == nil {
		space.Engine = entity.NewDefaultEngine()
	}

	//check engine name and DynamicSchema is ok
	if err := space.Validate(); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	space.Version = 1 //first start with 1

	log.Debug("create space, db: %s", c.Param(dbName))
	if err := this.masterService.createSpaceService(ctx.(context.Context), dbName, space); err != nil {
		log.Debug("create space, db: %s", c.Param(dbName))
		log.Error("createSpaceService err: %v", err)
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(space)
	}
}

func (this *clusterApi) deleteSpace(c *gin.Context) {
	log.Debug("delete space, db: %s, space: %s", c.Param(dbName), c.Param(sapceName))
	dbName := c.Param(dbName)
	sapceName := c.Param(sapceName)

	ctx, _ := c.Get(baudhttp.Ctx)

	if err := this.masterService.deleteSpaceService(ctx.(context.Context), dbName, sapceName); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) getSpace(c *gin.Context) {
	dbName := c.Param(dbName)
	sapceName := c.Param(sapceName)

	ctx, _ := c.Get(baudhttp.Ctx)

	dbID, err := this.masterService.Master().QueryDBName2Id(ctx.(context.Context), dbName)
	if err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	if space, err := this.masterService.Master().QuerySpaceByName(ctx.(context.Context), dbID, sapceName); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(space)
	}
}

func (this *clusterApi) updateSpace(c *gin.Context) {
	dbName := c.Param(dbName)
	sapceName := c.Param(sapceName)

	ctx, _ := c.Get(baudhttp.Ctx)

	space := &entity.Space{Name: sapceName}

	if err := c.ShouldBindJSON(space); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	if spaceResult, err := this.masterService.updateSpaceService(ctx.(context.Context), dbName, sapceName, space); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)

	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(spaceResult)
	}
}

//list servers
func (this *clusterApi) serverList(c *gin.Context) {

	ctx, _ := c.Get(baudhttp.Ctx)

	servers, err := this.masterService.Master().QueryServers(ctx.(context.Context))

	if err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
		return
	}

	ids := c.Query("ids")

	if ids != "" {
		split := strings.Split(ids, ",")
		nodeIDMap := make(map[entity.NodeID]bool)
		for _, v := range split {
			nodeIDMap[entity.NodeID(cast.ToUint64(v))] = true
		}

		temps := make([]*entity.Server, 0, len(split))

		for _, s := range servers {
			if nodeIDMap[s.ID] {
				temps = append(temps, s)
			}
		}
		servers = temps
	}

	ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(map[string]interface{}{"servers": servers, "count": len(servers)})
}

//list db
func (this *clusterApi) dbList(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)

	if dbs, err := this.masterService.queryDBs(ctx.(context.Context)); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(dbs)
	}
}

//list space
func (this *clusterApi) spaceList(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)

	db := c.Query(DB)

	if db == "" {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(fmt.Errorf("can find param in url ?db=[dbName or dbId]"))
		return
	}

	var dbId entity.DBID
	if unicode.IsNumber([]rune(db)[0]) {
		if id, err := cast.ToInt64E(db); err != nil {
			ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
			return
		} else {
			dbId = entity.DBID(id)
		}
	} else {
		if id, err := this.masterService.Master().QueryDBName2Id(ctx.(context.Context), db); err != nil {
			ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
			return
		} else {
			dbId = id
		}
	}

	if dbs, err := this.masterService.Master().QuerySpaces(ctx.(context.Context), dbId); err != nil {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplySuccess(dbs)
	}
}

func (this *clusterApi) auth(c *gin.Context) {
	ctx, _ := c.Get(baudhttp.Ctx)
	if err := this._auth(ctx.(context.Context), c); err != nil {
		defer this.dh.TimeOutEndHandler(c)
		c.Abort()
		ginutil.NewAutoMehtodName(c, this.monitor).SendJsonHttpReplyError(err)
	}
}

func (this *clusterApi) _auth(ctx context.Context, c *gin.Context) error {

	if config.Conf().Global.SkipAuth {
		return nil
	}

	headerData := c.GetHeader(headerAuthKey)

	if headerData == "" {
		return pkg.ErrMasterAuthenticationFailed
	}

	username, password, err := util.AuthDecrypt(headerData)
	if err != nil {
		return err
	}

	if username != "root" || password != config.Conf().Global.Signkey {
		return pkg.ErrMasterAuthenticationFailed
	}

	return nil

}
