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
	"github.com/vearch/vearch/monitor"
	"github.com/vearch/vearch/util/server/vearchhttp"
	"github.com/vearch/vearch/util/uuid"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/util/netutil"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/ginutil"
	"github.com/vearch/vearch/util/log"
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
	dh            *vearchhttp.BaseHandler
}

func ExportToClusterHandler(router *gin.Engine, masterService *masterService) {

	dh := vearchhttp.NewBaseHandler(30)

	c := &clusterApi{router: router, masterService: masterService, dh: dh}

	router.Handle(http.MethodGet, "/", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.handleClusterInfo, dh.TimeOutEndHandler)

	//cluster handler
	router.Handle(http.MethodGet, "/clean_lock", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.cleanLock, dh.TimeOutEndHandler)

	//db,servers handler
	router.Handle(http.MethodGet, "/list/server", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.serverList, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/list/db", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.dbList, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/list/space", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.spaceList, dh.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/list/partition", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.partitionList, dh.TimeOutEndHandler)

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
	router.Handle(http.MethodPost, "/partition/change_member", dh.PaincHandler, dh.TimeOutHandler, c.auth, c.changeMember, dh.TimeOutEndHandler)

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

	ginutil.NewAutoMehtodName(c).SendJson(layer)
}

//clean lock for admin , when space locked , waring make sure not create space ing
func (this *clusterApi) cleanLock(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	removed := make([]string, 0, 1)

	if keys, _, err := this.masterService.Master().Store.PrefixScan(ctx.(context.Context), entity.PrefixLock); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	} else {
		for _, key := range keys {
			if err := this.masterService.Master().Store.Delete(ctx.(context.Context), string(key)); err != nil {
				ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
				return
			}
			removed = append(removed, string(key))
		}
	}
	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(removed)
}

//for ps startup to register self and get ip response
func (this *clusterApi) register(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	ip := c.Request.RemoteAddr[0:strings.LastIndex(c.Request.RemoteAddr, ":")]
	log.Debug("register from: %v", ip)
	clusterName := c.Query("clusterName")
	nodeId := entity.NodeID(cast.ToInt64(c.Query("nodeId")))

	if clusterName == "" || nodeId == 0 {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("param err must has clusterName AND nodeId"))
		return
	}

	if clusterName != config.Conf().Global.Name {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("cluster name not same ,please check"))
		return
	}

	server, err := this.masterService.registerServerService(ctx.(context.Context), ip, nodeId)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(server)
}

//wen partition leader got it will register self to this api
func (this *clusterApi) registerPartition(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	partition := &entity.Partition{}

	if err := c.ShouldBindJSON(partition); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	partition.UpdateTime = time.Now().UnixNano()

	if err := this.masterService.registerPartitionService(ctx.(context.Context), partition); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) createDB(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("createDB", startTime)
	db := &entity.DB{}

	if err := c.Bind(db); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	log.Debug("create db: %s", db.Name)

	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := this.masterService.createDBService(ctx.(context.Context), db); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(db)
	}
}

func (this *clusterApi) deleteDB(c *gin.Context) {
	log.Debug("delete db, db: %s", c.Param(DB))
	db := c.Param(DB)
	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := this.masterService.deleteDBService(ctx.(context.Context), db); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) getDB(c *gin.Context) {
	db := c.Param(DB)
	ctx, _ := c.Get(vearchhttp.Ctx)

	if db, err := this.masterService.queryDBService(ctx.(context.Context), db); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(db)
	}
}

func (this *clusterApi) createSpace(c *gin.Context) {
	log.Debug("create space, db: %s", c.Param(dbName))

	dbName := c.Param(dbName)

	ctx, _ := c.Get(vearchhttp.Ctx)

	space := &entity.Space{}

	if err := c.ShouldBindJSON(space); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Debug("create space, space: %s, err: %s", body, err.Error())
		log.Error("parse space settings err: %v", err)
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if space.PartitionNum <= 0 {
		space.PartitionNum = 1
	}

	log.Debug("create space, db: %s", c.Param(dbName))
	if space.ReplicaNum <= 0 {
		space.ReplicaNum = 1
	}

	log.Debug("create space, db: %s", c.Param(dbName))
	if space.Engine == nil {
		space.Engine = entity.NewDefaultEngine()
	}

	//check engine name and DynamicSchema is ok
	if err := space.Validate(); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	space.Version = 1 //first start with 1

	log.Debug("create space, db: %s", c.Param(dbName))
	if err := this.masterService.createSpaceService(ctx.(context.Context), dbName, space); err != nil {
		log.Debug("create space, db: %s", c.Param(dbName))
		log.Error("createSpaceService err: %v", err)
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(space)
	}
}

func (this *clusterApi) deleteSpace(c *gin.Context) {
	log.Debug("delete space, db: %s, space: %s", c.Param(dbName), c.Param(sapceName))
	dbName := c.Param(dbName)
	sapceName := c.Param(sapceName)

	ctx, _ := c.Get(vearchhttp.Ctx)

	if err := this.masterService.deleteSpaceService(ctx.(context.Context), dbName, sapceName); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) getSpace(c *gin.Context) {
	dbName := c.Param(dbName)
	sapceName := c.Param(sapceName)

	ctx, _ := c.Get(vearchhttp.Ctx)

	dbID, err := this.masterService.Master().QueryDBName2Id(ctx.(context.Context), dbName)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if space, err := this.masterService.Master().QuerySpaceByName(ctx.(context.Context), dbID, sapceName); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(space)
	}
}

func (this *clusterApi) updateSpace(c *gin.Context) {
	dbName := c.Param(dbName)
	sapceName := c.Param(sapceName)

	ctx, _ := c.Get(vearchhttp.Ctx)

	space := &entity.Space{Name: sapceName}

	if err := c.ShouldBindJSON(space); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if spaceResult, err := this.masterService.updateSpaceService(ctx.(context.Context), dbName, sapceName, space); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)

	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(spaceResult)
	}
}

//list servers
func (this *clusterApi) serverList(c *gin.Context) {

	ctx, _ := c.Get(vearchhttp.Ctx)

	servers, err := this.masterService.Master().QueryServers(ctx.(context.Context))

	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
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

	serverInfos := make([]map[string]interface{}, 0, len(servers))

	for _, server := range servers {
		serverInfo := make(map[string]interface{})
		serverInfo["server"] = server

		partitionInfos, err := this.masterService.Client.PS().Beg(ctx.(context.Context), uuid.FlakeUUID()).Admin(server.RpcAddr()).PartitionInfos()
		if err != nil {
			serverInfo["error"] = err.Error()
		} else {
			serverInfo["partitions"] = partitionInfos
		}
		serverInfos = append(serverInfos, serverInfo)
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(map[string]interface{}{"servers": serverInfos, "count": len(servers)})
}

//list db
func (this *clusterApi) dbList(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	if dbs, err := this.masterService.queryDBs(ctx.(context.Context)); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(dbs)
	}
}

//list space
func (this *clusterApi) spaceList(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	db := c.Query(DB)

	if db == "" {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("can find param in url ?db=[dbName or dbId]"))
		return
	}

	var dbId entity.DBID
	if unicode.IsNumber([]rune(db)[0]) {
		if id, err := cast.ToInt64E(db); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		} else {
			dbId = entity.DBID(id)
		}
	} else {
		if id, err := this.masterService.Master().QueryDBName2Id(ctx.(context.Context), db); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			return
		} else {
			dbId = id
		}
	}

	if dbs, err := this.masterService.Master().QuerySpaces(ctx.(context.Context), dbId); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(dbs)
	}
}

//list partition
func (this *clusterApi) partitionList(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	partitions, err := this.masterService.Master().QueryPartitions(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(partitions)
	}
}

func (this *clusterApi) changeMember(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)

	cm := &entity.ChangeMember{}

	if err := c.ShouldBindJSON(cm); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	if err := this.masterService.ChangeMember(ctx.(context.Context), cm); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
	}
}

func (this *clusterApi) auth(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	if err := this._auth(ctx.(context.Context), c); err != nil {
		defer this.dh.TimeOutEndHandler(c)
		c.Abort()
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	}
}

func (this *clusterApi) _auth(ctx context.Context, c *gin.Context) error {

	if config.Conf().Global.SkipAuth {
		return nil
	}

	headerData := c.GetHeader(headerAuthKey)

	if headerData == "" {
		return pkg.CodeErr(pkg.ERRCODE_AUTHENTICATION_FAILED)
	}

	username, password, err := util.AuthDecrypt(headerData)
	if err != nil {
		return err
	}

	if username != "root" || password != config.Conf().Global.Signkey {
		return pkg.CodeErr(pkg.ERRCODE_AUTHENTICATION_FAILED)
	}

	return nil
}
