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
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/entity/errors"
	"github.com/vearch/vearch/v3/internal/monitor"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/httphelper"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/netutil"
	"github.com/vearch/vearch/v3/internal/pkg/server/vearchhttp"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
)

const (
	DB                  = "db"
	dbName              = "db_name"
	spaceName           = "space_name"
	aliasName           = "alias_name"
	headerAuthKey       = "Authorization"
	NodeID              = "node_id"
	DefaultResourceName = "default"
)

type clusterAPI struct {
	router        *gin.Engine
	masterService *masterService
	dh            *vearchhttp.BaseHandler
	server        *Server
}

func ExportToClusterHandler(router *gin.Engine, masterService *masterService, server *Server) {
	dh := vearchhttp.NewBaseHandler(30)

	c := &clusterAPI{router: router, masterService: masterService, dh: dh, server: server}

	var group *gin.RouterGroup
	if !config.Conf().Global.SkipAuth {
		group = router.Group("", dh.PaincHandler, dh.TimeOutHandler, gin.BasicAuth(gin.Accounts{
			"root": config.Conf().Global.Signkey,
		}))
	} else {
		group = router.Group("", dh.PaincHandler, dh.TimeOutHandler)
	}

	group.GET("/", c.handleClusterInfo, dh.TimeOutEndHandler)

	// cluster handler
	group.GET("/clean_lock", c.cleanLock, dh.TimeOutEndHandler)

	// servers handler
	group.GET("/servers", c.serverList, dh.TimeOutEndHandler)

	// router  handler
	group.GET("/routers", c.routerList, dh.TimeOutEndHandler)

	// partition register
	group.POST("/register", c.register, dh.TimeOutEndHandler)
	group.POST("/register_partition", c.registerPartition, dh.TimeOutEndHandler)
	group.POST("/register_router", c.registerRouter, dh.TimeOutEndHandler)

	// db handler
	group.POST(fmt.Sprintf("/dbs/:%s", dbName), c.createDB, dh.TimeOutEndHandler)
	group.GET(fmt.Sprintf("/dbs/:%s", dbName), c.getDB, dh.TimeOutEndHandler)
	group.GET("/dbs", c.getDB, dh.TimeOutEndHandler)
	group.DELETE(fmt.Sprintf("/dbs/:%s", dbName), c.deleteDB, dh.TimeOutEndHandler)
	group.PUT(fmt.Sprintf("/dbs/:%s", dbName), c.modifyDB, dh.TimeOutEndHandler)

	// space handler
	group.POST(fmt.Sprintf("/dbs/:%s/spaces", dbName), c.createSpace, dh.TimeOutEndHandler)
	group.GET(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.getSpace, dh.TimeOutEndHandler)
	group.GET(fmt.Sprintf("/dbs/:%s/spaces", dbName), c.getSpace, dh.TimeOutEndHandler)
	group.DELETE(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.deleteSpace, dh.TimeOutEndHandler)
	// group.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.updateSpace, dh.TimeOutEndHandler)
	group.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.updateSpaceResource, dh.TimeOutEndHandler)
	group.POST(fmt.Sprintf("/backup/dbs/:%s/spaces/:%s", dbName, spaceName), c.backupSpace, dh.TimeOutEndHandler)

	// modify engine config handler
	group.POST("/config/:"+dbName+"/:"+spaceName, c.modifyEngineCfg, dh.TimeOutEndHandler)
	group.GET("/config/:"+dbName+"/:"+spaceName, c.getEngineCfg, dh.TimeOutEndHandler)

	// partition handler
	group.GET("/partitions", c.partitionList, dh.TimeOutEndHandler)
	group.POST("/partitions/change_member", c.changeMember, dh.TimeOutEndHandler)

	// schedule
	group.POST("/schedule/recover_server", c.RecoverFailServer, dh.TimeOutEndHandler)
	group.POST("/schedule/change_replicas", c.ChangeReplicas, dh.TimeOutEndHandler)
	group.GET("/schedule/fail_server", c.FailServerList, dh.TimeOutEndHandler)
	group.DELETE("/schedule/fail_server/:"+NodeID, c.FailServerClear, dh.TimeOutEndHandler)
	group.GET("/schedule/clean_task", c.CleanTask, dh.TimeOutEndHandler)

	// remove server metadata
	group.POST("/meta/remove_server", c.RemoveServerMeta, dh.TimeOutEndHandler)

	// alias handler
	group.POST(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", aliasName, dbName, spaceName), c.createAlias, dh.TimeOutEndHandler)
	group.GET(fmt.Sprintf("/alias/:%s", aliasName), c.getAlias, dh.TimeOutEndHandler)
	group.GET("/alias", c.getAlias, dh.TimeOutEndHandler)
	group.DELETE(fmt.Sprintf("/alias/:%s", aliasName), c.deleteAlias, dh.TimeOutEndHandler)
	group.PUT(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", aliasName, dbName, spaceName), c.modifyAlias, dh.TimeOutEndHandler)
}

func (ca *clusterAPI) handleClusterInfo(c *gin.Context) {
	versionLayer := make(map[string]interface{})
	versionLayer["build_version"] = config.GetBuildVersion()
	versionLayer["build_time"] = config.GetBuildTime()
	versionLayer["commit_id"] = config.GetCommitID()

	layer := make(map[string]interface{})
	layer["name"] = config.Conf().Global.Name
	layer["version"] = versionLayer

	httphelper.New(c).JsonSuccess(layer)
}

// cleanLock lock for admin, when space locked, waring make sure not create space ing
func (ca *clusterAPI) cleanLock(c *gin.Context) {
	removed := make([]string, 0, 1)

	if keys, _, err := ca.masterService.Master().Store.PrefixScan(c, entity.PrefixLock); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
	} else {
		for _, key := range keys {
			if err := ca.masterService.Master().Store.Delete(c, string(key)); err != nil {
				httphelper.New(c).JsonError(errors.NewErrInternal(err))
				return
			}
			removed = append(removed, string(key))
		}
		httphelper.New(c).JsonSuccess(removed)
	}
}

// for ps startup to register self and get ip response
func (ca *clusterAPI) register(c *gin.Context) {
	ip := c.ClientIP()
	log.Debug("register from: %v", ip)
	clusterName := c.Query("clusterName")
	nodeID := entity.NodeID(cast.ToInt64(c.Query("nodeID")))

	if clusterName == "" || nodeID == 0 {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has clusterName AND nodeID")))
		return
	}

	if clusterName != config.Conf().Global.Name {
		httphelper.New(c).JsonError(errors.NewErrUnprocessable(fmt.Errorf("cluster name different, please check")))
		return
	}

	// if node id is already existed, return failed
	if err := ca.masterService.IsExistNode(c, nodeID, ip); err != nil {
		log.Debug("nodeID[%d] exist %v", nodeID, err)
		httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}

	server, err := ca.masterService.registerServerService(c, ip, nodeID)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	httphelper.New(c).JsonSuccess(server)
}

// for router startup to register self and get ip response
func (ca *clusterAPI) registerRouter(c *gin.Context) {
	ip := c.ClientIP()
	log.Debug("register from: %s", ip)
	clusterName := c.Query("clusterName")

	if clusterName != config.Conf().Global.Name {
		httphelper.New(c).JsonError(errors.NewErrUnprocessable(fmt.Errorf("cluster name different, please check")))
		return
	}

	httphelper.New(c).JsonSuccess(ip)
}

// when partition leader got it will register self to this api
func (ca *clusterAPI) registerPartition(c *gin.Context) {
	partition := &entity.Partition{}

	if err := c.ShouldBindJSON(partition); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	partition.UpdateTime = time.Now().UnixNano()

	if err := ca.masterService.registerPartitionService(c, partition); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(nil)
	}
}

func (ca *clusterAPI) createDB(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("createDB", startTime)
	dbName := c.Param(dbName)
	db := &entity.DB{Name: dbName}

	log.Debug("create db: %s", db.Name)

	if err := ca.masterService.createDBService(c, db); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(db)
	}
}

func (ca *clusterAPI) deleteDB(c *gin.Context) {
	log.Debug("delete db, db: %s", c.Param(dbName))
	db := c.Param(dbName)

	if err := ca.masterService.deleteDBService(c, db); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getDB(c *gin.Context) {
	db := c.Param(dbName)
	if db == "" {
		if dbs, err := ca.masterService.queryDBs(c); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httphelper.New(c).JsonSuccess(dbs)
		}
	} else {
		if db, err := ca.masterService.queryDBService(c, db); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httphelper.New(c).JsonSuccess(db)
		}
	}
}

func (ca *clusterAPI) modifyDB(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	dbModify := &entity.DBModify{}
	if err := c.ShouldBindJSON(dbModify); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if db, err := ca.masterService.updateDBIpList(ctx.(context.Context), dbModify); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(db)
	}
}

func (ca *clusterAPI) createSpace(c *gin.Context) {
	log.Debug("create space, db: %s", c.Param(dbName))

	dbName := c.Param(dbName)

	space := &entity.Space{}
	if err := c.ShouldBindJSON(space); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	// set default resource name
	if space.ResourceName == "" {
		space.ResourceName = DefaultResourceName
	}

	if space.PartitionNum <= 0 {
		space.PartitionNum = 1
	}

	if config.Conf().Global.LimitedReplicaNum && space.ReplicaNum < 3 {
		err := fmt.Errorf("LimitedReplicaNum is set and in order to ensure high availability replica should not be less than 3")
		log.Error(err.Error())
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if space.ReplicaNum <= 0 {
		space.ReplicaNum = 1
	}

	// check index name is ok
	if err := space.Validate(); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	space.Version = 1 // first start with 1

	if err := ca.masterService.createSpaceService(c, dbName, space); err != nil {
		log.Error("createSpaceService err: %v", err)
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(space)
	}

	if space.Index == nil {
		space.Index = entity.NewDefaultIndex()
	}
}

func (ca *clusterAPI) deleteSpace(c *gin.Context) {
	log.Debug("delete space, db: %s, space: %s", c.Param(dbName), c.Param(spaceName))
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)

	if err := ca.masterService.deleteSpaceService(c, dbName, spaceName); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getSpace(c *gin.Context) {
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	detail := c.Query("detail")
	detail_info := false
	if detail == "true" {
		detail_info = true
	}

	dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	if spaceName != "" {
		if space, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			spaceInfo := &entity.SpaceInfo{}
			spaceInfo.DbName = dbName
			spaceInfo.SpaceName = spaceName
			spaceInfo.Schema = &entity.SpaceSchema{
				Fields: space.Fields,
			}
			spaceInfo.PartitionNum = space.PartitionNum
			spaceInfo.ReplicaNum = space.ReplicaNum
			if err := ca.masterService.describeSpaceService(c, space, spaceInfo, detail_info); err != nil {
				httphelper.New(c).JsonError(errors.NewErrInternal(err))
			} else {
				httphelper.New(c).JsonSuccess(spaceInfo)
			}
		}
	} else {
		if spaces, err := ca.masterService.Master().QuerySpaces(c, dbID); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			spaceInfos := make([]*entity.SpaceInfo, 0, len(spaces))
			for _, space := range spaces {
				var spaceInfo = &entity.SpaceInfo{}
				spaceInfo.DbName = dbName
				spaceInfo.SpaceName = space.Name
				spaceInfo.Schema = &entity.SpaceSchema{
					Fields: space.Fields,
				}
				spaceInfo.PartitionNum = space.PartitionNum
				spaceInfo.ReplicaNum = space.ReplicaNum
				if err := ca.masterService.describeSpaceService(c, space, spaceInfo, detail_info); err != nil {
					httphelper.New(c).JsonError(errors.NewErrInternal(err))
				} else {
					spaceInfos = append(spaceInfos, spaceInfo)
				}
			}
			httphelper.New(c).JsonSuccess(spaceInfos)
		}
	}
}

func (ca *clusterAPI) updateSpace(c *gin.Context) {
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)

	space := &entity.Space{Name: spaceName}

	if err := c.ShouldBindJSON(space); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if spaceResult, err := ca.masterService.updateSpaceService(c, dbName, spaceName, space); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(spaceResult)
	}
}

func (ca *clusterAPI) updateSpaceResource(c *gin.Context) {
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)

	space := &entity.SpaceResource{
		SpaceName: spaceName,
		DbName:    dbName,
	}

	if err := c.ShouldBindJSON(space); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("updateSpaceResource %v", space)

	if spaceResult, err := ca.masterService.updateSpaceResourceService(c, space); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(spaceResult)
	}
}

func (ca *clusterAPI) backupSpace(c *gin.Context) {
	var err error
	defer errutil.CatchError(&err)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	data, err := io.ReadAll(c.Request.Body)

	errutil.ThrowError(err)
	log.Debug("engine config json data is [%+v]", string(data))
	backup := &entity.BackupSpace{}
	err = vjson.Unmarshal(data, &backup)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if err := ca.masterService.BackupSpace(c, dbName, spaceName, backup); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(backup)
	}
}

func (ca *clusterAPI) createAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}

	if _, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	alias := &entity.Alias{Name: aliasName, DbName: dbName, SpaceName: spaceName}

	log.Debug("create alias: %s, dbName: %s, spaceName: %s", aliasName, dbName, spaceName)

	if err := ca.masterService.createAliasService(c, alias); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(alias)
	}
}

func (ca *clusterAPI) deleteAlias(c *gin.Context) {
	log.Debug("delete alias: %s", c.Param(aliasName))
	aliasName := c.Param(aliasName)

	if err := ca.masterService.deleteAliasService(c, aliasName); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	if aliasName == "" {
		if alias, err := ca.masterService.queryAllAlias(c); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httphelper.New(c).JsonSuccess(alias)
		}
	} else {
		if alias, err := ca.masterService.queryAliasService(c, aliasName); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httphelper.New(c).JsonSuccess(alias)
		}
	}
}

func (ca *clusterAPI) modifyAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	if _, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}

	alias := &entity.Alias{
		Name:      aliasName,
		DbName:    dbName,
		SpaceName: spaceName,
	}
	if err := ca.masterService.updateAliasService(c, alias); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(alias)
	}
}

// get engine config
func (ca *clusterAPI) getEngineCfg(c *gin.Context) {
	var err error
	defer errutil.CatchError(&err)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	errutil.ThrowError(err)
	if cfg, err := ca.masterService.GetEngineCfg(c, dbName, spaceName); err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		httphelper.New(c).JsonSuccess(cfg)
	}
}

// modify engine config
func (ca *clusterAPI) modifyEngineCfg(c *gin.Context) {
	var err error
	defer errutil.CatchError(&err)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	data, err := io.ReadAll(c.Request.Body)

	errutil.ThrowError(err)
	log.Debug("engine config json data is [%+v]", string(data))
	cacheCfg := &entity.EngineCfg{}
	err = vjson.Unmarshal(data, &cacheCfg)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if cacheCfg.CacheModels == nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("engine config [%+v] is error", string(data))))
		return
	}
	if err := ca.masterService.ModifyEngineCfg(c, dbName, spaceName, cacheCfg); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(cacheCfg)
	}
}

// serverList list servers
func (ca *clusterAPI) serverList(c *gin.Context) {
	servers, err := ca.masterService.Master().QueryServers(c)

	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
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

		partitionInfos, err := client.PartitionInfos(server.RpcAddr())
		if err != nil {
			serverInfo["error"] = err.Error()
		} else {
			serverInfo["partitions"] = partitionInfos
		}
		serverInfos = append(serverInfos, serverInfo)
	}

	httphelper.New(c).JsonSuccess(map[string]interface{}{"servers": serverInfos, "count": len(servers)})
}

// routerList list router
func (ca *clusterAPI) routerList(c *gin.Context) {
	if routerIPs, err := ca.masterService.Master().QueryRouter(c, config.Conf().Global.Name); err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		httphelper.New(c).JsonSuccess(routerIPs)
	}
}

// partitionList list partition
func (ca *clusterAPI) partitionList(c *gin.Context) {
	partitions, err := ca.masterService.Master().QueryPartitions(c)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		httphelper.New(c).JsonSuccess(partitions)
	}
}

// list fail servers
func (cluster *clusterAPI) FailServerList(c *gin.Context) {

	ctx, _ := c.Get(vearchhttp.Ctx)

	failServers, err := cluster.masterService.Master().QueryAllFailServer(ctx.(context.Context))

	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	httphelper.New(c).JsonSuccess(map[string]interface{}{"fail_servers": failServers, "count": len(failServers)})
}

// clear fail server by nodeID
func (cluster *clusterAPI) FailServerClear(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	nodeID := c.Param(NodeID)
	if nodeID == "" {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has nodeId")))
		return
	}
	id, err := strconv.ParseUint(nodeID, 10, 64)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("nodeId err")))
		return
	}
	err = cluster.masterService.Master().DeleteFailServerByNodeID(ctx.(context.Context), id)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	httphelper.New(c).JsonSuccess(map[string]interface{}{"nodeID": nodeID})
}

// clear task
func (cluster *clusterAPI) CleanTask(c *gin.Context) {
	CleanTask(cluster.server)
	httphelper.New(c).JsonSuccess("clean task success!")
}

// remove etcd meta about the nodeID
func (cluster *clusterAPI) RemoveServerMeta(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	rfs := &entity.RecoverFailServer{}
	if err := c.ShouldBindJSON(rfs); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	// get nodeID
	nodeID := rfs.FailNodeID
	// ipAddr
	ipAdd := rfs.FailNodeAddr
	if nodeID == 0 && ipAdd == "" {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has fail_node_id or fail_node_addr")))
		return
	}
	log.Debug("RemoveServerMeta info is %+v", rfs)
	// get failServer
	var failServer *entity.FailServer
	if nodeID > 0 {
		failServer = cluster.masterService.Master().QueryFailServerByNodeID(ctx.(context.Context), nodeID)
	}
	// if nodeId can't get server info
	if failServer == nil && ipAdd != "" {
		failServer = cluster.masterService.Master().QueryServerByIPAddr(ctx.(context.Context), ipAdd)
	}
	// get all partition
	if failServer != nil && failServer.Node != nil {
		for _, pid := range failServer.Node.PartitionIds {
			// begin clear pid in nodeID
			cm := &entity.ChangeMember{}
			cm.NodeID = failServer.ID
			cm.PartitionID = pid
			cm.Method = proto.ConfRemoveNode
			log.Debug("begin ChangeMember %+v", cm)
			err := cluster.masterService.ChangeMember(ctx.(context.Context), cm)
			if err != nil {
				log.Error("ChangePartitionMember [%+v] err is %s", cm, err.Error())
				httphelper.New(c).JsonError(errors.NewErrInternal(err))
				return
			}
		}
	} else {
		httphelper.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("can't find server [%v]", failServer)))
		return
	}
	httphelper.New(c).JsonSuccess(fmt.Sprintf("nodeid [%d], server [%v] remove node success!", nodeID, failServer))
}

// recover the failserver by a newserver
func (cluster *clusterAPI) RecoverFailServer(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	rs := &entity.RecoverFailServer{}
	if err := c.ShouldBindJSON(rs); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	rsStr := vjson.ToJsonString(rs)
	log.Info("RecoverFailServer is %s,", rsStr)
	if err := cluster.masterService.RecoverFailServer(ctx.(context.Context), rs); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("%s failed recover,err is %v", rsStr, err)))
	} else {
		httphelper.New(c).JsonSuccess(fmt.Sprintf("%s success recover!", rsStr))
	}
}

// change replicas by dbname and spaceName
func (cluster *clusterAPI) ChangeReplicas(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	dbModify := &entity.DBModify{}
	if err := c.ShouldBindJSON(dbModify); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	dbByte, err := vjson.Marshal(dbModify)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	dbStr := string(dbByte)
	log.Info("dbModify is %s", dbStr)
	if dbModify.DbName == "" || dbModify.SpaceName == "" {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("dbModify info incorrect [%s]", dbStr)))
	}
	if err := cluster.masterService.ChangeReplica(ctx.(context.Context), dbModify); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("[%s] failed ChangeReplicas,err is %v", dbStr, err)))
	} else {
		httphelper.New(c).JsonSuccess(fmt.Sprintf("[%s] success ChangeReplicas!", dbStr))
	}
}

func (ca *clusterAPI) changeMember(c *gin.Context) {
	cm := &entity.ChangeMembers{}

	if err := c.ShouldBindJSON(cm); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if err := ca.masterService.ChangeMembers(c, cm); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(nil)
	}
}
