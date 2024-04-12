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
	"github.com/vearch/vearch/internal/client"
	"github.com/vearch/vearch/internal/config"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/monitor"
	"github.com/vearch/vearch/internal/pkg/errutil"
	"github.com/vearch/vearch/internal/pkg/ginutil"
	"github.com/vearch/vearch/internal/pkg/log"
	"github.com/vearch/vearch/internal/pkg/netutil"
	"github.com/vearch/vearch/internal/pkg/server/vearchhttp"
	"github.com/vearch/vearch/internal/pkg/vjson"
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
	group.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.updateSpace, dh.TimeOutEndHandler)

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
	layer["cluster_name"] = config.Conf().Global.Name
	layer["cluster_uuid"] = ""
	layer["version"] = versionLayer
	layer["tagline"] = ""

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(layer)
}

// cleanLock lock for admin, when space locked, waring make sure not create space ing
func (ca *clusterAPI) cleanLock(c *gin.Context) {
	removed := make([]string, 0, 1)

	if keys, _, err := ca.masterService.Master().Store.PrefixScan(c, entity.PrefixLock); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		for _, key := range keys {
			if err := ca.masterService.Master().Store.Delete(c, string(key)); err != nil {
				ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
				return
			}
			removed = append(removed, string(key))
		}
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(removed)
	}
}

// for ps startup to register self and get ip response
func (ca *clusterAPI) register(c *gin.Context) {
	ip := c.ClientIP()
	log.Debug("register from: %v", ip)
	clusterName := c.Query("clusterName")
	nodeID := entity.NodeID(cast.ToInt64(c.Query("nodeID")))

	if clusterName == "" || nodeID == 0 {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("param err must has clusterName AND nodeID"))
		return
	}

	if clusterName != config.Conf().Global.Name {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("cluster name not same, please check"))
		return
	}

	// if node id is already existed, return failed
	if err := ca.masterService.IsExistNode(c, nodeID, ip); err != nil {
		log.Debug("nodeID[%d] exist %v", nodeID, err)
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	server, err := ca.masterService.registerServerService(c, ip, nodeID)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(server)
}

// for router startup to register self and get ip response
func (ca *clusterAPI) registerRouter(c *gin.Context) {
	ip := c.ClientIP()
	log.Debug("register from: %s", ip)
	clusterName := c.Query("clusterName")

	if clusterName != config.Conf().Global.Name {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("cluster name not same ,please check"))
		return
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(ip)
}

// when partition leader got it will register self to this api
func (ca *clusterAPI) registerPartition(c *gin.Context) {
	partition := &entity.Partition{}

	if err := c.ShouldBindJSON(partition); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	partition.UpdateTime = time.Now().UnixNano()

	if err := ca.masterService.registerPartitionService(c, partition); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
	}
}

func (ca *clusterAPI) createDB(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("createDB", startTime)
	dbName := c.Param(dbName)
	db := &entity.DB{Name: dbName}

	log.Debug("create db: %s", db.Name)

	if err := ca.masterService.createDBService(c, db); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(db)
	}
}

func (ca *clusterAPI) deleteDB(c *gin.Context) {
	log.Debug("delete db, db: %s", c.Param(dbName))
	db := c.Param(dbName)

	if err := ca.masterService.deleteDBService(c, db); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
	}
}

func (ca *clusterAPI) getDB(c *gin.Context) {
	db := c.Param(dbName)
	if db == "" {
		if dbs, err := ca.masterService.queryDBs(c); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		} else {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(dbs)
		}
	} else {
		if db, err := ca.masterService.queryDBService(c, db); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		} else {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(db)
		}
	}
}

func (ca *clusterAPI) modifyDB(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	dbModify := &entity.DBModify{}
	if err := c.ShouldBindJSON(dbModify); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	if db, err := ca.masterService.updateDBIpList(ctx.(context.Context), dbModify); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(db)
	}
}

func (ca *clusterAPI) createSpace(c *gin.Context) {
	log.Debug("create space, db: %s", c.Param(dbName))

	dbName := c.Param(dbName)

	space := &entity.Space{}
	if err := c.ShouldBindJSON(space); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
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
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	if space.ReplicaNum <= 0 {
		space.ReplicaNum = 1
	}

	// check index name is ok
	if err := space.Validate(); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	space.Version = 1 // first start with 1

	if err := ca.masterService.createSpaceService(c, dbName, space); err != nil {
		log.Error("createSpaceService err: %v", err)
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(space)
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
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
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
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	if spaceName != "" {
		if space, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		} else {
			spaceInfo := &entity.SpaceInfo{}
			spaceInfo.DbName = dbName
			spaceInfo.SpaceName = spaceName
			spaceInfo.Schema = &entity.SpaceSchema{
				Index:  space.Index,
				Fields: space.Fields,
			}
			spaceInfo.PartitionNum = space.PartitionNum
			spaceInfo.ReplicaNum = space.ReplicaNum
			if err := ca.masterService.describeSpaceService(c, space, spaceInfo, detail_info); err != nil {
				ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
			} else {
				ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(spaceInfo)
			}
		}
	} else {
		if spaces, err := ca.masterService.Master().QuerySpaces(c, dbID); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		} else {
			spaceInfos := make([]*entity.SpaceInfo, 0, len(spaces))
			for _, space := range spaces {
				var spaceInfo = &entity.SpaceInfo{}
				spaceInfo.DbName = dbName
				spaceInfo.SpaceName = space.Name
				spaceInfo.Schema = &entity.SpaceSchema{
					Index:  space.Index,
					Fields: space.Fields,
				}
				spaceInfo.PartitionNum = space.PartitionNum
				spaceInfo.ReplicaNum = space.ReplicaNum
				if err := ca.masterService.describeSpaceService(c, space, spaceInfo, detail_info); err != nil {
					ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
				} else {
					spaceInfos = append(spaceInfos, spaceInfo)
				}
			}
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(spaceInfos)
		}
	}
}

func (ca *clusterAPI) updateSpace(c *gin.Context) {
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)

	space := &entity.Space{Name: spaceName}

	if err := c.ShouldBindJSON(space); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if spaceResult, err := ca.masterService.updateSpaceService(c, dbName, spaceName, space); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)

	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(spaceResult)
	}
}

func (ca *clusterAPI) createAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if _, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	alias := &entity.Alias{Name: aliasName, DbName: dbName, SpaceName: spaceName}

	log.Debug("create alias: %s, dbName: %s, spaceName: %s", aliasName, dbName, spaceName)

	if err := ca.masterService.createAliasService(c, alias); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(alias)
	}
}

func (ca *clusterAPI) deleteAlias(c *gin.Context) {
	log.Debug("delete alias: %s", c.Param(aliasName))
	aliasName := c.Param(aliasName)

	if err := ca.masterService.deleteAliasService(c, aliasName); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
	}
}

func (ca *clusterAPI) getAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	if aliasName == "" {
		if alias, err := ca.masterService.queryAllAlias(c); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		} else {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(alias)
		}
	} else {
		if alias, err := ca.masterService.queryAliasService(c, aliasName); err != nil {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		} else {
			ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(alias)
		}
	}
}

func (ca *clusterAPI) modifyAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	if _, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	alias := &entity.Alias{
		Name:      aliasName,
		DbName:    dbName,
		SpaceName: spaceName,
	}
	if err := ca.masterService.updateAliasService(c, alias); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(alias)
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
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(cfg)
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
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}

	if cacheCfg.CacheModels == nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("engine config [%+v] is error", string(data)))
		return
	}
	if err := ca.masterService.ModifyEngineCfg(c, dbName, spaceName, cacheCfg); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(cacheCfg)
	}
}

// serverList list servers
func (ca *clusterAPI) serverList(c *gin.Context) {
	servers, err := ca.masterService.Master().QueryServers(c)

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

		partitionInfos, err := client.PartitionInfos(server.RpcAddr())
		if err != nil {
			serverInfo["error"] = err.Error()
		} else {
			serverInfo["partitions"] = partitionInfos
		}
		serverInfos = append(serverInfos, serverInfo)
	}

	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(map[string]interface{}{"servers": serverInfos, "count": len(servers)})
}

// routerList list router
func (ca *clusterAPI) routerList(c *gin.Context) {
	if routerIPs, err := ca.masterService.Master().QueryRouter(c, config.Conf().Global.Name); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(routerIPs)
	}
}

// partitionList list partition
func (ca *clusterAPI) partitionList(c *gin.Context) {
	partitions, err := ca.masterService.Master().QueryPartitions(c)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(partitions)
	}
}

// list fail servers
func (cluster *clusterAPI) FailServerList(c *gin.Context) {

	ctx, _ := c.Get(vearchhttp.Ctx)

	failServers, err := cluster.masterService.Master().QueryAllFailServer(ctx.(context.Context))

	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(map[string]interface{}{"fail_servers": failServers, "count": len(failServers)})
}

// clear fail server by nodeID
func (cluster *clusterAPI) FailServerClear(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	nodeID := c.Param(NodeID)
	if nodeID == "" {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("param err must has nodeId"))
		return
	}
	id, err := strconv.ParseUint(nodeID, 10, 64)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("nodeId err"))
		return
	}
	err = cluster.masterService.Master().DeleteFailServerByNodeID(ctx.(context.Context), id)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(map[string]interface{}{"nodeID": nodeID})
}

// clear task
func (cluster *clusterAPI) CleanTask(c *gin.Context) {
	CleanTask(cluster.server)
	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess("clean task success!")
}

// remove etcd meta about the nodeID
func (cluster *clusterAPI) RemoveServerMeta(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	rfs := &entity.RecoverFailServer{}
	if err := c.ShouldBindJSON(rfs); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	// get nodeID
	nodeID := rfs.FailNodeID
	// ipAddr
	ipAdd := rfs.FailNodeAddr
	if nodeID == 0 && ipAdd == "" {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("param err must has fail_node_id or fail_node_addr"))
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
				ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
				return
			}
		}
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("can't find server [%v]", failServer))
		return
	}
	ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(fmt.Sprintf("nodeid [%d], server [%v] remove node success!", nodeID, failServer))
}

// recover the failserver by a newserver
func (cluster *clusterAPI) RecoverFailServer(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	rs := &entity.RecoverFailServer{}
	if err := c.ShouldBindJSON(rs); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	rsStr := vjson.ToJsonString(rs)
	log.Info("RecoverFailServer is %s,", rsStr)
	if err := cluster.masterService.RecoverFailServer(ctx.(context.Context), rs); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("%s failed recover,err is %v", rsStr, err))
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(fmt.Sprintf("%s success recover!", rsStr))
	}
}

// change replicas by dbname and spaceName
func (cluster *clusterAPI) ChangeReplicas(c *gin.Context) {
	ctx, _ := c.Get(vearchhttp.Ctx)
	dbModify := &entity.DBModify{}
	if err := c.ShouldBindJSON(dbModify); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	dbByte, err := vjson.Marshal(dbModify)
	if err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	dbStr := string(dbByte)
	log.Info("dbModify is %s", dbStr)
	if dbModify.DbName == "" || dbModify.SpaceName == "" {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("dbModify info incorrect [%s]", dbStr))
	}
	if err := cluster.masterService.ChangeReplica(ctx.(context.Context), dbModify); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(fmt.Errorf("[%s] failed ChangeReplicas,err is %v", dbStr, err))
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(fmt.Sprintf("[%s] success ChangeReplicas!", dbStr))
	}
}

func (ca *clusterAPI) changeMember(c *gin.Context) {
	cm := &entity.ChangeMembers{}

	if err := c.ShouldBindJSON(cm); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
		return
	}
	if err := ca.masterService.ChangeMembers(c, cm); err != nil {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplyError(err)
	} else {
		ginutil.NewAutoMehtodName(c).SendJsonHttpReplySuccess(nil)
	}
}
