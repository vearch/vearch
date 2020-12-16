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
	"encoding/json"
	"fmt"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/router/document/resp"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/gorillautil"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/tiglabs/raft/proto"
	"github.com/vearch/vearch/util/cbjson"

	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/monitor"
	"github.com/vearch/vearch/util/server/vearchhttp"

	"github.com/vearch/vearch/util/netutil"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util/log"
)

const (
	DB            = "db"
	dbName        = "db_name"
	spaceName     = "space_name"
	headerAuthKey = "Authorization"
	NodeID        = "node_id"
)

type MasterClusterAPI struct {
	masterService *masterService
	Dh            *vearchhttp.BaseHandler
}

func (handler *DocumentHandler) GorillaExport(masterService *masterService) error {
	masterApi := &MasterClusterAPI{masterService : masterService}
	// cluster handler
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, handler.handleRouterInfo}, nil)

	//cluster handler
	handler.httpServer.HandlesMethods([]string{http.MethodGet},  "/clean_lock", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.cleanLock},nil )


	//db,servers handler
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/server", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.serverList}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/db", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.dbList}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/space", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.spaceList}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/partition", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.partitionList}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/list/router", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.routerList}, nil)

	//partition register
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/register", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.register}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/register_partition", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.registerPartition}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/register_router", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.registerRouter}, nil)

	//db handler
	handler.httpServer.HandlesMethods([]string{http.MethodPut}, "/db/_create", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.createDB}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodDelete}, fmt.Sprintf("/db/{%s}", DB), []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.deleteDB}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/db/{%s}", DB), []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.getDB}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/db/modify", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.modifyDB}, nil)

	//space handler
	handler.httpServer.HandlesMethods([]string{http.MethodPut}, fmt.Sprintf("/space/{%s}/_create", dbName), []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.createSpace}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/space/{%s}/{%s}", dbName, spaceName), []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.getSpace}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodDelete}, fmt.Sprintf("/space/{%s}/{%s}", dbName, spaceName), []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.deleteSpace}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/space/{%s}/{%s}", dbName, spaceName), []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.updateSpace}, nil)

	//partition handler
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/partition/change_member", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.ChangeMember}, nil)

	//schedule
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/schedule/recover_server", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.RecoverFailServer}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/schedule/change_replicas", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.ChangeReplicas}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, "/schedule/fail_server/list", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.FailServerList}, nil)
	handler.httpServer.HandlesMethods([]string{http.MethodDelete}, fmt.Sprintf("/schedule/fail_server/{%s}", NodeID), []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.FailServerClear}, nil)

	//remove server metadata
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, "/meta/remove_server", []netutil.HandleContinued{handler.handleTimeout, masterApi.Auth, masterApi.RemoveServerMeta}, nil)

	return nil
}

func (mApi *MasterClusterAPI) handleClusterInfo(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {

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
	return ctx, true
}

//cleanLock lock for admin , when space locked , waring make sure not create space ing
func (mApi *MasterClusterAPI) cleanLock(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	removed := make([]string, 0, 1)

	if keys, _, err := mApi.masterService.Master().Store.PrefixScan(ctx, entity.PrefixLock); err != nil {
		log.Errorf("cleanLock failed, err: [%s]", err.Error())
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		for _, key := range keys {
			if err := mApi.masterService.Master().Store.Delete(ctx, string(key)); err != nil {
				gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
				return ctx, true
			}
			removed = append(removed, string(key))
		}
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess("")
	}
	return ctx, true
}

//for ps startup to register self and get ip response
func (mApi *MasterClusterAPI) register(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	// var r *http.Request
	ip := netutil.ClientIP(r)
	log.Debug("register from: %v", ip)
	log.Debug("params is [+%v]", params)
	head := setRequestHead(params, r)
	clusterName := head.Params["clusterName"]
	nodeID := entity.NodeID(cast.ToInt64(head.Params["nodeID"]))
	log.Debug("nodeId is [+%v], clusterName is [%+v] ", nodeID, clusterName)
	if clusterName == "" || nodeID == 0 {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(
			fmt.Errorf("param err must has clusterName AND nodeID"))
		return ctx, true
	}

	if clusterName != config.Conf().Global.Name {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(
			fmt.Errorf("cluster name not same ,please check"))
		return ctx, true
	}

	server, err := mApi.masterService.registerServerService(ctx, ip, nodeID)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}
	gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(server)
	return ctx, true
}

//for router startup to register self and get ip response
func (mApi *MasterClusterAPI) registerRouter(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	ip := netutil.ClientIP(r)
	log.Debug("register from: %s", ip)
	clusterName := params.ByName("clusterName")

	if clusterName != config.Conf().Global.Name {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(
			fmt.Errorf("cluster name not same ,please check"))
		return ctx, true
	}
	gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(ip)
	return ctx, true
}

//when partition leader got it will register self to this api
func (mApi *MasterClusterAPI) registerPartition(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	partition := &entity.Partition{}
	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, partition)
	if err != nil {
		err = fmt.Errorf(" param convert json err: [%s]", string(reqBody))
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(
			fmt.Errorf("cluster name not same ,please check"))
		return ctx, true
	}

	partition.UpdateTime = time.Now().UnixNano()

	if err := mApi.masterService.registerPartitionService(ctx, partition); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess("")
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) createDB(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("createDB", startTime)
	db := &entity.DB{}

	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, db)
	if err != nil {
		err = fmt.Errorf(" param convert json err: [%s]", string(reqBody))
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}
	log.Debug("create db: %s", db.Name)
	if err := mApi.masterService.createDBService(ctx, db); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(db)
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) deleteDB(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {

	log.Debug("delete db, db: %s", params.ByName(DB))
	db := params.ByName(DB)

	if err := mApi.masterService.deleteDBService(ctx, db); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(nil)
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) getDB(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	db := params.ByName(DB)

	if db, err := mApi.masterService.queryDBService(ctx, db); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(db)
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) modifyDB(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbModify := &entity.DBModify{}
	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, dbModify)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}

	if db, err := mApi.masterService.updateDBIpList(ctx.(context.Context), dbModify); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(db)
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) createSpace(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {

	log.Debug("create space, db: %s",  params.ByName(dbName))
	dbName := params.ByName(dbName)

	space := &entity.Space{}

	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, space)
	if err != nil {
		log.Debug("create space, space: %s, err: %s", reqBody, err.Error())
		log.Error("parse space settings err: %v", err)
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}


	if space.PartitionNum <= 0 {
		space.PartitionNum = 1
	}

	if space.ReplicaNum <= 0 {
		space.ReplicaNum = 1
	}

	if space.Engine == nil {
		space.Engine = entity.NewDefaultEngine()
	}

	//check engine name and DynamicSchema is ok
	if err := space.Validate(); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}

	space.Version = 1 //first start with 1

	if err := mApi.masterService.createSpaceService(ctx, dbName, space); err != nil {
		log.Error("createSpaceService err: %v", err)
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(space)
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) deleteSpace(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	log.Debug("delete space, db: %s, space: %s", params.ByName(dbName), params.ByName(spaceName))
	dbName := params.ByName(dbName)
	sapceName := params.ByName(spaceName)

	if err := mApi.masterService.deleteSpaceService(ctx, dbName, sapceName); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess("")
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) getSpace(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(dbName)
	sapceName := params.ByName(spaceName)

	dbID, err := mApi.masterService.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}

	if space, err := mApi.masterService.Master().QuerySpaceByName(ctx, dbID, sapceName); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(space)
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) updateSpace(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(dbName)
	sapceName := params.ByName(spaceName)

	space := &entity.Space{Name: sapceName}

	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, space)
	if err != nil {
		log.Debug("updateSpace space, space: %s, err: %s", reqBody, err.Error())
		log.Error("parse space settings err: %v", err)
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}

	if spaceResult, err := mApi.masterService.updateSpaceService(ctx, dbName, sapceName, space); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(spaceResult)
	}
	return ctx, true
}

//serverList list servers
func (mApi *MasterClusterAPI) serverList(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	servers, err := mApi.masterService.Master().QueryServers(ctx)

	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}

	head := setRequestHead(params, r)

	ids := head.Params["ids"]

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

	gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(map[string]interface{}{"servers": serverInfos, "count": len(servers)})

	return ctx, true
}

//dbList list db
func (mApi *MasterClusterAPI) dbList(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	if dbs, err := mApi.masterService.queryDBs(ctx); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(dbs)
	}
	return ctx, true
}

//routerList list router
func (mApi *MasterClusterAPI) routerList(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	if routerIPs, err := mApi.masterService.Master().QueryRouter(ctx, config.Conf().Global.Name); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(routerIPs)
	}
	return ctx, true
}

//list space
func (mApi *MasterClusterAPI) spaceList(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	head := setRequestHead(params, r)
	db := head.Params[DB]
	if db == "" {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(fmt.Errorf("can find param in url ?db=[dbName or dbId]"))
		return ctx, true
	}

	var dbId entity.DBID
	if unicode.IsNumber([]rune(db)[0]) {
		id, err := cast.ToInt64E(db)
		if err != nil {
			gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
			return ctx, true
		}
		dbId = entity.DBID(id)
	} else {
		id, err := mApi.masterService.Master().QueryDBName2Id(ctx, db)
		if err != nil {
			gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
			return ctx, true
		}
		dbId = id
	}

	if dbs, err := mApi.masterService.Master().QuerySpaces(ctx, dbId); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(dbs)
	}
	return ctx, true
}

//partitionList list partition
func (mApi *MasterClusterAPI) partitionList(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	partitions, err := mApi.masterService.Master().QueryPartitions(ctx)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(partitions)
	}
	return ctx, true
}

//FailServerList : list fail servers
func (mApi *MasterClusterAPI) FailServerList(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {

	failServers, err := mApi.masterService.Master().QueryAllFailServer(ctx)

	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	}
	gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(map[string]interface{}{"fail_servers": failServers, "count": len(failServers)})
	return ctx, true
}

//FailServerClear : clear fail server by nodeID
func (mApi *MasterClusterAPI) FailServerClear(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	head := setRequestHead(params, r)
	nodeID := head.Params[NodeID]
	if nodeID == "" {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(fmt.Errorf("param err must has nodeId"))
		return ctx, true
	}
	id, err := strconv.ParseUint(nodeID, 10, 64)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(fmt.Errorf("nodeId err"))
		return ctx, true
	}
	err = mApi.masterService.Master().DeleteFailServerByNodeID(ctx.(context.Context), id)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}
	gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(map[string]interface{}{"nodeID": nodeID})
	return ctx, true
}

// RemoveServerMeta : remove etcd meta about the nodeID
func (mApi *MasterClusterAPI) RemoveServerMeta(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool)  {

	rfs := &entity.RecoverFailServer{}

	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, rfs)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
	}

	// get nodeID
	nodeID := rfs.FailNodeID
	// ipAddr
	ipAdd := rfs.FailNodeAddr
	if nodeID == 0 && ipAdd == "" {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(fmt.Errorf(" has fail_node_id or fail_node_addr"))
	}
	log.Debug("RemoveServerMeta info is %+v", rfs)
	//get failServer
	var failServer *entity.FailServer
	if nodeID > 0 {
		failServer = mApi.masterService.Master().QueryFailServerByNodeID(ctx.(context.Context), nodeID)
	}
	//if nodeId can't get server info
	if failServer == nil && ipAdd != "" {
		failServer = mApi.masterService.Master().QueryServerByIPAddr(ctx.(context.Context), ipAdd)
	}
	//get all partition
	if failServer != nil && failServer.Node != nil {
		for _, pid := range failServer.Node.PartitionIds {
			// begin clear pid in nodeID
			cm := &entity.ChangeMember{}
			cm.NodeID = failServer.ID
			cm.PartitionID = pid
			cm.Method = proto.ConfRemoveNode
			log.Debug("begin  ChangeMember %+v", cm)
			err := mApi.masterService.ChangeMember(ctx.(context.Context), cm)
			if err != nil {
				log.Error("ChangePartitionMember [%+v] err is %s", cm, err.Error())
				gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
			}
		}
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(fmt.Errorf("server [%v] can't find", failServer))
	}

	gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(fmt.Sprintf("nodeid [%d], "+
		"server [%v] remove node success!", nodeID, failServer))
	return ctx, true
}

//recover the failserver by a newserver
func (mApi *MasterClusterAPI) RecoverFailServer(ctx context.Context, w http.ResponseWriter,
r *http.Request, params netutil.UriParams) (context.Context, bool) {
	rs := &entity.RecoverFailServer{}

	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, rs)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}

	rsStr := cbjson.ToJsonString(rs)
	log.Info("RecoverFailServer is %s,", rsStr)
	if err := mApi.masterService.RecoverFailServer(ctx.(context.Context), rs); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(fmt.Errorf("%s failed recover,err is %v", rsStr, err))
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(fmt.Sprintf("%s success recover!", rsStr))
	}
	return ctx, true
}

//change replicas by dbname and spaceName
func (mApi *MasterClusterAPI) ChangeReplicas(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbModify := &entity.DBModify{}

	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, dbModify)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}

	dbByte, err := json.Marshal(dbModify)
	if err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, true
	}
	dbStr := string(dbByte)
	log.Info("dbModify is %s", dbStr)
	if dbModify.DbName == "" || dbModify.SpaceName == "" {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(fmt.Errorf("dbModify info incorrect [%s]", dbStr))
		return ctx, true
	}
	if err := mApi.masterService.ChangeReplica(ctx.(context.Context), dbModify); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(fmt.Errorf("[%s] failed ChangeReplicas,err is %v",
			dbStr, err))
	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(fmt.Sprintf("[%s] success ChangeReplicas!"))
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) ChangeMember(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	cm := &entity.ChangeMember{}

	reqBody, err := netutil.GetReqBody(r)
	err = cbjson.Unmarshal(reqBody, cm)
	if err != nil {
		resp.SendError(ctx, w, 500, err.Error())
		return ctx, true
	}

	if err := mApi.masterService.ChangeMember(ctx, cm); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)

	} else {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplySuccess(nil)
	}
	return ctx, true
}

func (mApi *MasterClusterAPI) Auth(ctx context.Context, w http.ResponseWriter,
	r *http.Request, params netutil.UriParams) (context.Context, bool) {
	if err := Auth(ctx, w, r); err != nil {
		gorillautil.NewAutoMehtodName(ctx, w).SendJsonHttpReplyError(err)
		return ctx, false
	}
	return ctx, true
}

// Auth valid whether username is root and password equal config
func Auth(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if config.Conf().Global.SkipAuth {
		return nil
	}
	headerData := r.Header.Get("Authorization")
	username, password, err := util.AuthDecrypt(headerData)
	if username != "root" || password != config.Conf().Global.Signkey {
		return vearchpb.NewError(vearchpb.ErrorEnum_AUTHENTICATION_FAILED, err)
	}
	return nil
}
