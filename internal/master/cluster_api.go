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
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
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
	"github.com/vearch/vearch/v3/internal/entity/response"
	"github.com/vearch/vearch/v3/internal/monitor"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/netutil"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

const (
	paramDb             = "db"
	paramSpace          = "space"
	paramDetail         = "detail"
	paramDbName         = "db_name"
	paramSpaceName      = "space_name"
	paramAliasName      = "alias_name"
	paramUserName       = "user_name"
	paramRoleName       = "role_name"
	paramMemberId       = "member_id"
	paramPeerAddrs      = "peer_addrs"
	paramHeaderAuthKey  = "Authorization"
	paramNodeID         = "node_id"
	paramRequestID      = "X-Request-Id"
	defaultResourceName = "default"
)

type clusterAPI struct {
	router        *gin.Engine
	masterService *masterService
	server        *Server
}

// handleError handles different types of errors and returns appropriate HTTP responses
func (ca *clusterAPI) handleError(c *gin.Context, err error) int {
	httpCode := http.StatusOK
	if err == nil {
		return httpCode
	}

	// Check if it's a VearchErr and handle specific error types
	if vErr, ok := err.(*vearchpb.VearchErr); ok {
		switch vErr.GetError().Code {
		case vearchpb.ErrorEnum_DB_EXIST, vearchpb.ErrorEnum_SPACE_EXIST, vearchpb.ErrorEnum_ALIAS_EXIST,
			vearchpb.ErrorEnum_USER_EXIST, vearchpb.ErrorEnum_ROLE_EXIST:
			httpCode = response.New(c).JsonError(errors.NewErrUnprocessable(err))
		case vearchpb.ErrorEnum_DB_NOT_EXIST, vearchpb.ErrorEnum_SPACE_NOT_EXIST, vearchpb.ErrorEnum_ALIAS_NOT_EXIST,
			vearchpb.ErrorEnum_USER_NOT_EXIST, vearchpb.ErrorEnum_ROLE_NOT_EXIST, vearchpb.ErrorEnum_PARTITION_SERVER_NOT_EXIST:
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		case vearchpb.ErrorEnum_PARAM_ERROR, vearchpb.ErrorEnum_CONFIG_ERROR:
			httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		case vearchpb.ErrorEnum_AUTHENTICATION_FAILED:
			httpCode = response.New(c).JsonError(errors.NewErrUnauthorized(err))
		default:
			httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		}
	} else {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	}
	return httpCode
}

func BasicAuthMiddleware(masterService *masterService) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			err := fmt.Errorf("auth header is empty")
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Basic" {
			err := fmt.Errorf("auth header type is invalid")
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		credentials := strings.SplitN(string(decoded), ":", 2)
		if len(credentials) != 2 {
			err := fmt.Errorf("auth header credentials is invalid")
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		user, err := masterService.Role().QueryUserWithPassword(c, credentials[0], true)
		if err != nil {
			ferr := fmt.Errorf("auth header user %s is invalid", credentials[0])
			response.New(c).JsonError(errors.NewErrUnauthorized(ferr))
			c.Abort()
			return
		}
		if *user.Password != credentials[1] {
			err := fmt.Errorf("auth header password is invalid")
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		role, err := masterService.Role().QueryRole(c, user.Role.Name)
		if err != nil {
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}
		endpoint := c.FullPath()
		method := c.Request.Method
		if err := role.HasPermissionForResources(endpoint, method); err != nil {
			response.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		c.Next()
	}
}

func TimeoutMiddleware(defaultTimeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		timeoutStr := c.Query("timeout")
		var timeout time.Duration
		if timeoutStr != "" {
			if t, err := strconv.Atoi(timeoutStr); err == nil {
				timeout = time.Duration(t) * time.Millisecond
			} else {
				msg := fmt.Sprintf("timeout[%s] param parse to int failed, err: %s", timeoutStr, err.Error())
				err := errors.NewErrBadRequest(fmt.Errorf(msg))
				log.Error(msg)
				c.JSON(err.HttpCode(),
					gin.H{
						"request_id": c.GetHeader(paramRequestID),
						"code":       err.Code(),
						"msg":        err.Msg()})
				c.Abort()
				return
			}
		} else {
			timeout = defaultTimeout
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		c.Request = c.Request.WithContext(ctx)
		resultCh := make(chan *response.Response, 1)

		go func() {
			c.Next()
			httpResponse, exist := c.Get("httpResponse")
			var httpResp *response.Response
			if exist && httpResponse != nil {
				if _, ok := httpResponse.(*response.Response); ok {
					httpResp = httpResponse.(*response.Response)
				}
			}

			if httpResp == nil {
				httpReply := &response.HttpReply{
					Code:      int(vearchpb.ErrorEnum_INTERNAL_ERROR),
					RequestId: c.GetHeader(paramRequestID),
					Msg:       "get response data error",
				}
				res := &response.Response{}
				res.SetHttpReply(httpReply)
				res.SetHttpStatus(http.StatusInternalServerError)
			}
			resultCh <- httpResp
		}()

		select {
		case <-ctx.Done():
			c.JSON(http.StatusGatewayTimeout,
				gin.H{
					"request_id": c.GetHeader(paramRequestID),
					"code":       vearchpb.ErrorEnum_TIMEOUT,
					"msg":        "request timeout"})
			c.Abort()
		case res := <-resultCh:
			c.JSON(int(res.GetHttpStatus()), res.GetHttpReply())
		}
	}
}

func RecoveryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				var msg string
				switch r := r.(type) {
				case error:
					msg = r.Error()
				default:
					if str, err := cast.ToStringE(r); err != nil {
						msg = "Server internal error "
					} else {
						msg = str
					}
				}
				log.Error(msg)

				c.JSON(http.StatusInternalServerError, map[string]string{"message": msg})
				c.Abort()
			}
		}()
	}
}

func ExportToClusterHandler(router *gin.Engine, masterService *masterService, server *Server) {
	c := &clusterAPI{router: router, masterService: masterService, server: server}
	router.Use(RecoveryMiddleware())
	router.Use(TimeoutMiddleware(10 * time.Second))

	var groupAuth *gin.RouterGroup
	var group *gin.RouterGroup = router.Group("")
	if !config.Conf().Global.SkipAuth {
		groupAuth = router.Group("", BasicAuthMiddleware(masterService))
	} else {
		groupAuth = router.Group("")
	}

	group.GET("/", c.handleClusterInfo)

	// cluster handler
	groupAuth.GET("/clean_lock", c.cleanLock)

	// servers handler
	groupAuth.GET("/servers", c.serverList)

	// router  handler
	groupAuth.GET("/routers", c.routerList)

	// partition register, use internal so no need to auth
	group.POST("/register", c.register)
	group.POST("/register_partition", c.registerPartition)
	group.POST("/register_router", c.registerRouter)

	// db handler
	groupAuth.POST(fmt.Sprintf("/dbs/:%s", paramDbName), c.createDB)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s", paramDbName), c.getDB)
	groupAuth.GET("/dbs", c.getDB)
	groupAuth.DELETE(fmt.Sprintf("/dbs/:%s", paramDbName), c.deleteDB)
	groupAuth.PUT(fmt.Sprintf("/dbs/:%s", paramDbName), c.modifyDB)

	// space handler
	groupAuth.POST(fmt.Sprintf("/dbs/:%s/spaces", paramDbName), c.createSpace)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s/spaces/:%s", paramDbName, paramSpaceName), c.getSpace)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s/spaces", paramDbName), c.getSpace)
	groupAuth.DELETE(fmt.Sprintf("/dbs/:%s/spaces/:%s", paramDbName, paramSpaceName), c.deleteSpace)
	groupAuth.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", paramDbName, paramSpaceName), c.updateSpace)
	groupAuth.POST(fmt.Sprintf("/backup/dbs/:%s/spaces/:%s", paramDbName, paramSpaceName), c.backupSpace)
	groupAuth.POST(fmt.Sprintf("/backup/dbs/:%s", paramDbName), c.backupDb)

	// modify engine config handler
	groupAuth.POST("/config/:"+paramDbName+"/:"+paramSpaceName, c.modifySpaceConfig)
	groupAuth.GET("/config/:"+paramDbName+"/:"+paramSpaceName, c.getSpaceConfig)

	// modify query limit enabled config handler
	groupAuth.POST("/config/request_limit", c.modifyRequestLimitCfg)
	groupAuth.GET("/config/request_limit", c.getRequestLimitCfg)

	//modify and get memory limit config handler
	groupAuth.POST("/config/memory_limit", c.modifyMemoryLimitCfg)
	groupAuth.GET("/config/memory_limit", c.getMemoryLimitCfg)

	group.POST("/config/slow_search_isolation", c.modifySlowSearchIsolationCfg)
	group.GET("/config/slow_search_isolation", c.getSlowSearchIsolationCfg)

	// partition handler
	groupAuth.GET("/partitions", c.partitionList)
	groupAuth.POST("/partitions/change_member", c.changeMember)
	groupAuth.POST("/partitions/resource_limit", c.ResourceLimit)

	// schedule
	groupAuth.POST("/schedule/recover_server", c.RecoverFailServer)
	groupAuth.POST("/schedule/change_replicas", c.ChangeReplicas)
	groupAuth.GET("/schedule/fail_server", c.FailServerList)
	groupAuth.DELETE("/schedule/fail_server/:"+paramNodeID, c.FailServerClear)
	groupAuth.GET("/schedule/clean_task", c.CleanTask)

	// remove server metadata
	groupAuth.POST("/meta/remove_server", c.RemoveServerMeta)

	// alias handler
	groupAuth.POST(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", paramAliasName, paramDbName, paramSpaceName), c.createAlias)
	groupAuth.GET(fmt.Sprintf("/alias/:%s", paramAliasName), c.getAlias)
	groupAuth.GET("/alias", c.getAlias)
	groupAuth.DELETE(fmt.Sprintf("/alias/:%s", paramAliasName), c.deleteAlias)
	groupAuth.PUT(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", paramAliasName, paramDbName, paramSpaceName), c.modifyAlias)

	// user handler
	groupAuth.POST("/users", c.createUser)
	groupAuth.GET(fmt.Sprintf("/users/:%s", paramUserName), c.getUser)
	groupAuth.GET("/users", c.getUser)
	groupAuth.DELETE(fmt.Sprintf("/users/:%s", paramUserName), c.deleteUser)
	groupAuth.PUT("/users", c.updateUser)

	// role handler
	groupAuth.POST("/roles", c.createRole)
	groupAuth.GET(fmt.Sprintf("/roles/:%s", paramRoleName), c.getRole)
	groupAuth.GET("/roles", c.getRole)
	groupAuth.DELETE(fmt.Sprintf("/roles/:%s", paramRoleName), c.deleteRole)
	groupAuth.PUT("/roles", c.changeRolePrivilege)

	// cluster handler
	groupAuth.GET("/cluster/stats", c.stats)
	groupAuth.GET("/cluster/health", c.health)

	// members handler
	groupAuth.GET("/members", c.getMembers)
	groupAuth.GET("/members/stats", c.getMemberStatus)
	groupAuth.DELETE("/members", c.deleteMember)
	groupAuth.POST("/members", c.addMember)
}

// got every partition servers system info
func (ca *clusterAPI) stats(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleClusterStats"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	list, err := ca.masterService.Server().Stats(c)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	httpCode = response.New(c).JsonSuccess(list)
}

// cluster health in partition level
func (ca *clusterAPI) health(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleClusterHealth"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	dbName = c.Query(paramDb)
	spaceName = c.Query(paramSpace)
	detail := c.Query(paramDetail)

	result, err := ca.masterService.Partition().PartitionInfo(c, ca.masterService.DB(), ca.masterService.Space(), dbName, spaceName, detail)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	httpCode = response.New(c).JsonSuccess(result)
}

func (ca *clusterAPI) handleClusterInfo(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleClusterInfo"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	layer := map[string]any{
		"name": config.Conf().Global.Name,
		"version": map[string]any{
			"build_version": config.GetBuildVersion(),
			"build_time":    config.GetBuildTime(),
			"commit_id":     config.GetCommitID(),
		},
	}

	httpCode = response.New(c).JsonSuccess(layer)
}

// cleanLock lock for admin, when space locked, waring make sure not create space ing
func (ca *clusterAPI) cleanLock(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleCleanLock"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	removed := make([]string, 0, 1)

	if keys, _, err := ca.masterService.Master().Store.PrefixScan(c, entity.PrefixLock); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
	} else {
		for _, key := range keys {
			if err := ca.masterService.Master().Store.Delete(c, string(key)); err != nil {
				httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
				return
			}
			removed = append(removed, string(key))
		}
		httpCode = response.New(c).JsonSuccess(removed)
	}
}

// for ps startup to register self and get ip response
func (ca *clusterAPI) register(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleRegisterPartitionServer"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	ip := c.ClientIP()
	log.Debug("register from: %v", ip)
	clusterName := c.Query("clusterName")
	nodeID := entity.NodeID(cast.ToInt64(c.Query("nodeID")))

	if clusterName == "" || nodeID == 0 {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has clusterName AND nodeID")))
		return
	}

	if clusterName != config.Conf().Global.Name {
		httpCode = response.New(c).JsonError(errors.NewErrUnprocessable(fmt.Errorf("cluster[%s] name different, config cluster name is %s", clusterName, config.Conf().Global.Name)))
		return
	}

	// if node id is already existed, return failed
	if err := ca.masterService.Server().IsExistNode(c, nodeID, ip); err != nil {
		log.Debug("nodeID[%d] exist %v", nodeID, err)
		httpCode = response.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}

	server, err := ca.masterService.Server().RegisterServer(c, ip, nodeID)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	httpCode = response.New(c).JsonSuccess(server)
}

// for router startup to register self and get ip response
func (ca *clusterAPI) registerRouter(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleRegisterRouter"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	ip := c.ClientIP()
	log.Debug("register from: %s", ip)
	clusterName := c.Query("clusterName")

	if clusterName != config.Conf().Global.Name {
		httpCode = response.New(c).JsonError(errors.NewErrUnprocessable(fmt.Errorf("cluster name different, please check")))
		return
	}

	httpCode = response.New(c).JsonSuccess(ip)
}

// when partition leader got it will register self to this api
func (ca *clusterAPI) registerPartition(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleRegisterPartition"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	partition := &entity.Partition{}

	if err := c.ShouldBindJSON(partition); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	partition.UpdateTime = time.Now().UnixNano()

	if err := ca.masterService.Partition().RegisterPartition(c, partition); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(nil)
	}
}

func (ca *clusterAPI) createDB(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleCreateDB"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	dbName = c.Param(paramDbName)
	db := &entity.DB{Name: dbName}

	log.Debug("create db: %s", db.Name)

	if err := ca.masterService.DB().CreateDB(c, db); err != nil {
		httpCode = ca.handleError(c, err)
	} else {
		httpCode = response.New(c).JsonSuccess(db)
	}
}

func (ca *clusterAPI) deleteDB(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDeleteDB"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	log.Debug("delete db, db: %s", c.Param(paramDbName))
	dbName = c.Param(paramDbName)

	if err := ca.masterService.DB().DeleteDB(c, dbName); err != nil {
		httpCode = ca.handleError(c, err)
	} else {
		httpCode = response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getDB(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetDB"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	dbName = c.Param(paramDbName)
	if dbName == "" {
		if dbs, err := ca.masterService.DB().QueryDBs(c); err != nil {
			httpCode = ca.handleError(c, err)
		} else {
			httpCode = response.New(c).JsonSuccess(dbs)
		}
	} else {
		if db, err := ca.masterService.DB().QueryDB(c, dbName); err != nil {
			httpCode = ca.handleError(c, err)
		} else {
			httpCode = response.New(c).JsonSuccess(db)
		}
	}
}

func (ca *clusterAPI) modifyDB(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleModifyDB"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	dbModify := &entity.DBModify{}
	if err := c.ShouldBindJSON(dbModify); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	dbName = dbModify.DbName

	if db, err := ca.masterService.DB().UpdateDBIpList(c.Request.Context(), dbModify); err != nil {
		httpCode = ca.handleError(c, err)
	} else {
		httpCode = response.New(c).JsonSuccess(db)
	}
}

func (ca *clusterAPI) createSpace(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleCreateSpace"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	log.Debug("create space, db: %s", c.Param(paramDbName))

	dbName = c.Param(paramDbName)
	// set default refresh interval
	refreshInterval := int32(entity.DefaultRefreshInterval)
	enableIdCache := entity.DefaultEnableIdCache
	space := &entity.Space{
		RefreshInterval: &refreshInterval,
		EnableIdCache:   &enableIdCache,
		EnableRealtime:  &entity.DefalutEnableRealtime,
	}
	if err := c.ShouldBindJSON(space); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	spaceName = space.Name

	// set default resource name
	if space.ResourceName == "" {
		space.ResourceName = defaultResourceName
	}

	if space.PartitionNum <= 0 {
		space.PartitionNum = 1
	}

	if config.Conf().Global.LimitedReplicaNum && space.ReplicaNum < 3 {
		err := fmt.Errorf("LimitedReplicaNum is set and in order to ensure high availability replica should not be less than 3")
		log.Error(err.Error())
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if space.ReplicaNum <= 0 {
		space.ReplicaNum = 3
	}

	// check index name is ok
	if err := space.Validate(); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	space.Version = 1 // first start with 1

	if err := ca.masterService.Space().CreateSpace(c, ca.masterService.DB(), dbName, space); err != nil {
		log.Error("createSpace db: %s, space: %s, space id: %d, err: %s", dbName, space.Name, space.Id, err.Error())
		httpCode = ca.handleError(c, err)
		return
	}

	cfg, err := ca.masterService.Config().GetSpaceConfigByName(c, dbName, space.Name)
	if err != nil {
		log.Error("get engine config err: %s", err.Error())
		httpCode = ca.handleError(c, err)
		return
	}
	cfg.Id = space.Id
	cfg.DBId = space.DBId
	err = ca.masterService.Config().UpdateSpaceConfig(c, space, cfg)
	if err != nil {
		log.Error("update engine config err: %s", err.Error())
		httpCode = ca.handleError(c, err)
		return
	}
	httpCode = response.New(c).JsonSuccess(space)
}

func (ca *clusterAPI) deleteSpace(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDeleteSpace"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	log.Debug("delete space, db: %s, space: %s", c.Param(paramDbName), c.Param(paramSpaceName))
	dbName = c.Param(paramDbName)
	spaceName = c.Param(paramSpaceName)

	if err := ca.masterService.Space().DeleteSpace(c, ca.masterService.Alias(), dbName, spaceName); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getSpace(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetSpace"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	dbName = c.Param(paramDbName)
	spaceName = c.Param(paramSpaceName)
	detail := c.Query(paramDetail)
	detail_info := false
	if detail == "true" {
		detail_info = true
	}

	dbID, err := ca.masterService.Master().QueryDBName2ID(c, dbName)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	if spaceName != "" {
		if space, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			spaceInfo := &entity.SpaceInfo{}
			spaceInfo.DbName = dbName
			spaceInfo.SpaceName = spaceName
			spaceInfo.Schema = &entity.SpaceSchema{
				Fields: space.Fields,
			}
			spaceInfo.PartitionNum = space.PartitionNum
			spaceInfo.ReplicaNum = space.ReplicaNum
			spaceInfo.PartitionRule = space.PartitionRule
			if _, err := ca.masterService.Space().DescribeSpace(c, space, spaceInfo, detail_info); err != nil {
				httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
				return
			} else {
				httpCode = response.New(c).JsonSuccess(spaceInfo)
			}
		}
	} else {
		if spaces, err := ca.masterService.Master().QuerySpaces(c, dbID); err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
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
				spaceInfo.PartitionRule = space.PartitionRule
				if _, err := ca.masterService.Space().DescribeSpace(c, space, spaceInfo, detail_info); err != nil {
					httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
					return
				} else {
					spaceInfos = append(spaceInfos, spaceInfo)
				}
			}
			httpCode = response.New(c).JsonSuccess(spaceInfos)
		}
	}
}

func (ca *clusterAPI) updateSpace(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleUpdateSpace"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	dbName = c.Param(paramDbName)
	spaceName = c.Param(paramSpaceName)

	space := &entity.Space{
		Name: spaceName,
	}

	if err := c.ShouldBindJSON(space); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("updateSpace %+v", space)

	if spaceResult, err := ca.masterService.Space().UpdateSpace(c, ca.masterService.DB(), dbName, spaceName, space, ""); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(spaceResult)
	}
}

func (ca *clusterAPI) backupDb(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleBackupDB"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	var err error
	defer errutil.CatchError(&err)
	dbName = c.Param(paramDbName)
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	dbID, err := ca.masterService.Master().QueryDBName2ID(c, dbName)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	spaces, err := ca.masterService.Master().QuerySpaces(c, dbID)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	backup := &entity.BackupSpaceRequest{}
	err = json.Unmarshal(data, backup)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	res := &entity.BackupSpaceResponse{}
	for _, space := range spaces {
		res, err = ca.masterService.Backup().BackupSpace(c, ca.masterService.DB(), ca.masterService.Space(), ca.masterService.Config(), dbName, space.Name, backup)
		if err != nil {
			err = fmt.Errorf("backup space %s failed, err: %s", space.Name, err.Error())
			httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}
	}
	httpCode = response.New(c).JsonSuccess(res)
}

func (ca *clusterAPI) backupSpace(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleBackupSpace"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	var err error
	dbName = c.Param(paramDbName)
	spaceName = c.Param(paramSpaceName)
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("engine config json data is [%+v]", string(data))
	backup := &entity.BackupSpaceRequest{}
	err = json.Unmarshal(data, backup)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	res := &entity.BackupSpaceResponse{}

	res, err = ca.masterService.Backup().BackupSpace(c, ca.masterService.DB(), ca.masterService.Space(), ca.masterService.Config(), dbName, spaceName, backup)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httpCode = response.New(c).JsonSuccess(res)
	}
}

func (ca *clusterAPI) ResourceLimit(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleResourceLimit"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	resourceLimit := &entity.ResourceLimit{}
	if err := c.ShouldBindJSON(resourceLimit); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if resourceLimit.DbName != nil {
		dbName = *resourceLimit.DbName
	}
	if resourceLimit.SpaceName != nil {
		spaceName = *resourceLimit.SpaceName
	}

	if err := ca.masterService.Server().ResourceLimitService(c, ca.masterService.DB(), resourceLimit); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httpCode = response.New(c).JsonSuccess(resourceLimit)
	}
}

func (ca *clusterAPI) createAlias(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleCreateAlias"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	aliasName := c.Param(paramAliasName)
	dbName = c.Param(paramDbName)
	spaceName = c.Param(paramSpaceName)
	dbID, err := ca.masterService.Master().QueryDBName2ID(c, dbName)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}

	if _, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	alias := &entity.Alias{Name: aliasName, DbName: dbName, SpaceName: spaceName}

	log.Debug("create alias: %s, dbName: %s, spaceName: %s", aliasName, dbName, spaceName)

	if err := ca.masterService.Alias().CreateAlias(c, alias); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httpCode = response.New(c).JsonSuccess(alias)
	}
}

func (ca *clusterAPI) deleteAlias(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDeleteAlias"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	log.Debug("delete alias: %s", c.Param(paramAliasName))
	aliasName := c.Param(paramAliasName)

	if err := ca.masterService.Alias().DeleteAlias(c, aliasName); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httpCode = response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getAlias(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetAlias"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	aliasName := c.Param(paramAliasName)
	if aliasName == "" {
		if alias, err := ca.masterService.Alias().QueryAllAlias(c); err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
			return
		} else {
			httpCode = response.New(c).JsonSuccess(alias)
		}
	} else {
		if alias, err := ca.masterService.Alias().QueryAlias(c, aliasName); err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
			return
		} else {
			httpCode = response.New(c).JsonSuccess(alias)
		}
	}
}

func (ca *clusterAPI) modifyAlias(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleModifyAlias"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	aliasName := c.Param(paramAliasName)
	dbName = c.Param(paramDbName)
	spaceName = c.Param(paramSpaceName)
	dbID, err := ca.masterService.Master().QueryDBName2ID(c, dbName)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	if _, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}

	alias := &entity.Alias{
		Name:      aliasName,
		DbName:    dbName,
		SpaceName: spaceName,
	}
	if err := ca.masterService.Alias().UpdateAlias(c, alias); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httpCode = response.New(c).JsonSuccess(alias)
	}
}

func (ca *clusterAPI) createUser(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleCreateUser"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	user := &entity.User{}
	if err := c.ShouldBindJSON(user); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("create user: %s", user.Name)

	if err := ca.masterService.User().CreateUser(c, ca.masterService.Role(), user, true); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		// just return user name
		user_return := &entity.User{Name: user.Name}
		httpCode = response.New(c).JsonSuccess(user_return)
	}
}

func (ca *clusterAPI) deleteUser(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDeleteUser"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	log.Debug("delete user: %s", c.Param(paramUserName))
	name := c.Param(paramUserName)

	if err := ca.masterService.User().DeleteUser(c, ca.masterService.Role(), name); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httpCode = response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getUser(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetUser"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	name := c.Param(paramUserName)
	if name == "" {
		if users, err := ca.masterService.User().QueryAllUser(c, ca.masterService.Role()); err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
			return
		} else {
			httpCode = response.New(c).JsonSuccess(users)
		}
	} else {
		if user, err := ca.masterService.User().QueryUser(c, ca.masterService.Role(), name, true); err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
			return
		} else {
			httpCode = response.New(c).JsonSuccess(user)
		}
	}
}

func (ca *clusterAPI) updateUser(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleUpdateUser"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	user := &entity.User{}
	if err := c.ShouldBindJSON(user); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	auth_user := ""
	if !config.Conf().Global.SkipAuth {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			err := fmt.Errorf("auth header is empty")
			httpCode = response.New(c).JsonError(errors.NewErrUnauthorized(err))
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Basic" {
			err := fmt.Errorf("auth header type is invalid")
			httpCode = response.New(c).JsonError(errors.NewErrUnauthorized(err))
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrUnauthorized(err))
			return
		}

		credentials := strings.SplitN(string(decoded), ":", 2)
		if len(credentials) != 2 {
			err := fmt.Errorf("auth header credentials is invalid")
			httpCode = response.New(c).JsonError(errors.NewErrUnauthorized(err))
			return
		}
		auth_user = credentials[0]
	}

	if err := ca.masterService.User().UpdateUser(c, ca.masterService.Role(), user, auth_user); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(user)
	}
}

func (ca *clusterAPI) createRole(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleCreateRole"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	role := &entity.Role{}
	if err := c.ShouldBindJSON(role); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("create role: %s", role.Name)

	if err := ca.masterService.Role().CreateRole(c, role); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(role)
	}
}

func (ca *clusterAPI) deleteRole(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDeleteRole"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	log.Debug("delete role: %s", c.Param(paramRoleName))
	name := c.Param(paramRoleName)

	if err := ca.masterService.Role().DeleteRole(c, name); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getRole(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetRole"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	name := c.Param(paramRoleName)
	if name == "" {
		if roles, err := ca.masterService.Role().QueryAllRole(c); err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httpCode = response.New(c).JsonSuccess(roles)
		}
	} else {
		if role, err := ca.masterService.Role().QueryRole(c, name); err != nil {
			httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httpCode = response.New(c).JsonSuccess(role)
		}
	}
}

func (ca *clusterAPI) changeRolePrivilege(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleChangeRolePrivilege"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	role := &entity.Role{}
	if err := c.ShouldBindJSON(role); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	log.Debug("update role: %s privilege", role.Name)

	if new_role, err := ca.masterService.Role().ChangeRolePrivilege(c, role); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(new_role)
	}
}

func (ca *clusterAPI) getMembers(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetMembers"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	resp, err := ca.masterService.Client.Master().MemberList(context.Background())
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	response.New(c).JsonSuccess(resp)
}

func (ca *clusterAPI) getMemberStatus(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetMemberStatus"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	resp, err := ca.masterService.Client.Master().MemberStatus(context.Background())
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	httpCode = response.New(c).JsonSuccess(resp)
}

func (ca *clusterAPI) addMember(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleAddMember"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	addMemberRequest := &entity.AddMemberRequest{}

	if err := c.ShouldBindJSON(addMemberRequest); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	resp, err := ca.masterService.Member().AddMember(c, addMemberRequest.PeerAddrs)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	httpCode = response.New(c).JsonSuccess(resp)
}

func (ca *clusterAPI) deleteMember(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleDeleteMember"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	master := &entity.MemberInfoRequest{}
	if err := c.ShouldBindJSON(master); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	resp, err := ca.masterService.Member().RemoveMember(c, master)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	httpCode = response.New(c).JsonSuccess(resp)
}

// get engine config
func (ca *clusterAPI) getSpaceConfig(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetSpaceConfig"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	dbName = c.Param(paramDbName)
	spaceName = c.Param(paramSpaceName)
	if cfg, err := ca.masterService.Config().GetSpaceConfigByName(c, dbName, spaceName); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		httpCode = response.New(c).JsonSuccess(cfg)
	}
}

// modify engine config
func (ca *clusterAPI) modifySpaceConfig(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleModifySpaceConfig"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	var err error
	defer errutil.CatchError(&err)
	dbName = c.Param(paramDbName)
	spaceName = c.Param(paramSpaceName)
	spaceCfg := &entity.SpaceConfig{}

	if err := c.ShouldBindJSON(spaceCfg); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if err := ca.masterService.Config().ModifySpaceConfig(c, dbName, spaceName, spaceCfg); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(spaceCfg)
	}
}

// serverList list servers
func (ca *clusterAPI) serverList(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleServerList"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	var err error
	defer errutil.CatchError(&err)
	servers, err := ca.masterService.Master().QueryServers(c)

	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
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

	serverInfos := make([]map[string]any, 0, len(servers))

	for _, server := range servers {
		serverInfo := make(map[string]any)
		serverInfo["server"] = server

		partitionInfos, err := client.PartitionInfos(server.RpcAddr())
		if err != nil {
			serverInfo["error"] = err.Error()
		} else {
			serverInfo["partitions"] = partitionInfos
		}
		serverInfos = append(serverInfos, serverInfo)
	}

	httpCode = response.New(c).JsonSuccess(map[string]any{"servers": serverInfos, "count": len(servers)})
}

// routerList list router
func (ca *clusterAPI) routerList(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleRouterList"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	if routerIPs, err := ca.masterService.Master().QueryRouter(c, config.Conf().Global.Name); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		httpCode = response.New(c).JsonSuccess(routerIPs)
	}
}

// partitionList list partition
func (ca *clusterAPI) partitionList(c *gin.Context) {
	startTime := time.Now()
	operateName := "handlePartitionList"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	partitions, err := ca.masterService.Master().QueryPartitions(c)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		httpCode = response.New(c).JsonSuccess(partitions)
	}
}

// list fail servers
func (cluster *clusterAPI) FailServerList(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleFailServerList"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	failServers, err := cluster.masterService.Master().QueryAllFailServer(c.Request.Context())

	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	httpCode = response.New(c).JsonSuccess(map[string]any{"fail_servers": failServers, "count": len(failServers)})
}

// clear fail server by nodeID
func (cluster *clusterAPI) FailServerClear(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleFailServerClear"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	nodeID := c.Param(paramNodeID)
	if nodeID == "" {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has nodeId")))
		return
	}
	id, err := strconv.ParseUint(nodeID, 10, 64)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("nodeId err")))
		return
	}
	err = cluster.masterService.Master().DeleteFailServerByNodeID(c.Request.Context(), id)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	httpCode = response.New(c).JsonSuccess(map[string]any{"nodeID": nodeID})
}

// clear task
func (cluster *clusterAPI) CleanTask(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleCleanTask"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	CleanTask(cluster.server)
	httpCode = response.New(c).JsonSuccess("clean task success!")
}

// remove etcd meta about the nodeID
func (cluster *clusterAPI) RemoveServerMeta(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleRemoveServerMeta"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	rfs := &entity.RecoverFailServer{}
	if err := c.ShouldBindJSON(rfs); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	// get nodeID
	nodeID := rfs.FailNodeID
	// ipAddr
	ipAdd := rfs.FailNodeAddr
	if nodeID == 0 && ipAdd == "" {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has fail_node_id or fail_node_addr")))
		return
	}
	log.Debug("RemoveServerMeta info is %+v", rfs)
	// get failServer
	var failServer *entity.FailServer
	if nodeID > 0 {
		failServer = cluster.masterService.Master().QueryFailServerByNodeID(c.Request.Context(), nodeID)
	}
	// if nodeId can't get server info
	if failServer == nil && ipAdd != "" {
		failServer = cluster.masterService.Master().QueryServerByIPAddr(c.Request.Context(), ipAdd)
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
			err := cluster.masterService.Member().ChangeMember(c.Request.Context(), cluster.masterService.Space(), cm)
			if err != nil {
				log.Error("ChangePartitionMember [%+v] err is %s", cm, err.Error())
				httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
				return
			}
		}
	} else {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("can't find server [%v]", failServer)))
		return
	}
	httpCode = response.New(c).JsonSuccess(fmt.Sprintf("nodeid [%d], server [%v] remove node success!", nodeID, failServer))
}

// recover the failserver by a newserver
func (cluster *clusterAPI) RecoverFailServer(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleRecoverFailServer"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	rs := &entity.RecoverFailServer{}
	if err := c.ShouldBindJSON(rs); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	rsStr, err := json.Marshal(rs)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	log.Info("RecoverFailServer is %s,", rsStr)
	if err := cluster.masterService.Server().RecoverFailServer(c.Request.Context(), cluster.masterService.Space(), cluster.masterService.Member(), rs); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("%s failed recover, err is %v", rsStr, err)))
	} else {
		httpCode = response.New(c).JsonSuccess(fmt.Sprintf("%s success recover!", rsStr))
	}
}

// change replicas by dbname and spaceName
func (cluster *clusterAPI) ChangeReplicas(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleChangeReplicas"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	dbModify := &entity.DBModify{}
	if err := c.ShouldBindJSON(dbModify); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	dbName = dbModify.DbName
	spaceName = dbModify.SpaceName

	dbByte, err := json.Marshal(dbModify)
	if err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	dbStr := string(dbByte)
	log.Info("dbModify is %s", dbStr)
	if dbModify.DbName == "" || dbModify.SpaceName == "" {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("dbModify info incorrect [%s]", dbStr)))
		return
	}
	if err := cluster.masterService.Member().ChangeReplica(c.Request.Context(), cluster.masterService.DB(), cluster.masterService.Space(), dbModify); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("[%s] failed ChangeReplicas,err is %v", dbStr, err)))
	} else {
		httpCode = response.New(c).JsonSuccess(fmt.Sprintf("[%s] success ChangeReplicas!", dbStr))
	}
}

func (ca *clusterAPI) changeMember(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleChangeMember"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	cm := &entity.ChangeMembers{}

	if err := c.ShouldBindJSON(cm); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if err := ca.masterService.Member().ChangeMembers(c, ca.masterService.Space(), cm); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(nil)
	}
}

// get engine config
func (ca *clusterAPI) getRequestLimitCfg(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetRequestLimitCfg"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	if request_limit, err := ca.masterService.Config().GetRequestLimitCfg(c); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(request_limit)
	}
}

// modify engine config
func (ca *clusterAPI) modifyRequestLimitCfg(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleModifyRequestLimitCfg"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	rlc := &entity.RequestLimitCfg{}

	if err := c.ShouldBindJSON(rlc); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if err := ca.masterService.Config().ModifyRequestLimitCfg(c, rlc); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(rlc)
	}
}

// get memory limite config
func (ca *clusterAPI) getMemoryLimitCfg(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetMemoryLimitCfg"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	if memory_limit, err := ca.masterService.Config().GetMemoryLimitCfg(c); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(memory_limit)
	}
}

// modify memory limit config
func (ca *clusterAPI) modifyMemoryLimitCfg(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleModifyMemoryLimitCfg"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	mlc := &entity.MemoryLimitCfg{}

	if err := c.ShouldBindJSON(mlc); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if err := ca.masterService.Config().ModifyMemoryLimitCfg(c, mlc); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(mlc)
	}
}

// get long search isolation config
func (ca *clusterAPI) getSlowSearchIsolationCfg(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleGetSlowSearchIsolationCfg"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	if slow_search_isolation, err := ca.masterService.Config().GetSlowSearchIsolationCfg(c); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(slow_search_isolation)
	}
}

// modify long search isolation config
func (ca *clusterAPI) modifySlowSearchIsolationCfg(c *gin.Context) {
	startTime := time.Now()
	operateName := "handleModifySlowSearchIsolationCfg"
	httpCode := http.StatusOK
	dbName := ""
	spaceName := ""
	defer func() {
		defer monitor.Profiler(operateName, startTime, httpCode, dbName, spaceName)
	}()
	lsc := &entity.SlowSearchIsolationCfg{}

	if err := c.ShouldBindJSON(lsc); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	if err := ca.masterService.Config().ModifySlowSearchIsolationCfg(c, lsc); err != nil {
		httpCode = response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httpCode = response.New(c).JsonSuccess(lsc)
	}
}
