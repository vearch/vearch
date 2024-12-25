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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/gin-gonic/gin"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
)

const (
	DB                  = "db"
	dbName              = "db_name"
	spaceName           = "space_name"
	aliasName           = "alias_name"
	userName            = "user_name"
	roleName            = "role_name"
	memberId            = "member_id"
	peerAddrs           = "peer_addrs"
	headerAuthKey       = "Authorization"
	NodeID              = "node_id"
	DefaultResourceName = "default"
)

type clusterAPI struct {
	router        *gin.Engine
	masterService *masterService
	server        *Server
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

		user, err := masterService.queryUserWithPasswordService(c, credentials[0], true)
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

		role, err := masterService.queryRoleService(c, user.Role.Name)
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
				msg := fmt.Sprintf("timeout[%s] param parse to int failed, err: %s", timeout, err.Error())
				log.Error(msg)
				response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf(msg)))
				c.Abort()
				return
			}
		} else {
			timeout = defaultTimeout
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		c.Request = c.Request.WithContext(ctx)
		done := make(chan struct{})

		go func() {
			c.Next()
			close(done)
		}()

		select {
		case <-ctx.Done():
			response.New(c).JsonError(errors.NewErrUnprocessable(fmt.Errorf("timeout")))
			c.Abort()
		case <-done:
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
	groupAuth.POST(fmt.Sprintf("/dbs/:%s", dbName), c.createDB)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s", dbName), c.getDB)
	groupAuth.GET("/dbs", c.getDB)
	groupAuth.DELETE(fmt.Sprintf("/dbs/:%s", dbName), c.deleteDB)
	groupAuth.PUT(fmt.Sprintf("/dbs/:%s", dbName), c.modifyDB)

	// space handler
	groupAuth.POST(fmt.Sprintf("/dbs/:%s/spaces", dbName), c.createSpace)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.getSpace)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s/spaces", dbName), c.getSpace)
	groupAuth.DELETE(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.deleteSpace)
	// group.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.updateSpace)
	groupAuth.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.updateSpaceResource)
	groupAuth.POST(fmt.Sprintf("/backup/dbs/:%s/spaces/:%s", dbName, spaceName), c.backupSpace)

	// modify engine config handler
	groupAuth.POST("/config/:"+dbName+"/:"+spaceName, c.modifyEngineCfg)
	groupAuth.GET("/config/:"+dbName+"/:"+spaceName, c.getEngineCfg)

	// partition handler
	groupAuth.GET("/partitions", c.partitionList)
	groupAuth.POST("/partitions/change_member", c.changeMember)
	groupAuth.POST("/partitions/resource_limit", c.ResourceLimit)

	// schedule
	groupAuth.POST("/schedule/recover_server", c.RecoverFailServer)
	groupAuth.POST("/schedule/change_replicas", c.ChangeReplicas)
	groupAuth.GET("/schedule/fail_server", c.FailServerList)
	groupAuth.DELETE("/schedule/fail_server/:"+NodeID, c.FailServerClear)
	groupAuth.GET("/schedule/clean_task", c.CleanTask)

	// remove server metadata
	groupAuth.POST("/meta/remove_server", c.RemoveServerMeta)

	// alias handler
	groupAuth.POST(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", aliasName, dbName, spaceName), c.createAlias)
	groupAuth.GET(fmt.Sprintf("/alias/:%s", aliasName), c.getAlias)
	groupAuth.GET("/alias", c.getAlias)
	groupAuth.DELETE(fmt.Sprintf("/alias/:%s", aliasName), c.deleteAlias)
	groupAuth.PUT(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", aliasName, dbName, spaceName), c.modifyAlias)

	// user handler
	groupAuth.POST("/users", c.createUser)
	groupAuth.GET(fmt.Sprintf("/users/:%s", userName), c.getUser)
	groupAuth.GET("/users", c.getUser)
	groupAuth.DELETE(fmt.Sprintf("/users/:%s", userName), c.deleteUser)
	groupAuth.PUT("/users", c.updateUser)

	// role handler
	groupAuth.POST("/roles", c.createRole)
	groupAuth.GET(fmt.Sprintf("/roles/:%s", roleName), c.getRole)
	groupAuth.GET("/roles", c.getRole)
	groupAuth.DELETE(fmt.Sprintf("/roles/:%s", roleName), c.deleteRole)
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
	list, err := ca.masterService.statsService(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	response.New(c).JsonSuccess(list)
}

// cluster health in partition level
func (ca *clusterAPI) health(c *gin.Context) {
	dbName := c.Query("db")
	spaceName := c.Query("space")
	detail := c.Query("detail")

	result, err := ca.masterService.partitionInfo(c, dbName, spaceName, detail)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	response.New(c).JsonSuccess(result)
}

func (ca *clusterAPI) handleClusterInfo(c *gin.Context) {
	layer := map[string]interface{}{
		"name": config.Conf().Global.Name,
		"version": map[string]interface{}{
			"build_version": config.GetBuildVersion(),
			"build_time":    config.GetBuildTime(),
			"commit_id":     config.GetCommitID(),
		},
	}

	response.New(c).JsonSuccess(layer)
}

// cleanLock lock for admin, when space locked, waring make sure not create space ing
func (ca *clusterAPI) cleanLock(c *gin.Context) {
	removed := make([]string, 0, 1)

	if keys, _, err := ca.masterService.Master().Store.PrefixScan(c, entity.PrefixLock); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
	} else {
		for _, key := range keys {
			if err := ca.masterService.Master().Store.Delete(c, string(key)); err != nil {
				response.New(c).JsonError(errors.NewErrInternal(err))
				return
			}
			removed = append(removed, string(key))
		}
		response.New(c).JsonSuccess(removed)
	}
}

// for ps startup to register self and get ip response
func (ca *clusterAPI) register(c *gin.Context) {
	ip := c.ClientIP()
	log.Debug("register from: %v", ip)
	clusterName := c.Query("clusterName")
	nodeID := entity.NodeID(cast.ToInt64(c.Query("nodeID")))

	if clusterName == "" || nodeID == 0 {
		response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has clusterName AND nodeID")))
		return
	}

	if clusterName != config.Conf().Global.Name {
		response.New(c).JsonError(errors.NewErrUnprocessable(fmt.Errorf("cluster name different, please check")))
		return
	}

	// if node id is already existed, return failed
	if err := ca.masterService.IsExistNode(c, nodeID, ip); err != nil {
		log.Debug("nodeID[%d] exist %v", nodeID, err)
		response.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}

	server, err := ca.masterService.registerServerService(c, ip, nodeID)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	response.New(c).JsonSuccess(server)
}

// for router startup to register self and get ip response
func (ca *clusterAPI) registerRouter(c *gin.Context) {
	ip := c.ClientIP()
	log.Debug("register from: %s", ip)
	clusterName := c.Query("clusterName")

	if clusterName != config.Conf().Global.Name {
		response.New(c).JsonError(errors.NewErrUnprocessable(fmt.Errorf("cluster name different, please check")))
		return
	}

	response.New(c).JsonSuccess(ip)
}

// when partition leader got it will register self to this api
func (ca *clusterAPI) registerPartition(c *gin.Context) {
	partition := &entity.Partition{}

	if err := c.ShouldBindJSON(partition); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	partition.UpdateTime = time.Now().UnixNano()

	if err := ca.masterService.registerPartitionService(c, partition); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(nil)
	}
}

func (ca *clusterAPI) createDB(c *gin.Context) {
	startTime := time.Now()
	defer monitor.Profiler("createDB", startTime)
	dbName := c.Param(dbName)
	db := &entity.DB{Name: dbName}

	log.Debug("create db: %s", db.Name)

	if err := ca.masterService.createDBService(c, db); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(db)
	}
}

func (ca *clusterAPI) deleteDB(c *gin.Context) {
	log.Debug("delete db, db: %s", c.Param(dbName))
	db := c.Param(dbName)

	if err := ca.masterService.deleteDBService(c, db); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getDB(c *gin.Context) {
	db := c.Param(dbName)
	if db == "" {
		if dbs, err := ca.masterService.queryDBs(c); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			response.New(c).JsonSuccess(dbs)
		}
	} else {
		if db, err := ca.masterService.queryDBService(c, db); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			response.New(c).JsonSuccess(db)
		}
	}
}

func (ca *clusterAPI) modifyDB(c *gin.Context) {
	dbModify := &entity.DBModify{}
	if err := c.ShouldBindJSON(dbModify); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if db, err := ca.masterService.updateDBIpList(c.Request.Context(), dbModify); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(db)
	}
}

func (ca *clusterAPI) createSpace(c *gin.Context) {
	log.Debug("create space, db: %s", c.Param(dbName))

	dbName := c.Param(dbName)

	space := &entity.Space{}
	if err := c.ShouldBindJSON(space); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		response.New(c).JsonError(errors.NewErrBadRequest(err))
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
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if space.ReplicaNum <= 0 {
		space.ReplicaNum = 1
	}

	// check index name is ok
	if err := space.Validate(); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	space.Version = 1 // first start with 1

	if err := ca.masterService.createSpaceService(c, dbName, space); err != nil {
		log.Error("createSpaceService err: %v", err)
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	cfg, err := ca.masterService.GetEngineCfg(c, dbName, space.Name)
	if err != nil {
		log.Error("get engine config err: %s", err.Error())
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	err = ca.masterService.updateEngineConfig(c, space, cfg)
	if err != nil {
		log.Error("update engine config err: %s", err.Error())
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	response.New(c).JsonSuccess(space)
}

func (ca *clusterAPI) deleteSpace(c *gin.Context) {
	log.Debug("delete space, db: %s, space: %s", c.Param(dbName), c.Param(spaceName))
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)

	if err := ca.masterService.deleteSpaceService(c, dbName, spaceName); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).SuccessDelete()
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
		response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	if spaceName != "" {
		if space, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
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
			if _, err := ca.masterService.describeSpaceService(c, space, spaceInfo, detail_info); err != nil {
				response.New(c).JsonError(errors.NewErrInternal(err))
			} else {
				response.New(c).JsonSuccess(spaceInfo)
			}
		}
	} else {
		if spaces, err := ca.masterService.Master().QuerySpaces(c, dbID); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
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
				if _, err := ca.masterService.describeSpaceService(c, space, spaceInfo, detail_info); err != nil {
					response.New(c).JsonError(errors.NewErrInternal(err))
					return
				} else {
					spaceInfos = append(spaceInfos, spaceInfo)
				}
			}
			response.New(c).JsonSuccess(spaceInfos)
		}
	}
}

func (ca *clusterAPI) updateSpace(c *gin.Context) {
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)

	space := &entity.Space{Name: spaceName}

	if err := c.ShouldBindJSON(space); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if spaceResult, err := ca.masterService.updateSpaceService(c, dbName, spaceName, space); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(spaceResult)
	}
}

func (ca *clusterAPI) updateSpaceResource(c *gin.Context) {
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)

	space := &entity.SpacePartitionResource{
		SpaceName: spaceName,
		DbName:    dbName,
	}

	if err := c.ShouldBindJSON(space); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("updateSpaceResource %v", space)

	if spaceResult, err := ca.masterService.updateSpaceResourceService(c, space); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(spaceResult)
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
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if backup.Command == "create" {
		dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
		if err != nil {
			response.New(c).JsonError(errors.NewErrUnprocessable(err))
			return
		}

		space, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName)
		if err != nil {
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		spaceJson, err := vjson.Marshal(space)
		if err != nil {
			log.Error("vjson.Marshal err: %v", err)
		}

		backupFileName := space.Name + ".schema"

		err = os.WriteFile(backupFileName, spaceJson, 0644)
		if err != nil {
			err := fmt.Errorf("error writing to file: %v", err)
			log.Error(err)
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
			Secure: backup.S3Param.UseSSL,
		})
		if err != nil {
			err := fmt.Errorf("failed to create minio client: %+v", err)
			log.Error(err)
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}
		bucketName := backup.S3Param.BucketName
		objectName := fmt.Sprintf("%s/%s/%s.schema", dbName, space.Name, space.Name)
		_, err = minioClient.FPutObject(context.Background(), bucketName, objectName, backupFileName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			err := fmt.Errorf("failed to backup space: %+v", err)
			log.Error(err)
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}
		log.Info("backup schema success, file is [%s]", backupFileName)
		os.Remove(backupFileName)
	} else if backup.Command == "restore" {
		dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
		if err != nil {
			response.New(c).JsonError(errors.NewErrUnprocessable(err))
			return
		}

		_, err = ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName)
		if err == nil {
			response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("space duplicate")))
			return
		}
		minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
			Secure: backup.S3Param.UseSSL,
		})
		if err != nil {
			err := fmt.Errorf("failed to create minio client: %+v", err)
			log.Error(err)
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		backupFileName := spaceName + ".schema"
		bucketName := backup.S3Param.BucketName
		objectName := fmt.Sprintf("%s/%s/%s.schema", dbName, spaceName, spaceName)
		err = minioClient.FGetObject(c, bucketName, objectName, backupFileName, minio.GetObjectOptions{})
		if err != nil {
			err := fmt.Errorf("failed to download file from S3: %+v", err)
			log.Error(err)
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}
		defer os.Remove(backupFileName)
		log.Info("downloaded backup file from S3: %s", backupFileName)

		spaceJson, err := os.ReadFile(backupFileName)
		if err != nil {
			err := fmt.Errorf("error read file:%v", err)
			log.Error(err)
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		log.Debug("%s", spaceJson)
		space := &entity.Space{}
		err = vjson.Unmarshal(spaceJson, space)
		if err != nil {
			err := fmt.Errorf("unmarshal file: %v", err)
			log.Error(err)
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		partitionNum := len(space.Partitions)
		space.Partitions = make([]*entity.Partition, 0)

		objectCh := minioClient.ListObjects(c, bucketName, minio.ListObjectsOptions{
			Recursive: true,
		})

		patitionMap := make(map[string]string, 0)
		for object := range objectCh {
			if object.Err != nil {
				fmt.Println(object.Err)
				continue
			}
			if strings.HasSuffix(object.Key, ".json.zst") {
				patitionMap[object.Key] = object.Key
			}
		}

		if len(patitionMap) != partitionNum {
			response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("oss partition num %d not equal schema %d", len(patitionMap), partitionNum)))
			return
		}
		if err := ca.masterService.createSpaceService(c, dbName, space); err != nil {
			log.Error("createSpaceService err: %v", err)
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		cfg, err := ca.masterService.GetEngineCfg(c, dbName, spaceName)
		if err != nil {
			log.Error("get engine config err: %s", err.Error())
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		err = ca.masterService.updateEngineConfig(c, space, cfg)
		if err != nil {
			log.Error("update engine config err: %s", err.Error())
			response.New(c).JsonError(errors.NewErrInternal(err))
			return
		}
	}

	if err := ca.masterService.BackupSpace(c, dbName, spaceName, backup); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(backup)
	}
}

func (ca *clusterAPI) ResourceLimit(c *gin.Context) {
	resourceLimit := &entity.ResourceLimit{}
	if err := c.ShouldBindJSON(resourceLimit); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if err := ca.masterService.ResourceLimitService(c, resourceLimit); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		response.New(c).JsonSuccess(resourceLimit)
	}
}

func (ca *clusterAPI) createAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
	if err != nil {
		response.New(c).JsonError(errors.NewErrUnprocessable(err))
		return
	}

	if _, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	alias := &entity.Alias{Name: aliasName, DbName: dbName, SpaceName: spaceName}

	log.Debug("create alias: %s, dbName: %s, spaceName: %s", aliasName, dbName, spaceName)

	if err := ca.masterService.createAliasService(c, alias); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(alias)
	}
}

func (ca *clusterAPI) deleteAlias(c *gin.Context) {
	log.Debug("delete alias: %s", c.Param(aliasName))
	aliasName := c.Param(aliasName)

	if err := ca.masterService.deleteAliasService(c, aliasName); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	if aliasName == "" {
		if alias, err := ca.masterService.queryAllAlias(c); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			response.New(c).JsonSuccess(alias)
		}
	} else {
		if alias, err := ca.masterService.queryAliasService(c, aliasName); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			response.New(c).JsonSuccess(alias)
		}
	}
}

func (ca *clusterAPI) modifyAlias(c *gin.Context) {
	aliasName := c.Param(aliasName)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
	if err != nil {
		response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	if _, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName); err != nil {
		response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}

	alias := &entity.Alias{
		Name:      aliasName,
		DbName:    dbName,
		SpaceName: spaceName,
	}
	if err := ca.masterService.updateAliasService(c, alias); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(alias)
	}
}

func (ca *clusterAPI) createUser(c *gin.Context) {
	user := &entity.User{}
	if err := c.ShouldBindJSON(user); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("create user: %s", user.Name)

	if err := ca.masterService.createUserService(c, user, true); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		// just return user name
		user_return := &entity.User{Name: user.Name}
		response.New(c).JsonSuccess(user_return)
	}
}

func (ca *clusterAPI) deleteUser(c *gin.Context) {
	log.Debug("delete user: %s", c.Param(userName))
	name := c.Param(userName)

	if err := ca.masterService.deleteUserService(c, name); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getUser(c *gin.Context) {
	name := c.Param(userName)
	if name == "" {
		if users, err := ca.masterService.queryAllUser(c); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			response.New(c).JsonSuccess(users)
		}
	} else {
		if user, err := ca.masterService.queryUserService(c, name, true); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			response.New(c).JsonSuccess(user)
		}
	}
}

func (ca *clusterAPI) updateUser(c *gin.Context) {
	user := &entity.User{}
	if err := c.ShouldBindJSON(user); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	auth_user := ""
	if !config.Conf().Global.SkipAuth {
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
		auth_user = credentials[0]
	}

	if err := ca.masterService.updateUserService(c, user, auth_user); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(user)
	}
}

func (ca *clusterAPI) createRole(c *gin.Context) {
	role := &entity.Role{}
	if err := c.ShouldBindJSON(role); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("create role: %s", role.Name)

	if err := ca.masterService.createRoleService(c, role); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(role)
	}
}

func (ca *clusterAPI) deleteRole(c *gin.Context) {
	log.Debug("delete role: %s", c.Param(roleName))
	name := c.Param(roleName)

	if err := ca.masterService.deleteRoleService(c, name); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getRole(c *gin.Context) {
	name := c.Param(roleName)
	if name == "" {
		if roles, err := ca.masterService.queryAllRole(c); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			response.New(c).JsonSuccess(roles)
		}
	} else {
		if role, err := ca.masterService.queryRoleService(c, name); err != nil {
			response.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			response.New(c).JsonSuccess(role)
		}
	}
}

func (ca *clusterAPI) changeRolePrivilege(c *gin.Context) {
	role := &entity.Role{}
	if err := c.ShouldBindJSON(role); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	log.Debug("update role: %s privilege", role.Name)

	if new_role, err := ca.masterService.changeRolePrivilegeService(c, role); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(new_role)
	}
}

func (ca *clusterAPI) getMembers(c *gin.Context) {
	resp, err := ca.masterService.Client.Master().MemberList(context.Background())
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	response.New(c).JsonSuccess(resp)
}

func (ca *clusterAPI) getMemberStatus(c *gin.Context) {
	resp, err := ca.masterService.Client.Master().MemberStatus(context.Background())
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	response.New(c).JsonSuccess(resp)
}

func (ca *clusterAPI) addMember(c *gin.Context) {
	addMemberRequest := &entity.AddMemberRequest{}

	if err := c.ShouldBindJSON(addMemberRequest); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	resp, err := ca.masterService.addMemberService(c, addMemberRequest.PeerAddrs)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	response.New(c).JsonSuccess(resp)
}

func (ca *clusterAPI) deleteMember(c *gin.Context) {
	master := &entity.MemberInfoRequest{}
	if err := c.ShouldBindJSON(master); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	resp, err := ca.masterService.removeMemberService(c, master)
	if err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	response.New(c).JsonSuccess(resp)
}

// get engine config
func (ca *clusterAPI) getEngineCfg(c *gin.Context) {
	var err error
	defer errutil.CatchError(&err)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	errutil.ThrowError(err)
	if cfg, err := ca.masterService.GetEngineCfg(c, dbName, spaceName); err != nil {
		response.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		response.New(c).JsonSuccess(cfg)
	}
}

// modify engine config
func (ca *clusterAPI) modifyEngineCfg(c *gin.Context) {
	var err error
	defer errutil.CatchError(&err)
	dbName := c.Param(dbName)
	spaceName := c.Param(spaceName)
	cacheCfg := &entity.EngineConfig{}

	if err := c.ShouldBindJSON(cacheCfg); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	if err := ca.masterService.ModifyEngineCfg(c, dbName, spaceName, cacheCfg); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(cacheCfg)
	}
}

// serverList list servers
func (ca *clusterAPI) serverList(c *gin.Context) {
	servers, err := ca.masterService.Master().QueryServers(c)

	if err != nil {
		response.New(c).JsonError(errors.NewErrNotFound(err))
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

	response.New(c).JsonSuccess(map[string]interface{}{"servers": serverInfos, "count": len(servers)})
}

// routerList list router
func (ca *clusterAPI) routerList(c *gin.Context) {
	if routerIPs, err := ca.masterService.Master().QueryRouter(c, config.Conf().Global.Name); err != nil {
		response.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		response.New(c).JsonSuccess(routerIPs)
	}
}

// partitionList list partition
func (ca *clusterAPI) partitionList(c *gin.Context) {
	partitions, err := ca.masterService.Master().QueryPartitions(c)
	if err != nil {
		response.New(c).JsonError(errors.NewErrNotFound(err))
	} else {
		response.New(c).JsonSuccess(partitions)
	}
}

// list fail servers
func (cluster *clusterAPI) FailServerList(c *gin.Context) {
	failServers, err := cluster.masterService.Master().QueryAllFailServer(c.Request.Context())

	if err != nil {
		response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	response.New(c).JsonSuccess(map[string]interface{}{"fail_servers": failServers, "count": len(failServers)})
}

// clear fail server by nodeID
func (cluster *clusterAPI) FailServerClear(c *gin.Context) {
	nodeID := c.Param(NodeID)
	if nodeID == "" {
		response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has nodeId")))
		return
	}
	id, err := strconv.ParseUint(nodeID, 10, 64)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("nodeId err")))
		return
	}
	err = cluster.masterService.Master().DeleteFailServerByNodeID(c.Request.Context(), id)
	if err != nil {
		response.New(c).JsonError(errors.NewErrNotFound(err))
		return
	}
	response.New(c).JsonSuccess(map[string]interface{}{"nodeID": nodeID})
}

// clear task
func (cluster *clusterAPI) CleanTask(c *gin.Context) {
	CleanTask(cluster.server)
	response.New(c).JsonSuccess("clean task success!")
}

// remove etcd meta about the nodeID
func (cluster *clusterAPI) RemoveServerMeta(c *gin.Context) {
	rfs := &entity.RecoverFailServer{}
	if err := c.ShouldBindJSON(rfs); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	// get nodeID
	nodeID := rfs.FailNodeID
	// ipAddr
	ipAdd := rfs.FailNodeAddr
	if nodeID == 0 && ipAdd == "" {
		response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("param err must has fail_node_id or fail_node_addr")))
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
			err := cluster.masterService.ChangeMember(c.Request.Context(), cm)
			if err != nil {
				log.Error("ChangePartitionMember [%+v] err is %s", cm, err.Error())
				response.New(c).JsonError(errors.NewErrInternal(err))
				return
			}
		}
	} else {
		response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("can't find server [%v]", failServer)))
		return
	}
	response.New(c).JsonSuccess(fmt.Sprintf("nodeid [%d], server [%v] remove node success!", nodeID, failServer))
}

// recover the failserver by a newserver
func (cluster *clusterAPI) RecoverFailServer(c *gin.Context) {
	rs := &entity.RecoverFailServer{}
	if err := c.ShouldBindJSON(rs); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	rsStr := vjson.ToJsonString(rs)
	log.Info("RecoverFailServer is %s,", rsStr)
	if err := cluster.masterService.RecoverFailServer(c.Request.Context(), rs); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("%s failed recover, err is %v", rsStr, err)))
	} else {
		response.New(c).JsonSuccess(fmt.Sprintf("%s success recover!", rsStr))
	}
}

// change replicas by dbname and spaceName
func (cluster *clusterAPI) ChangeReplicas(c *gin.Context) {
	dbModify := &entity.DBModify{}
	if err := c.ShouldBindJSON(dbModify); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	dbByte, err := vjson.Marshal(dbModify)
	if err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	dbStr := string(dbByte)
	log.Info("dbModify is %s", dbStr)
	if dbModify.DbName == "" || dbModify.SpaceName == "" {
		response.New(c).JsonError(errors.NewErrBadRequest(fmt.Errorf("dbModify info incorrect [%s]", dbStr)))
	}
	if err := cluster.masterService.ChangeReplica(c.Request.Context(), dbModify); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("[%s] failed ChangeReplicas,err is %v", dbStr, err)))
	} else {
		response.New(c).JsonSuccess(fmt.Sprintf("[%s] success ChangeReplicas!", dbStr))
	}
}

func (ca *clusterAPI) changeMember(c *gin.Context) {
	cm := &entity.ChangeMembers{}

	if err := c.ShouldBindJSON(cm); err != nil {
		response.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if err := ca.masterService.ChangeMembers(c, cm); err != nil {
		response.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		response.New(c).JsonSuccess(nil)
	}
}
