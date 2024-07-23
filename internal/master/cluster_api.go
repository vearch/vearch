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
	userName            = "user_name"
	roleName            = "role_name"
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

func BasicAuthMiddleware(masterService *masterService) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			err := fmt.Errorf("auth header is empty")
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Basic" {
			err := fmt.Errorf("auth header type is invalid")
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		credentials := strings.SplitN(string(decoded), ":", 2)
		if len(credentials) != 2 {
			err := fmt.Errorf("auth header credentials is invalid")
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		user, err := masterService.queryUserWithPasswordService(c, credentials[0], true)
		if err != nil {
			ferr := fmt.Errorf("auth header user %s is invalid", credentials[0])
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(ferr))
			c.Abort()
			return
		}
		if *user.Password != credentials[1] {
			err := fmt.Errorf("auth header password is invalid")
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		role, err := masterService.queryRoleService(c, user.Role.Name)
		if err != nil {
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}
		endpoint := c.FullPath()
		method := c.Request.Method
		if err := role.HasPermissionForResources(endpoint, method); err != nil {
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		c.Next()
	}
}

func ExportToClusterHandler(router *gin.Engine, masterService *masterService, server *Server) {
	dh := vearchhttp.NewBaseHandler(30)

	c := &clusterAPI{router: router, masterService: masterService, dh: dh, server: server}

	var groupAuth *gin.RouterGroup
	var group *gin.RouterGroup = router.Group("", dh.PaincHandler, dh.TimeOutHandler)
	if !config.Conf().Global.SkipAuth {
		groupAuth = router.Group("", dh.PaincHandler, dh.TimeOutHandler, BasicAuthMiddleware(masterService))
	} else {
		groupAuth = router.Group("", dh.PaincHandler, dh.TimeOutHandler)
	}

	group.GET("/", c.handleClusterInfo, dh.TimeOutEndHandler)

	// cluster handler
	groupAuth.GET("/clean_lock", c.cleanLock, dh.TimeOutEndHandler)

	// servers handler
	groupAuth.GET("/servers", c.serverList, dh.TimeOutEndHandler)

	// router  handler
	groupAuth.GET("/routers", c.routerList, dh.TimeOutEndHandler)

	// partition register, use internal so no need to auth
	group.POST("/register", c.register, dh.TimeOutEndHandler)
	group.POST("/register_partition", c.registerPartition, dh.TimeOutEndHandler)
	group.POST("/register_router", c.registerRouter, dh.TimeOutEndHandler)

	// db handler
	groupAuth.POST(fmt.Sprintf("/dbs/:%s", dbName), c.createDB, dh.TimeOutEndHandler)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s", dbName), c.getDB, dh.TimeOutEndHandler)
	groupAuth.GET("/dbs", c.getDB, dh.TimeOutEndHandler)
	groupAuth.DELETE(fmt.Sprintf("/dbs/:%s", dbName), c.deleteDB, dh.TimeOutEndHandler)
	groupAuth.PUT(fmt.Sprintf("/dbs/:%s", dbName), c.modifyDB, dh.TimeOutEndHandler)

	// space handler
	groupAuth.POST(fmt.Sprintf("/dbs/:%s/spaces", dbName), c.createSpace, dh.TimeOutEndHandler)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.getSpace, dh.TimeOutEndHandler)
	groupAuth.GET(fmt.Sprintf("/dbs/:%s/spaces", dbName), c.getSpace, dh.TimeOutEndHandler)
	groupAuth.DELETE(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.deleteSpace, dh.TimeOutEndHandler)
	// group.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.updateSpace, dh.TimeOutEndHandler)
	groupAuth.PUT(fmt.Sprintf("/dbs/:%s/spaces/:%s", dbName, spaceName), c.updateSpaceResource, dh.TimeOutEndHandler)
	groupAuth.POST(fmt.Sprintf("/backup/dbs/:%s/spaces/:%s", dbName, spaceName), c.backupSpace, dh.TimeOutEndHandler)

	// modify engine config handler
	groupAuth.POST("/config/:"+dbName+"/:"+spaceName, c.modifyEngineCfg, dh.TimeOutEndHandler)
	groupAuth.GET("/config/:"+dbName+"/:"+spaceName, c.getEngineCfg, dh.TimeOutEndHandler)

	// partition handler
	groupAuth.GET("/partitions", c.partitionList, dh.TimeOutEndHandler)
	groupAuth.POST("/partitions/change_member", c.changeMember, dh.TimeOutEndHandler)
	groupAuth.POST("/partitions/resource_limit", c.ResourceLimit, dh.TimeOutEndHandler)

	// schedule
	groupAuth.POST("/schedule/recover_server", c.RecoverFailServer, dh.TimeOutEndHandler)
	groupAuth.POST("/schedule/change_replicas", c.ChangeReplicas, dh.TimeOutEndHandler)
	groupAuth.GET("/schedule/fail_server", c.FailServerList, dh.TimeOutEndHandler)
	groupAuth.DELETE("/schedule/fail_server/:"+NodeID, c.FailServerClear, dh.TimeOutEndHandler)
	groupAuth.GET("/schedule/clean_task", c.CleanTask, dh.TimeOutEndHandler)

	// remove server metadata
	groupAuth.POST("/meta/remove_server", c.RemoveServerMeta, dh.TimeOutEndHandler)

	// alias handler
	groupAuth.POST(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", aliasName, dbName, spaceName), c.createAlias, dh.TimeOutEndHandler)
	groupAuth.GET(fmt.Sprintf("/alias/:%s", aliasName), c.getAlias, dh.TimeOutEndHandler)
	groupAuth.GET("/alias", c.getAlias, dh.TimeOutEndHandler)
	groupAuth.DELETE(fmt.Sprintf("/alias/:%s", aliasName), c.deleteAlias, dh.TimeOutEndHandler)
	groupAuth.PUT(fmt.Sprintf("/alias/:%s/dbs/:%s/spaces/:%s", aliasName, dbName, spaceName), c.modifyAlias, dh.TimeOutEndHandler)

	// user handler
	groupAuth.POST("/users", c.createUser, dh.TimeOutEndHandler)
	groupAuth.GET(fmt.Sprintf("/users/:%s", userName), c.getUser, dh.TimeOutEndHandler)
	groupAuth.GET("/users", c.getUser, dh.TimeOutEndHandler)
	groupAuth.DELETE(fmt.Sprintf("/users/:%s", userName), c.deleteUser, dh.TimeOutEndHandler)
	groupAuth.PUT("/users", c.updateUser, dh.TimeOutEndHandler)

	// role handler
	groupAuth.POST("/roles", c.createRole, dh.TimeOutEndHandler)
	groupAuth.GET(fmt.Sprintf("/roles/:%s", roleName), c.getRole, dh.TimeOutEndHandler)
	groupAuth.GET("/roles", c.getRole, dh.TimeOutEndHandler)
	groupAuth.DELETE(fmt.Sprintf("/roles/:%s", roleName), c.deleteRole, dh.TimeOutEndHandler)
	groupAuth.PUT("/roles", c.changeRolePrivilege, dh.TimeOutEndHandler)

	groupAuth.GET("/cluster/stats", c.stats, dh.TimeOutEndHandler)
	groupAuth.GET("/cluster/health", c.health, dh.TimeOutEndHandler)

}

// got every partition servers system info
func (ca *clusterAPI) stats(c *gin.Context) {
	list, err := ca.masterService.statsService(c)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}
	httphelper.New(c).JsonSuccess(list)
}

// cluster health in partition level
func (ca *clusterAPI) health(c *gin.Context) {
	dbName := c.Query("db")
	spaceName := c.Query("space")
	detail := c.Query("detail")

	result, err := ca.masterService.partitionInfo(c, dbName, spaceName, detail)
	if err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	}

	httphelper.New(c).JsonSuccess(result)
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
			spaceInfo.PartitionRule = space.PartitionRule
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
				spaceInfo.PartitionRule = space.PartitionRule
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

	space := &entity.SpacePartitionResource{
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

	if backup.Command == "create" {
		dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
		if err != nil {
			httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
			return
		}

		space, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName)
		if err != nil {
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
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
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
			Secure: backup.S3Param.UseSSL,
		})
		if err != nil {
			err := fmt.Errorf("failed to create minio client: %+v", err)
			log.Error(err)
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
			return
		}
		bucketName := backup.S3Param.BucketName
		objectName := fmt.Sprintf("%s/%s/%s.schema", dbName, space.Name, space.Name)
		_, err = minioClient.FPutObject(context.Background(), bucketName, objectName, backupFileName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			err := fmt.Errorf("failed to backup space: %+v", err)
			log.Error(err)
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
			return
		}
		log.Info("backup schema success, file is [%s]", backupFileName)
		os.Remove(backupFileName)
	} else if backup.Command == "restore" && backup.WitchShema {
		dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
		if err != nil {
			httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
			return
		}

		_, err = ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName)
		if err == nil {
			httphelper.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("space duplicate")))
			return
		}
		minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
			Secure: backup.S3Param.UseSSL,
		})
		if err != nil {
			err := fmt.Errorf("failed to create minio client: %+v", err)
			log.Error(err)
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		backupFileName := spaceName + ".schema"
		bucketName := backup.S3Param.BucketName
		objectName := fmt.Sprintf("%s/%s/%s.schema", dbName, spaceName, spaceName)
		err = minioClient.FGetObject(c, bucketName, objectName, backupFileName, minio.GetObjectOptions{})
		if err != nil {
			err := fmt.Errorf("failed to download file from S3: %+v", err)
			log.Error(err)
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
			return
		}
		defer os.Remove(backupFileName)
		log.Info("downloaded backup file from S3: %s", backupFileName)

		spaceJson, err := os.ReadFile(backupFileName)
		if err != nil {
			err := fmt.Errorf("error read file:%v", err)
			log.Error(err)
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		log.Debug("%s", spaceJson)
		space := &entity.Space{}
		err = vjson.Unmarshal(spaceJson, space)
		if err != nil {
			err := fmt.Errorf("unmarshal file: %v", err)
			log.Error(err)
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
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
			httphelper.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("oss partition num %d not equal schema %d", len(patitionMap), partitionNum)))
			return
		}
		if err := ca.masterService.createSpaceService(c, dbName, space); err != nil {
			log.Error("createSpaceService err: %v", err)
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
		}
	} else if backup.Command == "restore" && !backup.WitchShema {
		dbID, err := ca.masterService.Master().QueryDBName2Id(c, dbName)
		if err != nil {
			httphelper.New(c).JsonError(errors.NewErrUnprocessable(err))
			return
		}
		space, err := ca.masterService.Master().QuerySpaceByName(c, dbID, spaceName)
		if err != nil {
			httphelper.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("space duplicate")))
			return
		}
		minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
			Secure: backup.S3Param.UseSSL,
		})
		if err != nil {
			err := fmt.Errorf("failed to create minio client: %+v", err)
			log.Error(err)
			httphelper.New(c).JsonError(errors.NewErrInternal(err))
			return
		}

		partitionNum := len(space.Partitions)
		space.Partitions = make([]*entity.Partition, 0)

		bucketName := backup.S3Param.BucketName
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
			httphelper.New(c).JsonError(errors.NewErrInternal(fmt.Errorf("oss partition num %d not equal schema %d", len(patitionMap), partitionNum)))
			return
		}
	}

	if err := ca.masterService.BackupSpace(c, dbName, spaceName, backup); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(backup)
	}
}

func (ca *clusterAPI) ResourceLimit(c *gin.Context) {
	resourceLimit := &entity.ResourceLimit{}
	if err := c.ShouldBindJSON(resourceLimit); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	if err := ca.masterService.ResourceLimitService(c, resourceLimit); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
		return
	} else {
		httphelper.New(c).JsonSuccess(resourceLimit)
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

func (ca *clusterAPI) createUser(c *gin.Context) {
	user := &entity.User{}
	if err := c.ShouldBindJSON(user); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("create user: %s", user.Name)

	if err := ca.masterService.createUserService(c, user, true); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		// just return user name
		user_return := &entity.User{Name: user.Name}
		httphelper.New(c).JsonSuccess(user_return)
	}
}

func (ca *clusterAPI) deleteUser(c *gin.Context) {
	log.Debug("delete user: %s", c.Param(userName))
	name := c.Param(userName)

	if err := ca.masterService.deleteUserService(c, name); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getUser(c *gin.Context) {
	name := c.Param(userName)
	if name == "" {
		if users, err := ca.masterService.queryAllUser(c); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httphelper.New(c).JsonSuccess(users)
		}
	} else {
		if user, err := ca.masterService.queryUserService(c, name, true); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httphelper.New(c).JsonSuccess(user)
		}
	}
}

func (ca *clusterAPI) updateUser(c *gin.Context) {
	user := &entity.User{}
	if err := c.ShouldBindJSON(user); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	auth_user := ""
	if !config.Conf().Global.SkipAuth {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			err := fmt.Errorf("auth header is empty")
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Basic" {
			err := fmt.Errorf("auth header type is invalid")
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}

		credentials := strings.SplitN(string(decoded), ":", 2)
		if len(credentials) != 2 {
			err := fmt.Errorf("auth header credentials is invalid")
			httphelper.New(c).JsonError(errors.NewErrUnauthorized(err))
			c.Abort()
			return
		}
		auth_user = credentials[0]
	}

	if err := ca.masterService.updateUserService(c, user, auth_user); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(user)
	}
}

func (ca *clusterAPI) createRole(c *gin.Context) {
	role := &entity.Role{}
	if err := c.ShouldBindJSON(role); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}

	log.Debug("create role: %s", role.Name)

	if err := ca.masterService.createRoleService(c, role); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(role)
	}
}

func (ca *clusterAPI) deleteRole(c *gin.Context) {
	log.Debug("delete role: %s", c.Param(roleName))
	name := c.Param(roleName)

	if err := ca.masterService.deleteRoleService(c, name); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).SuccessDelete()
	}
}

func (ca *clusterAPI) getRole(c *gin.Context) {
	name := c.Param(roleName)
	if name == "" {
		if roles, err := ca.masterService.queryAllRole(c); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httphelper.New(c).JsonSuccess(roles)
		}
	} else {
		if role, err := ca.masterService.queryRoleService(c, name); err != nil {
			httphelper.New(c).JsonError(errors.NewErrNotFound(err))
		} else {
			httphelper.New(c).JsonSuccess(role)
		}
	}
}

func (ca *clusterAPI) changeRolePrivilege(c *gin.Context) {
	role := &entity.Role{}
	if err := c.ShouldBindJSON(role); err != nil {
		body, _ := netutil.GetReqBody(c.Request)
		log.Error("create space request: %s, err: %s", body, err.Error())
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
		return
	}
	log.Debug("update role: %s privilege", role.Name)

	if new_role, err := ca.masterService.changeRolePrivilegeService(c, role); err != nil {
		httphelper.New(c).JsonError(errors.NewErrInternal(err))
	} else {
		httphelper.New(c).JsonSuccess(new_role)
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
	cacheCfg := &entity.EngineConfig{}

	if err := c.ShouldBindJSON(cacheCfg); err != nil {
		httphelper.New(c).JsonError(errors.NewErrBadRequest(err))
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
