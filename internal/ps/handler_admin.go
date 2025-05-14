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

package ps

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"

	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/metrics/mserver"
	vearch_os "github.com/vearch/vearch/v3/internal/pkg/runtime/os"
	"github.com/vearch/vearch/v3/internal/pkg/server/rpc/handler"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/router/document"
)

func ExportToRpcAdminHandler(server *Server) {
	initAdminHandler := &InitAdminHandler{server: server}

	psErrorChange := psErrorChange(server)

	if err := server.rpcServer.RegisterName(handler.NewChain(client.CreatePartitionHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &CreatePartitionHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.DeletePartitionHandler, handler.DefaultPanicHandler, psErrorChange, initAdminHandler, &DeletePartitionHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.DeleteReplicaHandler, handler.DefaultPanicHandler, psErrorChange, initAdminHandler, &DeleteReplicaHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.UpdatePartitionHandler, handler.DefaultPanicHandler, psErrorChange, initAdminHandler, &UpdatePartitionHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.IsLiveHandler, handler.DefaultPanicHandler, nil, initAdminHandler, new(IsLiveHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.PartitionInfoHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &PartitionInfoHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.StatsHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &StatsHandler{server: server}), ""); err != nil {
		panic(err)
	}
	if err := server.rpcServer.RegisterName(handler.NewChain(client.ChangeMemberHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &ChangeMemberHandler{server: server}), ""); err != nil {
		panic(err)
	}
	if err := server.rpcServer.RegisterName(handler.NewChain(client.EngineCfgHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &EngineCfgHandler{server: server}), ""); err != nil {
		panic(err)
	}
	if err := server.rpcServer.RegisterName(handler.NewChain(client.BackupHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &BackupHandler{server: server}), ""); err != nil {
		panic(err)
	}
	if err := server.rpcServer.RegisterName(handler.NewChain(client.ResourceLimitHandler, handler.DefaultPanicHandler, nil, initAdminHandler, &ResourceLimitHandler{server: server}), ""); err != nil {
		panic(err)
	}
}

type InitAdminHandler struct {
	server *Server
}

func (i *InitAdminHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	if i.server.stopping {
		return vearchpb.NewError(vearchpb.ErrorEnum_SERVICE_UNAVAILABLE, nil)
	}
	return nil
}

type CreatePartitionHandler struct {
	server *Server
}

func (c *CreatePartitionHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	space := new(entity.Space)
	err := json.Unmarshal(req.Data, space)
	if err != nil {
		log.Error("Create partition failed, err: [%s]", err.Error())
		return vearchpb.NewError(vearchpb.ErrorEnum_RPC_PARAM_ERROR, err)
	}
	c.server.partitions.Range(func(key, value any) bool {
		log.Debug("key %v, value %v", key, value)
		return true
	})

	if partitionStore := c.server.GetPartition(req.PartitionID); partitionStore != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_EXIST, nil)
	}

	if err := c.server.CreatePartition(ctx, space, req.PartitionID); err != nil {
		c.server.DeletePartition(req.PartitionID)
		log.Error(err)
		return err
	}
	return nil
}

type DeletePartitionHandler struct {
	server *Server
}

func (d *DeletePartitionHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	d.server.DeletePartition(req.PartitionID)
	return nil
}

type DeleteReplicaHandler struct {
	server *Server
}

func (d *DeleteReplicaHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	d.server.DeleteReplica(req.PartitionID)
	return nil
}

type UpdatePartitionHandler struct {
	server *Server
}

func (handler *UpdatePartitionHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}

	space := new(entity.Space)
	if err := json.Unmarshal(req.Data, space); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_RPC_PARAM_ERROR, err)
	}

	store := handler.server.GetPartition(req.PartitionID)
	if store == nil {
		msg := fmt.Sprintf("partition not found, partitionId:[%d], nodeID:[%d], node ip:[%s]",
			req.PartitionID, handler.server.nodeID, handler.server.ip)
		log.Error("%s", msg)
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, errors.New(msg))
	}

	err := store.UpdateSpace(ctx, space)
	if err != nil {
		return err
	}

	return nil
}

type IsLiveHandler int

func (*IsLiveHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	return nil
}

type PartitionInfoHandler struct {
	server *Server
}

func (pih *PartitionInfoHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	pid := req.PartitionID

	stores := make([]PartitionStore, 0, 1)

	if pid != 0 {
		store := pih.server.GetPartition(pid)
		stores = append(stores, store)
	} else {
		pih.server.RangePartition(func(id entity.PartitionID, store PartitionStore) {
			stores = append(stores, store)
		})
	}

	pis := make([]*entity.PartitionInfo, 0, 1)
	for _, store := range stores {
		status := &entity.EngineStatus{}
		err := store.GetEngine().GetEngineStatus(status)
		if err != nil {
			return err
		}

		value := &entity.PartitionInfo{}
		value.PartitionID = store.GetPartition().Id
		value.DocNum = uint64(status.DocNum)
		value.Unreachable = store.GetUnreachable(uint64(store.GetPartition().Id))
		value.Status = store.GetPartition().GetStatus()
		value.IndexStatus = int(status.IndexStatus)
		value.BackupStatus = int(status.BackupStatus)
		value.IndexNum = int(status.MinIndexedNum)
		value.MaxDocid = int(status.MaxDocid)
		if req.Type == vearchpb.OpType_GET {
			value.Path = store.GetPartition().Path
			value.RaftStatus = store.Status()

			// size, err := store.GetEngine().Reader().Capacity(ctx)
			// if err != nil {
			// 	return err
			// }
			// value.Size = size
		}

		pis = append(pis, value)
	}
	if reply.Data, err = json.Marshal(pis); err != nil {
		log.Error("Marshal partition info failed, err: [%v]", err)
		return err
	}
	return nil
}

type StatsHandler struct {
	server *Server
}

func (sh *StatsHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	stats := mserver.NewServerStats()
	stats.ActiveConn = len(sh.server.rpcServer.ActiveClientConn())
	stats.PartitionInfos = make([]*entity.PartitionInfo, 0, 1)
	sh.server.RangePartition(func(pid entity.PartitionID, store PartitionStore) {
		defer func() {
			if e := recover(); e != nil {
				log.Error("go partiton has err:[%v]", e)
			}
		}()

		pi := &entity.PartitionInfo{PartitionID: pid}
		stats.PartitionInfos = append(stats.PartitionInfos, pi)

		docNum, err := store.GetEngine().Reader().DocCount(ctx)
		if err != nil {
			err = fmt.Errorf("got docCount from engine err:[%s]", err.Error())
			pi.Error = err.Error()
			return
		}

		// size, err := store.GetEngine().Reader().Capacity(ctx)
		// if err != nil {
		// 	err = fmt.Errorf("got capacity from engine err:[%s]", err.Error())
		// 	pi.Error = err.Error()
		// 	return
		// }

		pi.DocNum = docNum
		pi.Size = 0
		pi.Path = store.GetPartition().Path
		pi.Unreachable = store.GetUnreachable(uint64(pid))
		pi.Status = store.GetPartition().GetStatus()
		index_status, index_num, max_docid := store.GetEngine().IndexInfo()
		pi.IndexStatus = index_status
		pi.IndexNum = index_num
		pi.MaxDocid = max_docid
		pi.RaftStatus = store.Status()
	})

	if values, err := json.Marshal(stats); err != nil {
		log.Error("Marshal partition info failed, err: [%v]", err)
		return err
	} else {
		reply.Data = values
	}
	return nil
}

type ChangeMemberHandler struct {
	server *Server
}

func (ch *ChangeMemberHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}

	reqObj := new(entity.ChangeMember)
	if err := json.Unmarshal(req.Data, reqObj); err != nil {
		return err
	}

	store := ch.server.GetPartition(req.PartitionID)
	if store == nil {
		msg := fmt.Sprintf("partition not found, partitionId:[%d], nodeID:[%d], node ip:[%s]", req.PartitionID, ch.server.nodeID, ch.server.ip)
		log.Error("%s", msg)
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, errors.New(msg))
	}

	if !store.IsLeader() {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_LEADER, nil)
	}

	server, err := ch.server.client.Master().QueryServer(ctx, reqObj.NodeID)
	if server == nil && reqObj.Method == proto.ConfRemoveNode {
		failServer := ch.server.client.Master().QueryFailServerByNodeID(ctx, reqObj.NodeID)
		if failServer != nil && failServer.Node != nil {
			server = failServer.Node
			log.Debug("Get server by failserver record %v.", server)
			err = nil
		}
	}
	if err != nil {
		log.Error("Get server info err %s", err.Error())
		return err
	}

	if reqObj.Method == proto.ConfAddNode {
		ch.server.raftResolver.AddNode(reqObj.NodeID, server.Replica())
	}

	if err := store.ChangeMember(reqObj.Method, server); err != nil {
		return err
	}

	if reqObj.Method == proto.ConfRemoveNode {
		ch.server.raftResolver.DeleteNode(reqObj.NodeID)
	}
	return nil
}

// redirect some other to response and send err to status when happen
func psErrorChange(server *Server) handler.ErrorChangeFun {
	return func(ctx context.Context, err error, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
		if vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code == vearchpb.ErrorEnum_PARTITION_NOT_LEADER || err == raft.ErrNotLeader {
			store := server.GetPartition(req.PartitionID)
			if store == nil {
				msg := fmt.Sprintf("partition not found, partitionId:[%d], nodeID:[%d], node ip:[%s]", req.PartitionID, server.nodeID, server.ip)
				log.Error("%s", msg)
				return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, errors.New(msg))
			}
			id, _ := store.GetLeader()
			if id == 0 {
				reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_PARTITION_NO_LEADER}
			} else {
				bytes, err := json.Marshal(server.raftResolver.ToReplica(id))
				if err != nil {
					log.Error("Find raft resolver err[%s]", err.Error())
					return err
				}
				reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_PARTITION_NOT_LEADER, Msg: string(bytes)}
			}

			return nil
		}
		return err
	}
}

type EngineCfgHandler struct {
	server *Server
}

func (ch *EngineCfgHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	// get store engine
	log.Debug("Request pid [%+v]", req.PartitionID)
	partitonStore := ch.server.GetPartition(req.PartitionID)
	if partitonStore == nil {
		log.Debug("PartitonStore is nil.")
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, fmt.Errorf("partition (%v), partitonStore is nil ", req.PartitionID))
	}
	engine := partitonStore.GetEngine()
	if engine == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, fmt.Errorf("partition (%v), engine is nil ", req.PartitionID))
	}
	if req.Type == vearchpb.OpType_CREATE {
		err := engine.SetEngineCfg(req.Data)
		if err != nil {
			log.Debug("Cache info set error [%+v]", err)
		}
	} else if req.Type == vearchpb.OpType_GET {
		// invoke c interface
		log.Debug("Invoke cfg info is get")
		cfg := &entity.SpaceConfig{}
		err := engine.GetEngineCfg(cfg)
		if err != nil {
			log.Debug("Cache info set error [%+v]", err)
		}
		data, _ := json.Marshal(cfg)
		reply.Data = data
	}
	return nil
}

type BackupHandler struct {
	server *Server
}

const (
	forceUploadPattern = `(.*\.log|CURRENT|MANIFEST-.*|OPTIONS-.*)`
)

func (bh *BackupHandler) syncBackupFiles(ctx context.Context, minioClient *minio.Client, bucketName, backupPath, s3Path string) error {
	forceUploadRegex, err := regexp.Compile(forceUploadPattern)
	if err != nil {
		return fmt.Errorf("invalid force upload pattern: %v", err)
	}

	// Get local file list by walking through the backup directory
	localFiles := make(map[string]os.FileInfo)
	err = filepath.Walk(backupPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Only store non-directory files
		if !info.IsDir() {
			relPath, _ := filepath.Rel(backupPath, path)
			localFiles[relPath] = info
		}
		return nil
	})
	if err != nil {
		log.Error("Failed to walk backup directory: %v", err)
		return err
	}

	// Get S3 object list by listing objects in the S3 bucket
	s3Files := make(map[string]minio.ObjectInfo)
	objectCh := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    s3Path,
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			log.Error("Failed to list S3 objects: %v", object.Err)
			continue
		}
		relPath := strings.TrimPrefix(object.Key, s3Path+"/")
		s3Files[relPath] = object
	}

	// Upload files that exist locally but not in S3
	for localPath := range localFiles {
		isForceUpload := forceUploadRegex.MatchString(localPath)
		if _, exists := s3Files[localPath]; isForceUpload || !exists {
			fullPath := filepath.Join(backupPath, localPath)
			objectName := filepath.Join(s3Path, localPath)

			_, err := minioClient.FPutObject(ctx, bucketName, objectName, fullPath,
				minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err != nil {
				log.Error("Failed to upload file %s to S3: %v", localPath, err)
				continue
			}
			log.Info("Uploaded file to S3: %s", localPath)
		}
	}

	// Delete files that exist in S3 but not locally
	for file := range s3Files {
		if _, exists := localFiles[file]; !exists {
			err := minioClient.RemoveObject(ctx, bucketName,
				filepath.Join(s3Path, file), minio.RemoveObjectOptions{})
			if err != nil {
				log.Error("failed to remove S3 file %s: %v", file, err)
				continue
			}
			log.Info("Removed file from S3: %s", file)
		}
	}

	return nil
}

type S3PathBuilder struct {
	clusterName string
}

func NewS3PathBuilder(clusterName string) *S3PathBuilder {
	return &S3PathBuilder{
		clusterName: clusterName,
	}
}

func (b *S3PathBuilder) BuildObjectPath(parts ...string) string {
	allParts := make([]string, 0, len(parts)+1)
	allParts = append(allParts, b.clusterName)

	for _, part := range parts {
		if part != "" {
			allParts = append(allParts, part)
		}
	}

	return filepath.ToSlash(filepath.Join(allParts...))
}

func (b *S3PathBuilder) BuildBackupPath(dbName, spaceName string, backupID, pid uint32) string {
	return b.BuildObjectPath("backup", dbName, spaceName, fmt.Sprintf("%d/%d", backupID, pid))
}

func (b *S3PathBuilder) BuildExportPath(dbName, spaceName string, backupID int, fileName string) string {
	return b.BuildObjectPath("export", dbName, spaceName, strconv.Itoa(backupID), fileName)
}

func (bh *BackupHandler) export(ctx context.Context, pid uint32, backup *entity.BackupSpaceRequest, minioClient *minio.Client, dbName string, path string) {
	pathBuilder := NewS3PathBuilder(config.Conf().Global.Name)

	exportDir := fmt.Sprintf("%s/export", path)
	if _, err := os.Stat(exportDir); os.IsNotExist(err) {
		if err = os.Mkdir(exportDir, 0644); err != nil {
			log.Error("Failed to create export dir: %s", err)
			return
		}
	}

	partitonStore := bh.server.GetPartition(pid)
	if partitonStore == nil {
		log.Error("PartitonStore is nil.")
		return
	}
	space := partitonStore.GetSpace()

	bh.server.backupStatus[pid] = 1
	defer func() {
		bh.server.backupStatus[pid] = 0
	}()

	part := 0
	fileName := fmt.Sprintf("%d_%d.zst", backup.Part, part)
	objectName := pathBuilder.BuildExportPath(dbName, space.Name, backup.BackupID, fileName)
	doneName := pathBuilder.BuildExportPath(dbName, space.Name, backup.BackupID, fmt.Sprintf("%d.done", backup.Part))
	backupFileName := filepath.Join(exportDir, fileName)

	file, err := os.Create(backupFileName)
	if err != nil {
		log.Error("Failed to create file: %s", err)
		return
	}

	zw, err := zstd.NewWriter(file,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(2))
	if err != nil {
		log.Error("Failed to create zstd writer: %s", err)
		file.Close()
		return
	}

	nextDocid := int32(-1)
	total := 0

	defer func() {
		if zw != nil {
			zw.Close()
		}
		if file != nil {
			file.Close()
		}
	}()

	for {
		doc := &vearchpb.Document{
			PKey: fmt.Sprintf("%d", nextDocid),
		}
		err := partitonStore.GetDocument(ctx, true, doc, true, true)
		if err != nil {
			log.Error("Get document error [%+v]", err)
			break
		}

		docOut := make(map[string]any)
		docOut["_id"] = doc.PKey

		if len(doc.Fields) > 0 {
			returnFieldsMap := make(map[string]string)
			nextDocid, _ = document.DocFieldSerialize(doc, &space, returnFieldsMap, true, docOut)
		}

		value, err := json.Marshal(docOut)
		if err != nil {
			log.Error("Marshal document error [%+v]", err)
			break
		}

		if _, err = zw.Write(value); err != nil {
			log.Error("Failed to write to compressor: %s", err)
			break
		}

		if _, err = zw.Write([]byte("\n")); err != nil {
			log.Error("Failed to write newline: %s", err)
			break
		}

		total++

		if total%1000000 == 0 {
			log.Info("Compressed %d documents", total)

			zw.Close()
			zw = nil
			file.Close()
			file = nil

			_, err = minioClient.FPutObject(ctx, backup.S3Param.BucketName, objectName, backupFileName,
				minio.PutObjectOptions{ContentType: "application/zstd"})
			if err != nil {
				log.Error("Failed to upload backup file: %+v", err)
				return
			}
			log.Info("Backup success, file is [%s]", backupFileName)

			// remove old file
			if err = os.Remove(backupFileName); err != nil {
				log.Error("Failed to remove file: %s", err)
				return
			}

			part++
			fileName = fmt.Sprintf("%d_%d.zst", backup.Part, part)
			objectName = pathBuilder.BuildExportPath(dbName, space.Name, backup.BackupID, fileName)
			backupFileName = filepath.Join(exportDir, fileName)

			file, err = os.Create(backupFileName)
			if err != nil {
				log.Error("Failed to create file: %s", err)
				return
			}

			zw, err = zstd.NewWriter(file,
				zstd.WithEncoderLevel(zstd.SpeedDefault),
				zstd.WithEncoderConcurrency(2))
			if err != nil {
				log.Error("Failed to create zstd writer: %s", err)
				file.Close()
				file = nil
				return
			}
		}
	}

	if zw != nil {
		zw.Close()
		zw = nil
	}
	if file != nil {
		file.Close()
		file = nil
	}

	if total%1000000 != 0 {
		_, err = minioClient.FPutObject(ctx, backup.S3Param.BucketName, objectName, backupFileName,
			minio.PutObjectOptions{ContentType: "application/zstd"})
		if err != nil {
			log.Error("Failed to upload backup file: %+v", err)
			return
		}
	}

	doneFile := fmt.Sprintf("%s/done_%d", exportDir, backup.Part)
	if err := os.WriteFile(doneFile, fmt.Appendf(nil, "%d", total), 0644); err != nil {
		log.Error("Failed to create done file: %s", err)
		return
	}

	_, err = minioClient.FPutObject(ctx, backup.S3Param.BucketName, doneName, doneFile,
		minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		log.Error("Failed to upload done file: %+v", err)
		os.Remove(doneFile)
		return
	}

	if err := os.Remove(doneFile); err != nil {
		log.Error("Failed to remove done file: %s", err)
	}

	if err := os.Remove(backupFileName); err != nil {
		log.Error("Failed to remove backup file: %s", err)
	}

	log.Info("Export completed successfully. Total documents: %d", total)
}

func (bh *BackupHandler) create(ctx context.Context, pid uint32, backup *entity.BackupSpaceRequest, minioClient *minio.Client, dbName, spaceName string, path string) {
	pathBuilder := NewS3PathBuilder(config.Conf().Global.Name)

	bh.server.backupStatus[pid] = 1
	defer func() {
		bh.server.backupStatus[pid] = 0
	}()

	pathes := []string{
		"data",
		"bitmap",
	}

	for _, p := range pathes {
		backupLocalPath := strings.Join([]string{
			path, "backup", p,
		}, "/")

		s3Path := pathBuilder.BuildBackupPath(dbName, spaceName, uint32(backup.BackupID), backup.Part) + "/" + p
		if err := bh.syncBackupFiles(ctx, minioClient, backup.S3Param.BucketName, backupLocalPath, s3Path); err != nil {
			log.Error("Failed to sync backup files: %v", err)
			return
		}
	}

	remoteFiles := []string{
		fmt.Sprintf("%s-%d.schema", spaceName, backup.Part),
	}
	localFiles := []string{
		fmt.Sprintf("%s-%d.schema", spaceName, pid),
	}
	for i, file := range remoteFiles {
		backupLocalPath := strings.Join([]string{
			path, localFiles[i],
		}, "/")

		s3Path := pathBuilder.BuildBackupPath(dbName, spaceName, uint32(backup.BackupID), backup.Part) + "/" + file
		_, err := minioClient.FPutObject(ctx, backup.S3Param.BucketName, s3Path, backupLocalPath,
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			log.Error("Failed to upload file %s to S3: %v", backupLocalPath, err)
			continue
		}
	}

	log.Info("Backup success")
}

func (bh *BackupHandler) downloadDirectory(ctx context.Context, minioClient *minio.Client, bucketName, s3Path, localPath string) error {
	if err := os.MkdirAll(localPath, 0755); err != nil {
		return fmt.Errorf("can't mkdir: %v", err)
	}

	opts := minio.ListObjectsOptions{
		Prefix:    s3Path,
		Recursive: true,
	}

	objChan := minioClient.ListObjects(ctx, bucketName, opts)
	for obj := range objChan {
		if obj.Err != nil {
			log.Error("List objects failed: %v", obj.Err)
			continue
		}

		if strings.HasSuffix(obj.Key, "/") {
			continue
		}

		relPath := strings.TrimPrefix(obj.Key, s3Path+"/")
		destPath := filepath.Join(localPath, relPath)

		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			log.Error("Create dir failed %s: %v", filepath.Dir(destPath), err)
			continue
		}

		if err := minioClient.FGetObject(ctx, bucketName, obj.Key, destPath, minio.GetObjectOptions{}); err != nil {
			log.Error("Failed to download file %s: %v", obj.Key, err)
			continue
		}

		log.Debug("Downloaded: %s", relPath)
	}

	return nil
}

func (bh *BackupHandler) restore(ctx context.Context, pid uint32, backup *entity.BackupSpaceRequest, minioClient *minio.Client, dbName, spaceName string, path string) {
	pathBuilder := NewS3PathBuilder(config.Conf().Global.Name)

	engine := bh.server.GetPartition(pid).GetEngine()
	if engine == nil {
		log.Error("Engine is nil")
		return
	}

	engine.Close()
	times := 0
	for hasClosed := engine.HasClosed(); !hasClosed; hasClosed = engine.HasClosed() {
		log.Debug("Wait engine close")
		time.Sleep(time.Second * 10)
		times += 1
		if times > 10 {
			log.Error("Engine close timeout")
			return
		}
	}
	bh.server.backupStatus[pid] = 1
	defer func() {
		bh.server.backupStatus[pid] = 0
	}()

	pathes := []string{
		"data",
		"bitmap",
	}

	for _, p := range pathes {
		os.RemoveAll(filepath.Join(path, p))
		os.Mkdir(filepath.Join(path, p), 0644)

		localPath := strings.Join([]string{
			path, p,
		}, "/")

		s3Path := pathBuilder.BuildBackupPath(dbName, spaceName, uint32(backup.BackupID), backup.Part) + "/" + p
		// Get directory from s3
		if err := bh.downloadDirectory(ctx, minioClient, backup.S3Param.BucketName, s3Path, localPath); err != nil {
			log.Error("Failed to download backup files: %v", err)
			return
		}
	}

	remoteFiles := []string{
		fmt.Sprintf("%s-%d.schema", spaceName, backup.Part),
	}

	localFiles := []string{
		fmt.Sprintf("%s-%d.schema", spaceName, pid),
	}

	for i, file := range remoteFiles {
		localPath := strings.Join([]string{
			path, localFiles[i],
		}, "/")
		s3Path := pathBuilder.BuildBackupPath(dbName, spaceName, uint32(backup.BackupID), backup.Part) + "/" + file
		err := minioClient.FGetObject(ctx, backup.S3Param.BucketName, s3Path, localPath, minio.GetObjectOptions{})
		if err != nil {
			log.Error("Failed to download file %s: %v", file, err)
			return
		}
	}

	err := engine.Load()
	if err != nil {
		log.Error("Reload partition error:[%v]", err)
		return
	}
	log.Info("Restore success")
}

func (bh *BackupHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}
	// get store engine
	pid := req.PartitionID
	log.Debug("Request pid [%+v]", pid)

	partitonStore := bh.server.GetPartition(pid)
	if partitonStore == nil {
		log.Error("PartitonStore %d is nil.", pid)
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, fmt.Errorf("partition (%v), partitonStore is nil ", pid))
	}
	e := partitonStore.GetEngine()
	if e == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_INVALID, fmt.Errorf("partition (%v), engine is nil ", pid))
	}

	backup := new(entity.BackupSpaceRequest)
	if err := json.Unmarshal(req.Data, backup); err != nil {
		log.Error("Unmarshal backup data error [%+v]", err)
		return vearchpb.NewError(vearchpb.ErrorEnum_RPC_PARAM_ERROR, err)
	}
	if backup.Command != "create" && backup.Command != "restore" && backup.Command != "export" && backup.Command != "list" {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unknow command %s", backup.Command))
	}
	space := partitonStore.GetSpace()
	dbName, err := bh.server.client.Master().QueryDBId2Name(ctx, space.DBId)
	if err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("find db by id err: %s, data: %d", err.Error(), space.DBId))
	}

	engineConfig := entity.SpaceConfig{}
	err = e.GetEngineCfg(&engineConfig)
	if err != nil {
		log.Error("Get engine config error [%+v]", err)
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("get engine config error: %s", err.Error()))
	}

	minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
		Secure: backup.S3Param.UseSSL,
		Region: backup.S3Param.Region,
	})
	if err != nil {
		log.Error("Failed to create minio client: %+v", err)
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to create minio client: %s", err.Error()))
	}

	if bh.server.backupStatus[pid] != 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("backup status %d", bh.server.backupStatus[req.PartitionID]))
	}

	if backup.Command == "export" {
		go func() {
			status := &entity.EngineStatus{}
			err := e.GetEngineStatus(status)

			if err != nil {
				log.Error("Get engine status error [%+v]", err)
				return
			}
			for status.BackupStatus != 0 {
				log.Debug("Status.BackupStatus %d", status.BackupStatus)
				return
			}
			bh.export(ctx, pid, backup, minioClient, dbName, *engineConfig.Path)
		}()
	} else if backup.Command == "create" {
		go func() {
			status := &entity.EngineStatus{}
			err := e.GetEngineStatus(status)

			if err != nil {
				log.Error("Get engine status error [%+v]", err)
				return
			}
			for status.BackupStatus != 0 {
				log.Debug("Status.BackupStatus %d", status.BackupStatus)
				return
			}
			err = e.BackupSpace(backup.Command)
			if err != nil {
				log.Error("Failed to backup space: %+v", err)
				return
			}
			bh.create(ctx, pid, backup, minioClient, dbName, space.Name, *engineConfig.Path)
		}()
	} else if backup.Command == "restore" {
		go func() {
			bh.restore(ctx, pid, backup, minioClient, dbName, space.Name, *engineConfig.Path)
		}()
	}
	return nil
}

type ResourceLimitHandler struct {
	server *Server
}

func (rlh *ResourceLimitHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) (err error) {
	reply.Err = &vearchpb.Error{Code: vearchpb.ErrorEnum_SUCCESS}

	partitonStore := rlh.server.GetPartition(req.PartitionID)
	if partitonStore == nil {
		log.Debug("PartitonStore is nil, pid %d not found", req.PartitionID)
		return nil
	}

	resourceLimit := new(entity.ResourceLimit)
	if err := json.Unmarshal(req.Data, resourceLimit); err != nil {
		return err
	}
	// check resource or set
	if resourceLimit.ResourceExhausted != nil {
		partitonStore.GetPartition().ResourceExhausted = *resourceLimit.ResourceExhausted
	} else {
		if resource_exhausted, err := vearch_os.CheckResource(partitonStore.GetPartition().Path); err != nil {
			return err
		} else {
			partitonStore.GetPartition().ResourceExhausted = resource_exhausted
		}
	}
	log.Debug("Partition %d set ResourceExhausted as %v", req.PartitionID, partitonStore.GetPartition().ResourceExhausted)
	return nil
}
