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

package services

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
)

// Type aliases to maintain original usage without entity prefix
type (
	BackupVersion                = entity.BackupVersion
	BackupVersionStatus          = entity.BackupVersionStatus
	BackupTaskStatus             = entity.BackupTaskStatus
	PartitionBackupInfo          = entity.PartitionBackupInfo
	PartitionBackupOrRestoreTask = entity.PartitionBackupOrRestoreTask
	VersionInfo                  = entity.VersionInfo
	VersionStatusInfo            = entity.VersionStatusInfo
	CreateVersionRequest         = entity.CreateVersionRequest
)

type BackupService struct {
	client        *client.Client
	backupManager *BackupManager
}

func buildSpaceKey(dbName, spaceName string) string {
	return fmt.Sprintf("%s-%s", dbName, spaceName)
}

func NewBackupService(client *client.Client) *BackupService {
	return &BackupService{
		client:        client,
		backupManager: NewBackupManager(client),
	}
}

// Start starts the backup service, including the backup monitor and version manager
func (s *BackupService) Start() {
	if s.backupManager != nil {
		s.backupManager.Start()
		log.Info("BackupService started successfully")
	}
}

// GetBackupProgress gets the backup progress for the specified space
func (s *BackupService) GetBackupProgress(ctx context.Context, dbName, spaceName, versionID string) (*entity.BackupProgressResponse, error) {
	// Construct spaceKey in format: dbName-spaceName
	spaceKey := buildSpaceKey(dbName, spaceName)

	if s.backupManager == nil || s.backupManager.backupMonitor == nil {
		return nil, fmt.Errorf("backup service not initialized")
	}

	s.backupManager.muVersionCache.RLock()
	versionInfo, exists := s.backupManager.versionCache[versionID]
	s.backupManager.muVersionCache.RUnlock()

	if exists {
		switch versionInfo.Status {
		case VersionStatusCompleted:
			return &entity.BackupProgressResponse{
				Status:    BackupStatusStringCompleted,
				VersionID: versionID,
			}, nil
		case VersionStatusFailed:
			return &entity.BackupProgressResponse{
				Status:    BackupStatusStringFailed,
				VersionID: versionID,
			}, nil
		}
	}

	progress := s.backupManager.backupMonitor.GetBackupProgress(spaceKey)
	if progress.TotalTasks == 0 {
		s.backupManager.muVersionCache.RLock()
		versionInfo, exists := s.backupManager.versionCache[versionID]
		s.backupManager.muVersionCache.RUnlock()

		if exists {
			switch versionInfo.Status {
			case VersionStatusCompleted:
				return &entity.BackupProgressResponse{
					Status:    BackupStatusStringCompleted,
					VersionID: versionID,
				}, nil
			case VersionStatusFailed:
				return &entity.BackupProgressResponse{
					Status:    BackupStatusStringFailed,
					VersionID: versionID,
				}, nil
			}
		}
	}

	if progress.TotalTasks > 0 {
		progress.Status = BackupStatusStringRunning
		progress.VersionID = versionID
	}

	return progress, nil
}

// GetRestoreProgress gets the restore progress for the specified space
func (s *BackupService) GetRestoreProgress(ctx context.Context, dbName, spaceName string) (*entity.BackupProgressResponse, error) {
	spaceKey := buildSpaceKey(dbName, spaceName)

	if s.backupManager == nil || s.backupManager.backupMonitor == nil {
		return nil, fmt.Errorf("backup service not initialized")
	}

	s.backupManager.backupMonitor.mu.RLock()
	tasks, exists := s.backupManager.backupMonitor.tasks[spaceKey]
	var versionID string
	if exists {
		for _, task := range tasks {
			if task.TaskType == BackupTaskTypeRestore && task.BackupRequest != nil {
				versionID = task.BackupRequest.VersionID
				break
			}
		}
	}
	if versionID == "" {
		versionID = s.backupManager.backupMonitor.restoreVersionMapping[spaceKey]
		if versionID != "" {
			log.Debug("Found versionID from restoreVersionMapping: spaceKey=%s, versionID=%s", spaceKey, versionID)
		}
	}
	s.backupManager.backupMonitor.mu.RUnlock()

	if versionID != "" {
		s.backupManager.muVersionCache.RLock()
		versionInfo, cacheExists := s.backupManager.versionCache[versionID]
		s.backupManager.muVersionCache.RUnlock()

		if cacheExists {
			switch versionInfo.Status {
			case VersionStatusCompleted:
				return &entity.BackupProgressResponse{
					Status:    "completed",
					VersionID: versionID,
				}, nil
			case VersionStatusFailed:
				return &entity.BackupProgressResponse{
					Status:    "failed",
					VersionID: versionID,
				}, nil
			}
		}
	}

	progress := s.backupManager.backupMonitor.GetRestoreProgress(spaceKey)

	if progress.TotalTasks == 0 && versionID != "" {
		s.backupManager.muVersionCache.RLock()
		versionInfo, cacheExists := s.backupManager.versionCache[versionID]
		s.backupManager.muVersionCache.RUnlock()

		if cacheExists {
			switch versionInfo.Status {
			case VersionStatusCompleted:
				return &entity.BackupProgressResponse{
					Status:    "completed",
					VersionID: versionID,
				}, nil
			case VersionStatusFailed:
				return &entity.BackupProgressResponse{
					Status:    "failed",
					VersionID: versionID,
				}, nil
			}
		}
	}

	if versionID != "" && progress.VersionID == "" {
		progress.VersionID = versionID
	}

	return progress, nil
}

// DeleteBackupVersion deletes a backup version
func (s *BackupService) DeleteBackupVersion(ctx context.Context, dbName, spaceName, versionID string, backup *entity.BackupSpaceRequest) error {
	// Construct spaceKey in format: dbName-spaceName
	spaceKey := buildSpaceKey(dbName, spaceName)

	mc := s.client.Master()

	// Query database and space information
	dbID, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return fmt.Errorf("failed to query database %s: %v", dbName, err)
	}

	space, err := mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		return fmt.Errorf("failed to query space %s/%s: %v", dbName, spaceName, err)
	}
	if space == nil {
		return fmt.Errorf("space %s/%s not found", dbName, spaceName)
	}

	if space.Partitions == nil || len(space.Partitions) == 0 {
		log.Warn("Space %s/%s has no partitions, nothing to delete", dbName, spaceName)
		return nil
	}

	log.Info("DeleteBackupVersion: deleting version %s for space %s/%s, found %d partitions",
		versionID, dbName, spaceName, len(space.Partitions))

	// Iterate through all partitions and send delete request to each partition's Leader node
	var deleteErrors []error
	for _, partition := range space.Partitions {
		if partition == nil {
			continue
		}

		// Select Leader node, if no Leader then select the first replica
		nodeID := partition.LeaderID
		if nodeID == 0 && len(partition.Replicas) > 0 {
			nodeID = partition.Replicas[0]
		}
		if nodeID == 0 {
			log.Warn("Partition %d has no valid node, skipping", partition.Id)
			continue
		}

		// Query server information
		server, err := mc.QueryServer(ctx, nodeID)
		if err != nil {
			log.Error("Failed to query server %d for partition %d: %v", nodeID, partition.Id, err)
			deleteErrors = append(deleteErrors, fmt.Errorf("failed to query server %d: %v", nodeID, err))
			continue
		}
		if server == nil {
			log.Warn("Server %d not found for partition %d, skipping", nodeID, partition.Id)
			continue
		}

		// Call PS-side DeleteBackupVersion RPC
		log.Info("DeleteBackupVersion: calling PS node %d (addr: %s) for partition %d",
			nodeID, server.RpcAddr(), partition.Id)

		err = client.DeleteBackupVersion(server.RpcAddr(), spaceKey, versionID, partition.Id)
		if err != nil {
			log.Error("Failed to delete backup version on partition %d (node %d): %v",
				partition.Id, nodeID, err)
			deleteErrors = append(deleteErrors, fmt.Errorf("partition %d: %v", partition.Id, err))
			continue
		}

		log.Info("DeleteBackupVersion: successfully deleted version %s for partition %d on node %d",
			versionID, partition.Id, nodeID)
	}

	// If there are errors, return the first error
	if len(deleteErrors) > 0 {
		log.Error("DeleteBackupVersion: encountered %d errors out of %d partitions",
			len(deleteErrors), len(space.Partitions))
		return fmt.Errorf("failed to delete backup version on %d partitions: %v",
			len(deleteErrors), deleteErrors[0])
	}

	log.Info("DeleteBackupVersion: successfully deleted version %s for all %d partitions",
		versionID, len(space.Partitions))
	return nil
}

func (s *BackupService) backupSchema(ctx context.Context, dbName, spaceName string, backup *entity.BackupSpaceRequest) (res *entity.BackupSpaceResponse, err error) {
	res = &entity.BackupSpaceResponse{

		BackupID: backup.BackupID,
	}
	mc := s.client.Master()
	dbID, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return res, err
	}

	space, err := mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		return res, err
	}

	spaceJson, err := json.Marshal(space)
	if err != nil {
		log.Error("json.Marshal err: %v", err)
		return res, err
	}

	backupFileName := space.Name + ".schema"

	err = os.WriteFile(backupFileName, spaceJson, 0644)
	if err != nil {
		err := fmt.Errorf("error writing to file: %v", err)
		log.Error(err)
		return res, err
	}

	minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
		Secure: backup.S3Param.UseSSL,
		Region: backup.S3Param.Region,
	})
	if err != nil {
		err = fmt.Errorf("failed to create minio client: %+v", err)
		log.Error(err)
		return res, err
	}
	bucketName := backup.S3Param.BucketName

	var objectName string
	if backup.Command == BackupCommandCreate {
		objectName = filepath.Join(config.Conf().Global.Name, "backup", dbName, space.Name, fmt.Sprintf("%d/%s.schema", backup.BackupID, space.Name))
	} else if backup.Command == BackupCommandExport {
		// res.BackupID is timestamp
		res.BackupID = int(time.Now().Unix())
		backup.BackupID = res.BackupID
		objectName = filepath.Join(config.Conf().Global.Name, "export", dbName, space.Name, fmt.Sprintf("%d/%s.schema", res.BackupID, space.Name))
	}
	_, err = minioClient.FPutObject(ctx, bucketName, objectName, backupFileName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		err = fmt.Errorf("failed to backup space: %+v", err)
		log.Error(err)
		return res, err
	}
	log.Info("backup schema success, file is [%s]", backupFileName)
	os.Remove(backupFileName)
	return res, nil
}

func (s *BackupService) restoreSchema(ctx context.Context, dbService *DBService, spaceService *SpaceService, configService *ConfigService, dbName, spaceName string, backup *entity.BackupSpaceRequest) (res *entity.BackupSpaceResponse, err error) {
	res = &entity.BackupSpaceResponse{
		BackupID: backup.BackupID,
	}
	mc := s.client.Master()
	dbID, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return res, err
	}

	_, err = mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err == nil {
		err = fmt.Errorf("space duplicate: space %s already exists in database %s", spaceName, dbName)
		return res, err
	}
	minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
		Secure: backup.S3Param.UseSSL,
		Region: backup.S3Param.Region,
	})
	if err != nil {
		err := fmt.Errorf("failed to create minio client: %+v", err)
		return res, err
	}

	backupFileName := spaceName + ".schema"
	bucketName := backup.S3Param.BucketName
	objectName := fmt.Sprintf("%s/backup/%s/%s/%d/%s.schema", config.Conf().Global.Name, dbName, spaceName, backup.BackupID, spaceName)
	err = minioClient.FGetObject(ctx, bucketName, objectName, backupFileName, minio.GetObjectOptions{})
	if err != nil {
		err := fmt.Errorf("failed to download %s from S3: %+v", objectName, err)
		return res, err
	}
	defer os.Remove(backupFileName)
	log.Info("downloaded backup file from S3: %s", backupFileName)

	spaceJson, err := os.ReadFile(backupFileName)
	if err != nil {
		err := fmt.Errorf("error read file:%v", err)
		log.Error(err)
		return res, err
	}

	log.Debug("%s", spaceJson)
	space := &entity.Space{}
	err = json.Unmarshal(spaceJson, space)
	if err != nil {
		err := fmt.Errorf("unmarshal file: %v", err)
		return res, err
	}

	partitionNum := len(space.Partitions)
	space.Partitions = make([]*entity.Partition, 0)

	s3Path := filepath.Join(config.Conf().Global.Name, "backup", dbName, spaceName, fmt.Sprintf("%d", backup.BackupID))
	objectCh := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    s3Path + "/",
		Recursive: false,
	})

	partitionDirs := make(map[string]bool)
	for object := range objectCh {
		if object.Err != nil {
			log.Error("failed to list S3 objects: %v", object.Err)
			continue
		}

		relativePath := strings.TrimPrefix(object.Key, s3Path+"/")

		if relativePath == "" {
			continue
		}

		if strings.HasSuffix(relativePath, "/") {
			dirName := strings.TrimSuffix(relativePath, "/")
			if !strings.Contains(dirName, "/") && dirName != "" {
				partitionDirs[dirName] = true
				log.Info("Found partition directory: %s", dirName)
			}
		}
	}

	log.Info("Total partition directories found: %d, expected: %d", len(partitionDirs), partitionNum)
	log.Debug("Partition directories: %v", partitionDirs)

	if len(partitionDirs) != partitionNum {
		err = fmt.Errorf("S3 partition directory count %d does not match schema partition count %d, found directories: %v",
			len(partitionDirs), partitionNum, partitionDirs)
		return res, err
	}

	if err := spaceService.CreateSpace(ctx, dbService, dbName, space); err != nil {
		log.Error("createSpace err: %v", err)
		return res, err
	}

	cfg, err := configService.GetSpaceConfigByName(ctx, dbName, spaceName)
	if err != nil {
		log.Error("get space config err: %s", err.Error())
		return res, err
	}

	err = configService.UpdateSpaceConfig(ctx, space, cfg)
	if err != nil {
		log.Error("update space config err: %s", err.Error())
		return res, err
	}
	return res, nil
}

func extractBackupID(key string) (int64, error) {
	parts := strings.Split(key, "/")
	if len(parts) < 5 {
		return 0, fmt.Errorf("invalid backup key format: %s", key)
	}

	backupIDStr := parts[len(parts)-2]

	backupID, err := strconv.ParseInt(backupIDStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid backup ID: %s, err: %v", backupIDStr, err)
	}

	return backupID, nil
}

func list(ctx context.Context, backup *entity.BackupSpaceRequest, minioClient *minio.Client, dbName, spaceName string) (backupIDs []int) {
	s3Path := filepath.Join(config.Conf().Global.Name, "backup", dbName, spaceName)
	objectCh := minioClient.ListObjects(ctx, backup.S3Param.BucketName, minio.ListObjectsOptions{
		Prefix:    s3Path + "/",
		Recursive: false,
	})
	backupIDs = make([]int, 0)

	for object := range objectCh {
		if object.Err != nil {
			log.Error("failed to list S3 objects: %v", object.Err)
			continue
		}
		relPath := strings.TrimPrefix(object.Key, s3Path+"/")
		relPath = strings.TrimSuffix(relPath, "/")
		log.Info("object: %s", relPath)
		backupID, err := strconv.ParseInt(relPath, 10, 64)
		if err != nil {
			log.Error("failed to parse backup ID: %v", err)
			continue
		}
		backupIDs = append(backupIDs, int(backupID))
	}
	return backupIDs
}

func (s *BackupService) BackupSpace(ctx context.Context, dbService *DBService, spaceService *SpaceService, configService *ConfigService, dbName, spaceName string, req *entity.BackupSpaceRequest) (res *entity.BackupSpaceResponse, err error) {
	// bucket/cluster/backup/db/space/backup_id/data_file
	// bucket/cluster/export/db/space/json_file

	res = &entity.BackupSpaceResponse{}
	// scan space checkpoint list
	minioClient, err := minio.New(req.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(req.S3Param.AccessKey, req.S3Param.SecretKey, ""),
		Secure: req.S3Param.UseSSL,
		Region: req.S3Param.Region,
	})
	if err != nil {
		err := fmt.Errorf("failed to create minio client: %+v", err)
		log.Error(err)
		return nil, err
	}

	if req.Command == BackupCommandList {
		res.BackupIDs = list(ctx, req, minioClient, dbName, spaceName)
		return res, nil
	}

	if req.Command == BackupCommandCreate {
		path := filepath.Join(config.Conf().Global.Name, "backup", dbName, spaceName)
		objectCh := minioClient.ListObjects(ctx, req.S3Param.BucketName, minio.ListObjectsOptions{
			Prefix: path,
		})
		backupID := int64(1) // only support bacup ID 1
		for object := range objectCh {
			if object.Err != nil {
				log.Error("failed to list objects: %v", object.Err)
				continue
			}

			id, err := extractBackupID(object.Key)
			if err != nil {
				log.Error("failed to extract backup ID: %v", err)
				continue
			}
			log.Debug("found backup ID: %d", id)
			if id > backupID {
				backupID = id
			}
		}
		req.BackupID = int(backupID)
	}
	if req.Command == BackupCommandCreate || req.Command == BackupCommandExport {
		res, err = s.backupSchema(ctx, dbName, spaceName, req)
		if err != nil {
			log.Error(err)
			return nil, err
		}
	} else if req.Command == BackupCommandRestore {
		res, err = s.restoreSchema(ctx, dbService, spaceService, configService, dbName, spaceName, req)
		if err != nil {
			log.Error(err)
			return nil, err
		}
	}

	mc := s.client.Master()
	// get space info
	dbID, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		log.Error(err)
	}

	space, err := mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		log.Error(err)
	}
	if space == nil || space.Partitions == nil {
		return nil, nil
	}

	// invoke all space nodeID
	s3PartitionMap := make(map[entity.PartitionID]entity.PartitionID, 0)
	if req.Command == BackupCommandRestore {
		s3Path := filepath.Join(config.Conf().Global.Name, "backup", dbName, spaceName, fmt.Sprintf("%d", req.BackupID))
		objectCh := minioClient.ListObjects(ctx, req.S3Param.BucketName, minio.ListObjectsOptions{
			Prefix:    s3Path + "/",
			Recursive: false,
		})

		i := 0
		p := space.Partitions[i]
		for object := range objectCh {
			if object.Err != nil {
				log.Error("failed to list S3 objects: %v", object.Err)
				continue
			}
			relPath := strings.TrimPrefix(object.Key, s3Path+"/")
			relPath = strings.TrimSuffix(relPath, "/")
			log.Info("object: %s", relPath)
			partitionID, err := strconv.ParseInt(relPath, 10, 64)
			if err != nil {
				log.Error("failed to parse partition ID: %v", err)
				continue
			}
			s3PartitionMap[p.Id] = entity.PartitionID(partitionID)
			i++
			if i < len(space.Partitions) {
				p = space.Partitions[i]
			}
		}
	} else if req.Command == BackupCommandCreate || req.Command == BackupCommandExport {
		remoteBasePid := entity.PartitionID(math.MaxUint32)
		for _, p := range space.Partitions {
			partition, err := mc.QueryPartition(ctx, p.Id)
			if err != nil {
				log.Error(err)
				continue
			}
			if partition.Replicas == nil {
				continue
			}

			if remoteBasePid > partition.Id {
				remoteBasePid = partition.Id
			}
		}
		for _, p := range space.Partitions {
			partition, err := mc.QueryPartition(ctx, p.Id)
			if err != nil {
				log.Error(err)
				continue
			}
			if partition.Replicas == nil {
				continue
			}
			s3PartitionMap[p.Id] = p.Id - remoteBasePid + 1
		}
	}

	for _, p := range space.Partitions {
		partition, err := mc.QueryPartition(ctx, p.Id)
		if err != nil {
			log.Error(err)
			continue
		}
		if partition.Replicas == nil {
			continue
		}

		if len(partition.Replicas) == 1 && partition.LeaderID == 0 {
			server, err := mc.QueryServer(ctx, partition.Replicas[0])
			if err != nil {
				log.Error(err)
				continue
			}
			log.Debug("invoke nodeID [%+v], address [%+v]", partition.Id, server.RpcAddr())
			req.Part = s3PartitionMap[partition.Id]
			err = client.BackupSpace(server.RpcAddr(), req, partition.Id)
			if err != nil {
				log.Error(err)
				continue
			}
		} else {
			for _, nodeID := range partition.Replicas {
				log.Debug("nodeID is [%+v], partition is [%+v], [%+v]", nodeID, partition.Id, partition.LeaderID)
				if nodeID != partition.LeaderID && req.Command != BackupCommandRestore {
					continue
				}
				server, err := mc.QueryServer(ctx, nodeID)
				if err != nil {
					log.Error(err)
					continue
				}
				log.Debug("invoke nodeID [%v], partition [%v] address [%+v]", nodeID, partition.Id, server.RpcAddr())
				req.Part = s3PartitionMap[partition.Id]
				err = client.BackupSpace(server.RpcAddr(), req, partition.Id)
				if err != nil {
					log.Error(err)
					continue
				}
			}
		}
	}
	return res, nil
}

func (s *BackupService) SpaceSnapshot(ctx context.Context, dbService *DBService, spaceService *SpaceService,
	configService *ConfigService, dbName, spaceName string,
	req *entity.BackupSpaceRequest) (res *entity.BackupSpaceResponse, err error) {

	backupManager := s.backupManager

	var backupID string
	if req.BackupID == 0 {
		backupID = uuid.NewString()
		log.Info("Generated new BackupID (UUID): %s for space %s/%s", backupID, dbName, spaceName)
	} else {
		backupID = strconv.Itoa(req.BackupID)
	}

	snapshotReq := &entity.BackupOrRestoreRequest{
		Database:          dbName,
		Space:             spaceName,
		BackupID:          backupID,
		VersionID:         req.VersionID,
		SourceClusterName: req.SourceClusterName,
		S3Param: struct {
			Region     string `json:"region"`
			BucketName string `json:"bucket_name"`
			EndPoint   string `json:"endpoint"`
			AccessKey  string `json:"access_key"`
			SecretKey  string `json:"secret_key"`
			UseSSL     bool   `json:"use_ssl"`
		}{
			Region:     req.S3Param.Region,
			BucketName: req.S3Param.BucketName,
			EndPoint:   req.S3Param.EndPoint,
			AccessKey:  req.S3Param.AccessKey,
			SecretKey:  req.S3Param.SecretKey,
			UseSSL:     req.S3Param.UseSSL,
		},
	}

	if req.Command == BackupCommandBackup {
		backupVersion, err := backupManager.createSnapshot(ctx, snapshotReq)
		if err != nil {
			log.Error("createSnapshot failed: %v", err)
			return nil, err
		}

		res = &entity.BackupSpaceResponse{
			BackupID:  req.BackupID,
			VersionID: backupVersion.VersionID,
		}

		log.Info("Backup created successfully for space %s/%s, versionID=%s, backupID=%s",
			dbName, spaceName, backupVersion.VersionID, backupVersion.BackupID)
		return res, nil

	} else if req.Command == BackupCommandRestore {
		if req.SourceClusterName != "" {
			if req.VersionID == "" && req.BackupID == 0 {
				return nil, fmt.Errorf("when source_cluster_name is specified, either version_id or backup_id must be provided")
			}
		}
		_, err = backupManager.restoreSnapshot(ctx, dbService, spaceService, configService, dbName, spaceName, snapshotReq)
		if err != nil {
			log.Error("restoreSnapshot failed: %v", err)
			return nil, err
		}

		res = &entity.BackupSpaceResponse{
			BackupID:  req.BackupID,
			VersionID: snapshotReq.VersionID,
		}

		log.Info("Restore started successfully for space %s/%s, backupID=%s, versionID=%s",
			dbName, spaceName, snapshotReq.BackupID, snapshotReq.VersionID)
		return res, nil

	} else {
		return nil, fmt.Errorf("unknown command: %s, SpaceSnapshot only supports backup/restore", req.Command)
	}
}

// ============================================================================
// VersionManager - version manager on master side
// ============================================================================
type VersionManager struct {
	mu          sync.RWMutex
	client      *client.Client
	minioClient *minio.Client
	bucketName  string

	versionCache map[string]*VersionInfo

	stopChan chan struct{}
}

func (vm *VersionManager) Start() error {
	vm.mu.Lock()
	if vm.versionCache == nil {
		vm.versionCache = make(map[string]*VersionInfo)
	}
	if vm.stopChan == nil {
		vm.stopChan = make(chan struct{})
	}
	vm.mu.Unlock()

	log.Info("Version manager started successfully")
	return nil
}

func (vm *VersionManager) getOrCreateVersionInfo(spaceKey string) *VersionInfo {
	versionInfo, ok := vm.versionCache[spaceKey]
	if !ok {
		versionInfo = &VersionInfo{
			SpaceKey:    spaceKey,
			Versions:    make([]*BackupVersion, 0),
			LastUpdated: time.Now(),
			TotalCount:  0,
		}
		vm.versionCache[spaceKey] = versionInfo
	}
	return versionInfo
}

// BackupVersionStatus constants
const (
	VersionStatusInited BackupVersionStatus = iota
	VersionStatusRunning
	VersionStatusFailed
	VersionStatusExpired
	VersionStatusDeleted
	VersionStatusCompleted
)

// PS snapshot status constants
const (
	PSSnapshotStatusRunning   int = iota // 0
	PSSnapshotStatusCompleted            // 1
	PSSnapshotStatusFailed               // 2
)

// BackupTaskStatus constants
const (
	BackupStatusInited BackupTaskStatus = iota
	BackupStatusRunning
	BackupStatusCompleted
	BackupStatusFailed
)

// Backup command constants
const (
	BackupCommandBackup  = "backup"
	BackupCommandRestore = "restore"
	BackupCommandCreate  = "create"
	BackupCommandExport  = "export"
	BackupCommandList    = "list"
)

// Backup task type constants
const (
	BackupTaskTypeBackup  = "backup"
	BackupTaskTypeRestore = "restore"
)

// Backup status string constants
const (
	BackupStatusStringCompleted = "completed"
	BackupStatusStringFailed    = "failed"
	BackupStatusStringRunning   = "running"
	BackupStatusStringNotFound  = "not_found"
)

// Backup description constants
const (
	BackupDescriptionBackup  = "backup"
	BackupDescriptionRestore = "restore"
)

// CreateVersion creates a new version
func (vm *VersionManager) CreateVersion(ctx context.Context, req *CreateVersionRequest) (*BackupVersion, error) {
	spaceKey := buildSpaceKey(req.DBName, req.SpaceName)

	// Create version object
	version := &BackupVersion{
		SpaceKey:    spaceKey,
		VersionID:   req.VersionID,
		BackupID:    req.BackupID,
		CreateTime:  time.Now(),
		Status:      VersionStatusInited,
		Description: req.Description,
		Partitions:  req.Partitions,
	}
	log.Info("Created new version %s for space %s", req.VersionID, spaceKey)

	vm.mu.Lock()
	if vm.versionCache == nil {
		vm.versionCache = make(map[string]*VersionInfo)
	}
	versionInfo := vm.getOrCreateVersionInfo(spaceKey)
	versionInfo.Versions = append(versionInfo.Versions, version)
	versionInfo.LastUpdated = time.Now()
	vm.mu.Unlock()

	log.Info("Created new version %s for space %s", req.VersionID, spaceKey)
	return version, nil
}

func (vm *VersionManager) RestoreVersion(ctx context.Context, req *CreateVersionRequest) (*BackupVersion, error) {
	spaceKey := buildSpaceKey(req.DBName, req.SpaceName)

	// Create version object
	version := &BackupVersion{
		SpaceKey:    spaceKey,
		VersionID:   req.VersionID,
		BackupID:    req.BackupID,
		CreateTime:  time.Now(),
		Status:      VersionStatusInited,
		Description: BackupDescriptionRestore,
		Partitions:  req.Partitions,
	}
	log.Info("restore version %s for space %s", req.VersionID, spaceKey)

	// Get or create version information
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if vm.versionCache == nil {
		vm.versionCache = make(map[string]*VersionInfo)
	}
	versionInfo := vm.getOrCreateVersionInfo(spaceKey)

	versionInfo.LastUpdated = time.Now()

	log.Info("Created new version %s for space %s", req.VersionID, spaceKey)
	return version, nil
}

func (vm *VersionManager) generateVersionID() string {
	now := time.Now()
	timestamp := now.Format("200601021504") // Go format string: 2025-11-12 15:04:05
	return timestamp
}

// ============================================================================
// BackupMonitor - monitor on master side
// ============================================================================

func (bm *BackupMonitor) updateTaskStatus(task *PartitionBackupOrRestoreTask, BackupTaskRunning BackupTaskStatus) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	task.Status = BackupTaskRunning
	task.StartTime = time.Now()
}

func (bm *BackupMonitor) markTaskFailed(task *PartitionBackupOrRestoreTask, err error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if task.Status != BackupStatusFailed {
		task.Status = BackupStatusFailed
		task.CompleteTime = time.Now()
		task.LastError = err
		if err != nil {
			task.LastErrorMsg = err.Error()
		}
	}
}

// ============================================================================
// BackupMonitor - monitor on master side
// ============================================================================
type BackupMonitor struct {
	mu     sync.RWMutex
	client *client.Client
	// key: backupID-partitionTasks
	tasks map[string][]*PartitionBackupOrRestoreTask

	ProcessingVersion []*BackupVersion

	restoreVersionMapping map[string]string

	healthCheckChan     chan entity.NodeID
	stopChan            chan struct{}
	updateVersionStatus func(versionID string, status BackupVersionStatus)
}

func NewBackupMonitor(client *client.Client) *BackupMonitor {
	return &BackupMonitor{
		client:                client,
		tasks:                 make(map[string][]*PartitionBackupOrRestoreTask),
		ProcessingVersion:     make([]*BackupVersion, 0),
		restoreVersionMapping: make(map[string]string),
		healthCheckChan:       make(chan entity.NodeID, 100),
		stopChan:              make(chan struct{}),
	}
}

func (bm *BackupMonitor) Start() {
	go bm.runningTasksMonitor()
	go bm.versionChecker()
}

// versionChecker checks if running backup versions are completed
func (bm *BackupMonitor) versionChecker() {
	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		ticker.Stop()
		if r := recover(); r != nil {
			log.Error("versionChecker panic recovered: %v", r)
		}
	}()

	for {
		select {
		case <-ticker.C:
			bm.checkVersionStatus()
		case <-bm.stopChan:
			return
		}
	}
}

// checkVersionStatus checks version status
func (bm *BackupMonitor) checkVersionStatus() {
	type completedInfo struct {
		idx       int
		versionID string
		status    BackupVersionStatus
	}
	var completed []completedInfo

	bm.mu.Lock()
	for idx, version := range bm.ProcessingVersion {
		spaceKey := version.SpaceKey
		tasks := bm.tasks[spaceKey]
		if len(tasks) == 0 {
			continue
		}

		allCompleted := true
		hasFailed := false
		hasRunning := false

		for _, task := range tasks {
			switch task.Status {
			case BackupStatusCompleted:
				// Task completed
			case BackupStatusFailed:
				hasFailed = true
				allCompleted = false
			case BackupStatusRunning, BackupStatusInited:
				hasRunning = true
				allCompleted = false
			default:
				allCompleted = false
			}
		}

		if allCompleted {
			version.Status = VersionStatusCompleted
			completed = append(completed, completedInfo{idx: idx, versionID: version.VersionID, status: VersionStatusCompleted})
		} else if hasFailed && !hasRunning {
			version.Status = VersionStatusFailed
			completed = append(completed, completedInfo{idx: idx, versionID: version.VersionID, status: VersionStatusFailed})
			log.Error("Version %s (backupID: %s) marked as failed due to partition backup task failures", version.VersionID, version.BackupID)
		}
	}
	bm.mu.Unlock()

	for _, info := range completed {
		if bm.updateVersionStatus != nil {
			bm.updateVersionStatus(info.versionID, info.status)
		}
	}

	if len(completed) > 0 {
		bm.mu.Lock()
		for i := len(completed) - 1; i >= 0; i-- {
			idx := completed[i].idx
			version := bm.ProcessingVersion[idx]
			spaceKey := version.SpaceKey

			if completed[i].status == VersionStatusCompleted {
				log.Info("Version %s (backupID: %s) marked as completed", version.VersionID, version.BackupID)
			} else {
				log.Info("Version %s (backupID: %s) marked as failed", version.VersionID, version.BackupID)
			}

			if _, exists := bm.tasks[spaceKey]; exists {
				delete(bm.tasks, spaceKey)
				log.Info("Cleaned up all tasks for spaceKey %s (version: %s, backupID: %s)",
					spaceKey, version.VersionID, version.BackupID)
			}

			bm.ProcessingVersion = append(bm.ProcessingVersion[:idx], bm.ProcessingVersion[idx+1:]...)
		}
		bm.mu.Unlock()
	}
}

// runningTasksMonitor monitors running tasks
func (bm *BackupMonitor) runningTasksMonitor() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		if r := recover(); r != nil {
			log.Error("runningTasksMonitor panic recovered: %v", r)
		}
	}()

	for {
		select {
		case <-ticker.C:
			bm.checkProcessingVersion()
			bm.checkRestoreTasks()
		case <-bm.stopChan:
			return
		}
	}
}

// checkProcessingVersion periodically checks running version tasks and updates partition backup status
func (bm *BackupMonitor) checkProcessingVersion() {
	ctx := context.Background()
	mc := bm.client.Master()

	for _, version := range bm.ProcessingVersion {
		spaceKey := version.SpaceKey
		tasks := bm.tasks[spaceKey]
		for _, task := range tasks {
			if task.PSNodeAddr == "" {
				server, err := mc.QueryServer(ctx, task.NodeID)
				if err != nil {
					log.Error("Failed to query server for node %d (partition %d): %v", task.NodeID, task.PartitionID, err)
					continue
				}
				task.PSNodeAddr = server.RpcAddr()
				log.Info("Updated PSNodeAddr for partition %d: %s", task.PartitionID, task.PSNodeAddr)
			}

			// RPC call to check backup status
			log.Info("Querying backup status: partitionID=%d, nodeID=%d, addr=%s, spaceKey=%s, backupID=%s",
				task.PartitionID, task.NodeID, task.PSNodeAddr, spaceKey, version.BackupID)
			statusResp, err := client.GetBackupStatus(task.PSNodeAddr, spaceKey, version.BackupID, task.PartitionID)
			if err != nil {
				log.Error("Failed to get backup status for partition %d (nodeID=%d, addr=%s): %v",
					task.PartitionID, task.NodeID, task.PSNodeAddr, err)
				continue
			}
			log.Info("get backup status response: partitionID=%d, nodeID=%d, status=%d, exists=%v",
				task.PartitionID, task.NodeID, statusResp.Status, statusResp.Exists)

			switch statusResp.Status {
			case PSSnapshotStatusRunning:
				task.Status = BackupStatusRunning
			case PSSnapshotStatusCompleted:
				if task.Status != BackupStatusCompleted {
					task.Status = BackupStatusCompleted
					task.CompleteTime = time.Now()
					log.Info("Task for partition %d marked as completed", task.PartitionID)
				}
			case PSSnapshotStatusFailed:
				if task.Status != BackupStatusFailed {
					task.Status = BackupStatusFailed
					task.CompleteTime = time.Now()
					if statusResp.ErrorMessage != "" {
						task.LastError = fmt.Errorf(statusResp.ErrorMessage)
						task.LastErrorMsg = statusResp.ErrorMessage
					}
					log.Error("Task for partition %d marked as failed: %s", task.PartitionID, statusResp.ErrorMessage)
				}
			default:
				log.Warn("Unknown status from PS for partition %d: %d", task.PartitionID, statusResp.Status)
			}

			if task.Status != BackupStatusRunning {
				continue
			}
			runningDuration := time.Since(task.StartTime)

			if !bm.isNodeHealthy(task.NodeID) {
				log.Warn("Detected node %d failure during backup execution for partition %d (running: %v)",
					task.NodeID, task.PartitionID, runningDuration)
				errMsg := fmt.Sprintf("node %d became unhealthy during backup execution", task.NodeID)
				task.LastError = fmt.Errorf(errMsg)
				task.LastErrorMsg = errMsg

				go bm.waitForPartitionMigration(task)

				log.Info("Task for partition %d marked as waiting for migration", task.PartitionID)
			} else {
				if runningDuration > 60*time.Minute {
					log.Warn("Task for partition %d on node %d has been running for %v (may be slow)",
						task.PartitionID, task.NodeID, runningDuration)
				}
			}
		}
	}
}

// checkRestoreTasks periodically checks restore task status
func (bm *BackupMonitor) checkRestoreTasks() {
	ctx := context.Background()
	mc := bm.client.Master()

	bm.mu.RLock()
	tasksCopy := make(map[string][]*PartitionBackupOrRestoreTask)
	for spaceKey, tasks := range bm.tasks {
		tasksCopy[spaceKey] = tasks
	}
	bm.mu.RUnlock()

	type completedRestoreInfo struct {
		spaceKey  string
		versionID string
		status    BackupVersionStatus
	}
	completedRestores := make([]completedRestoreInfo, 0)

	for spaceKey, tasks := range tasksCopy {
		hasRestoreTask := false
		allRestoreCompleted := true
		hasFailed := false
		hasRunning := false
		var restoreVersionID string

		for _, task := range tasks {
			if task.TaskType != BackupTaskTypeRestore {
				continue
			}

			hasRestoreTask = true

			// Get and record versionID
			if task.BackupRequest != nil && restoreVersionID == "" {
				restoreVersionID = task.BackupRequest.VersionID
			}

			if task.PSNodeAddr == "" {
				server, err := mc.QueryServer(ctx, task.NodeID)
				if err != nil {
					log.Error("Failed to query server for node %d (partition %d): %v", task.NodeID, task.PartitionID, err)
					continue
				}
				task.PSNodeAddr = server.RpcAddr()
				log.Info("Updated PSNodeAddr for restore partition %d: %s", task.PartitionID, task.PSNodeAddr)
			}

			// Get backupID
			var backupID string
			if task.BackupRequest != nil {
				backupID = task.BackupRequest.BackupID
			}
			if backupID == "" {
				log.Warn("Restore task for partition %d has no backupID, skipping status check", task.PartitionID)
				continue
			}

			// RPC call to check restore status
			log.Debug("Querying restore status: partitionID=%d, nodeID=%d, addr=%s, spaceKey=%s, backupID=%s",
				task.PartitionID, task.NodeID, task.PSNodeAddr, spaceKey, backupID)
			statusResp, err := client.GetBackupStatus(task.PSNodeAddr, spaceKey, backupID, task.PartitionID)
			if err != nil {
				log.Error("Failed to get restore status for partition %d (nodeID=%d, addr=%s): %v",
					task.PartitionID, task.NodeID, task.PSNodeAddr, err)
				continue
			}
			log.Debug("get restore status response: partitionID=%d, nodeID=%d, status=%d, exists=%v",
				task.PartitionID, task.NodeID, statusResp.Status, statusResp.Exists)

			// Status mapping: PS-side status needs to be correctly mapped to Master-side status
			switch statusResp.Status {
			case PSSnapshotStatusRunning:
				task.Status = BackupStatusRunning
				hasRunning = true
				allRestoreCompleted = false
			case PSSnapshotStatusCompleted:
				if task.Status != BackupStatusCompleted {
					task.Status = BackupStatusCompleted
					task.CompleteTime = time.Now()
					log.Info("Restore task for partition %d marked as completed", task.PartitionID)
				}
			case PSSnapshotStatusFailed:
				if task.Status != BackupStatusFailed {
					task.Status = BackupStatusFailed
					task.CompleteTime = time.Now()
					if statusResp.ErrorMessage != "" {
						task.LastError = fmt.Errorf(statusResp.ErrorMessage)
						task.LastErrorMsg = statusResp.ErrorMessage
					}
					log.Error("Restore task for partition %d marked as failed: %s", task.PartitionID, statusResp.ErrorMessage)
				}
				hasFailed = true
				allRestoreCompleted = false
			default:
				log.Warn("Unknown restore status from PS for partition %d: %d", task.PartitionID, statusResp.Status)
				allRestoreCompleted = false
			}

			if task.Status == BackupStatusRunning {
				runningDuration := time.Since(task.StartTime)
				if runningDuration > 60*time.Minute {
					log.Warn("Restore task for partition %d on node %d has been running for %v (may be slow)",
						task.PartitionID, task.NodeID, runningDuration)
				}
			}
		}

		if hasRestoreTask {
			if allRestoreCompleted && !hasRunning {
				var status BackupVersionStatus
				if hasFailed {
					status = VersionStatusFailed
					log.Info("All restore tasks for space %s completed with failures", spaceKey)
				} else {
					status = VersionStatusCompleted
					log.Info("All restore tasks for space %s completed successfully", spaceKey)
				}

				if restoreVersionID != "" {
					completedRestores = append(completedRestores, completedRestoreInfo{
						spaceKey:  spaceKey,
						versionID: restoreVersionID,
						status:    status,
					})
				}
			}
		}
	}

	for _, info := range completedRestores {
		if bm.updateVersionStatus != nil {
			// Use versionID to record to versionCache
			bm.updateVersionStatus(info.versionID, info.status)
			log.Info("Updated restore status to versionCache: versionID=%s, status=%d", info.versionID, info.status)
		}
	}

	if len(completedRestores) > 0 {
		bm.mu.Lock()
		for _, info := range completedRestores {
			if tasks, exists := bm.tasks[info.spaceKey]; exists {
				allRestore := true
				for _, task := range tasks {
					if task.TaskType != BackupTaskTypeRestore {
						allRestore = false
						break
					}
				}
				if allRestore {
					bm.restoreVersionMapping[info.spaceKey] = info.versionID
					delete(bm.tasks, info.spaceKey)
					log.Info("Cleaned up all restore tasks for spaceKey %s, saved mapping to versionID %s", info.spaceKey, info.versionID)
				}
			}
		}
		bm.mu.Unlock()
	}
}

func (bm *BackupMonitor) addVersionTask(ctx context.Context, version *BackupVersion, taskType string, backupRequest *entity.BackupOrRestoreRequest) error {
	partitionTasks := make([]*PartitionBackupOrRestoreTask, 0)
	mc := bm.client.Master()

	for _, partition := range version.Partitions {
		partitionRequest := *backupRequest
		if partition.S3PartitionID > 0 {
			partitionRequest.S3PartitionID = uint32(partition.S3PartitionID)
			log.Debug("Setting S3PartitionID=%d for partition %d in restore request", partition.S3PartitionID, partition.PartitionID)
		} else {
			partitionRequest.S3PartitionID = uint32(partition.PartitionID)
		}

		// Query Server by NodeID and get RpcAddr
		server, err := mc.QueryServer(ctx, partition.NodeID)
		if err != nil {
			log.Error("Failed to query server for node %d: %v", partition.NodeID, err)
			partitionTasks = append(partitionTasks, &PartitionBackupOrRestoreTask{
				PartitionID:   partition.PartitionID,
				NodeID:        partition.NodeID,
				PSNodeAddr:    "", // Empty when query fails
				TaskType:      taskType,
				Status:        BackupStatusInited,
				RetryCount:    0,
				MaxRetries:    3,
				StartTime:     time.Now(),
				CompleteTime:  time.Time{},
				BackupRequest: &partitionRequest,
			})
			continue
		}

		partitionTasks = append(partitionTasks, &PartitionBackupOrRestoreTask{
			PartitionID:   partition.PartitionID,
			NodeID:        partition.NodeID,
			PSNodeAddr:    server.RpcAddr(), // Initialize PSNodeAddr
			TaskType:      taskType,
			Status:        BackupStatusInited,
			RetryCount:    0,
			MaxRetries:    3,
			StartTime:     time.Now(),
			CompleteTime:  time.Time{},
			BackupRequest: &partitionRequest,
		})
	}
	bm.ProcessingVersion = append(bm.ProcessingVersion, version)
	spaceKey := version.SpaceKey
	bm.addTasks(spaceKey, partitionTasks)

	bm.dispatchTasks(partitionTasks)
	return nil
}

func (bm *BackupMonitor) addRestoreTasks(ctx context.Context, spaceKey string, partitions []*PartitionBackupInfo, backupRequest *entity.BackupOrRestoreRequest) error {
	partitionTasks := make([]*PartitionBackupOrRestoreTask, 0)
	mc := bm.client.Master()

	for _, partition := range partitions {
		partitionRequest := *backupRequest
		if partition.S3PartitionID > 0 {
			partitionRequest.S3PartitionID = uint32(partition.S3PartitionID)
			log.Debug("Setting S3PartitionID=%d for partition %d in restore request", partition.S3PartitionID, partition.PartitionID)
		} else {
			partitionRequest.S3PartitionID = uint32(partition.PartitionID)
		}

		server, err := mc.QueryServer(ctx, partition.NodeID)
		if err != nil {
			log.Error("Failed to query server for node %d: %v", partition.NodeID, err)
			partitionTasks = append(partitionTasks, &PartitionBackupOrRestoreTask{
				PartitionID:   partition.PartitionID,
				NodeID:        partition.NodeID,
				PSNodeAddr:    "", // Empty when query fails
				TaskType:      BackupTaskTypeRestore,
				Status:        BackupStatusInited,
				RetryCount:    0,
				MaxRetries:    3,
				StartTime:     time.Now(),
				CompleteTime:  time.Time{},
				BackupRequest: &partitionRequest,
			})
			continue
		}

		partitionTasks = append(partitionTasks, &PartitionBackupOrRestoreTask{
			PartitionID:   partition.PartitionID,
			NodeID:        partition.NodeID,
			PSNodeAddr:    server.RpcAddr(),
			TaskType:      BackupTaskTypeRestore,
			Status:        BackupStatusInited,
			RetryCount:    0,
			MaxRetries:    3,
			StartTime:     time.Now(),
			CompleteTime:  time.Time{},
			BackupRequest: &partitionRequest,
		})
	}

	bm.mu.Lock()
	for _, task := range partitionTasks {
		bm.tasks[spaceKey] = append(bm.tasks[spaceKey], task)
	}
	// Save restore task's spaceKey -> versionID mapping
	bm.restoreVersionMapping[spaceKey] = backupRequest.VersionID
	bm.mu.Unlock()
	log.Info("Added restore tasks and saved mapping: spaceKey=%s, versionID=%s, taskCount=%d", spaceKey, backupRequest.VersionID, len(partitionTasks))

	bm.dispatchTasks(partitionTasks)
	return nil
}

func (bm *BackupMonitor) addTasks(spaceKey string, tasks []*PartitionBackupOrRestoreTask) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for _, task := range tasks {
		bm.tasks[spaceKey] = append(bm.tasks[spaceKey], task)
	}
}

func (bm *BackupMonitor) dispatchTasks(tasks []*PartitionBackupOrRestoreTask) {
	wg := sync.WaitGroup{}
	wg.Add(len(tasks))
	for _, task := range tasks {
		go func(task *PartitionBackupOrRestoreTask) {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					log.Error("dispatchTasks goroutine panic recovered for partition %d (type: %s): %v", task.PartitionID, task.TaskType, r)
				}
			}()
			var err error
			if task.TaskType == BackupTaskTypeBackup {
				task.BackupRequest.Command = BackupCommandBackup
				err = bm.processBackupTask(task)
			} else {
				task.BackupRequest.Command = BackupCommandRestore
				err = bm.processRestoreTask(task)
			}
			if err != nil {
				log.Error("Task failed for partition %d (type: %s): %v", task.PartitionID, task.TaskType, err)
			}
		}(task)
	}
	wg.Wait()
	log.Info("successfully dispatch task!")
}

func (bm *BackupMonitor) processRestoreTask(task *PartitionBackupOrRestoreTask) error {
	defer func() {
		if r := recover(); r != nil {
			log.Error("processRestoreTask panic recovered for partition %d: %v", task.PartitionID, r)
			bm.markTaskFailed(task, fmt.Errorf("panic: %v", r))
		}
	}()
	bm.updateTaskStatus(task, BackupStatusRunning)

	task.StartTime = time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	mc := bm.client.Master()
	partitionId := task.PartitionID
	partition, err := mc.QueryPartition(ctx, partitionId)

	if err != nil {
		log.Error("Failed to query partition %d: %v", partitionId, err)
		bm.markTaskFailed(task, err)
		return err
	}
	if partition == nil || partition.Replicas == nil || len(partition.Replicas) == 0 {
		err := fmt.Errorf("partition %d has no replicas", partitionId)
		log.Error(err)
		bm.markTaskFailed(task, err)
		return err
	}

	if len(partition.Replicas) == 1 && partition.LeaderID == 0 {
		nodeID := partition.Replicas[0]
		server, err := mc.QueryServer(ctx, nodeID)
		if err != nil {
			log.Error(err)
			bm.markTaskFailed(task, err)
			return err
		}
		task.NodeID = nodeID
		task.PSNodeAddr = server.RpcAddr()
		log.Info("Sending restore request: partitionID=%d, nodeID=%d, addr=%s, backupID=%s",
			task.PartitionID, nodeID, server.RpcAddr(), task.BackupRequest.BackupID)
		err = client.OperateBackupOrRestore(server.RpcAddr(), task.BackupRequest, task.PartitionID)
		if err != nil {
			log.Error("Restore failed for partition %d on node %d (addr=%s): %v",
				task.PartitionID, nodeID, server.RpcAddr(), err)
			bm.markTaskFailed(task, err)
			return err
		}
	} else {
		for _, nodeID := range partition.Replicas {
			log.Debug("nodeID is [%+v], partition is [%+v], [%+v]", nodeID, partition.Id, partition.LeaderID)
			// if nodeID != partition.LeaderID {
			// 	continue
			// }
			server, err := mc.QueryServer(ctx, nodeID)
			if err != nil {
				log.Error(err)
				continue
			}
			task.NodeID = nodeID
			task.PSNodeAddr = server.RpcAddr()
			log.Info("Sending restore request: partitionID=%d, nodeID=%d, addr=%s, backupID=%s",
				task.PartitionID, nodeID, server.RpcAddr(), task.BackupRequest.BackupID)
			err = client.OperateBackupOrRestore(server.RpcAddr(), task.BackupRequest, task.PartitionID)
			if err != nil {
				log.Error("Restore failed for partition %d on node %d (addr=%s): %v",
					task.PartitionID, nodeID, server.RpcAddr(), err)
				bm.markTaskFailed(task, err)
				return err
			}
		}
	}

	log.Info("Restore task sent successfully for partition %d on node %d (addr=%s)",
		task.PartitionID, task.NodeID, task.PSNodeAddr)

	return nil
}

// processBackupTask dispatches tasks to PS nodes
func (bm *BackupMonitor) processBackupTask(task *PartitionBackupOrRestoreTask) error {
	defer func() {
		if r := recover(); r != nil {
			log.Error("processBackupTask panic recovered for partition %d: %v", task.PartitionID, r)
			bm.markTaskFailed(task, fmt.Errorf("panic: %v", r))
		}
	}()

	bm.updateTaskStatus(task, BackupStatusRunning)

	task.StartTime = time.Now()
	if !bm.isNodeHealthy(task.NodeID) {
		log.Warn("Node %d is unhealthy, waiting for partition migration", task.NodeID)
		//task.Status = BackupTaskWaitingMigration
		go bm.waitForPartitionMigration(task)
		return fmt.Errorf("node %d became unhealthy during backup execution", task.NodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	mc := bm.client.Master()
	partitionId := task.PartitionID
	partition, err := mc.QueryPartition(ctx, partitionId)

	if err != nil {
		log.Error("Failed to query partition %d: %v", partitionId, err)
		bm.markTaskFailed(task, err)
		return err
	}
	if partition == nil || partition.Replicas == nil || len(partition.Replicas) == 0 {
		err := fmt.Errorf("partition %d has no replicas", partitionId)
		log.Error(err)
		bm.markTaskFailed(task, err)
		return err
	}

	if len(partition.Replicas) == 1 && partition.LeaderID == 0 {
		nodeID := partition.Replicas[0]
		server, err := mc.QueryServer(ctx, nodeID)
		if err != nil {
			log.Error(err)
			bm.markTaskFailed(task, err)
			return err
		}
		log.Info("Sending backup request: partitionID=%d, nodeID=%d, addr=%s, backupID=%s",
			task.PartitionID, nodeID, server.RpcAddr(), task.BackupRequest.BackupID)
		err = client.OperateBackupOrRestore(server.RpcAddr(), task.BackupRequest, task.PartitionID)
		if err != nil {
			log.Error("Backup failed for partition %d on node %d (addr=%s): %v",
				task.PartitionID, nodeID, server.RpcAddr(), err)
			bm.markTaskFailed(task, err)
			return err
		}
		// Update task.NodeID to the actual node ID that received the backup request
		task.NodeID = nodeID
		task.PSNodeAddr = server.RpcAddr()
		log.Info("Updated task nodeID to %d (addr=%s) for partition %d",
			nodeID, server.RpcAddr(), task.PartitionID)
	} else {
		for _, nodeID := range partition.Replicas {
			log.Debug("nodeID is [%+v], partition is [%+v], [%+v]", nodeID, partition.Id, partition.LeaderID)
			if nodeID != partition.LeaderID {
				continue
			}
			server, err := mc.QueryServer(ctx, nodeID)
			if err != nil {
				log.Error(err)
				continue
			}
			log.Info("Sending backup request: partitionID=%d, nodeID=%d, addr=%s, backupID=%s",
				task.PartitionID, nodeID, server.RpcAddr(), task.BackupRequest.BackupID)
			err = client.OperateBackupOrRestore(server.RpcAddr(), task.BackupRequest, task.PartitionID)
			if err != nil {
				log.Error("Backup failed for partition %d on node %d (addr=%s): %v",
					task.PartitionID, nodeID, server.RpcAddr(), err)
				bm.markTaskFailed(task, err)
				return err
			}
			task.NodeID = nodeID
			task.PSNodeAddr = server.RpcAddr()
			log.Info("Updated task nodeID to %d (addr=%s) for partition %d",
				nodeID, server.RpcAddr(), task.PartitionID)
		}
	}

	log.Info("Backup task sent successfully for partition %d on node %d",
		task.PartitionID, task.NodeID)

	return nil
}

func (bm *BackupMonitor) waitForPartitionMigration(task *PartitionBackupOrRestoreTask) {
	log.Info("Waiting for partition %d to migrate from failed node %d", task.PartitionID, task.NodeID)

	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		ticker.Stop()
		if r := recover(); r != nil {
			log.Error("waitForPartitionMigration panic recovered: %v", r)
		}
	}()

	maxWaitTime := 10 * time.Minute
	startTime := time.Now()

	for {
		select {
		case <-ticker.C:
			if time.Since(startTime) > maxWaitTime {
				log.Error("Timeout waiting for partition %d migration after %v", task.PartitionID, maxWaitTime)
				bm.mu.Lock()
				task.Status = BackupStatusFailed
				errMsg := "timeout waiting for partition migration"
				task.LastError = fmt.Errorf(errMsg)
				task.LastErrorMsg = errMsg
				bm.mu.Unlock()
				return
			}

			// Query the current Leader node of the partition
			ctx := context.Background()
			newLeaderID, err := bm.getPartitionLeader(ctx, task.PartitionID)
			if err != nil {
				log.Debug("Failed to get partition %d leader: %v", task.PartitionID, err)
				continue
			}

			if newLeaderID != task.NodeID && newLeaderID != 0 {
				if bm.isNodeHealthy(newLeaderID) {
					log.Info("Partition %d has migrated to new node %d, retrying backup", task.PartitionID, newLeaderID)

					bm.mu.Lock()
					task.NodeID = newLeaderID
					task.RetryCount++
					bm.mu.Unlock()

					go bm.processBackupTask(task)
					return
				} else {
					log.Debug("New leader node %d for partition %d is not healthy yet", newLeaderID, task.PartitionID)
				}
			}

		case <-bm.stopChan:
			log.Info("Stop signal received, canceling migration wait for partition %d", task.PartitionID)
			return
		}
	}
}

// getPartitionLeader gets the Leader node ID of the partition
func (bm *BackupMonitor) getPartitionLeader(ctx context.Context, partitionID entity.PartitionID) (entity.NodeID, error) {
	// Query partition information through Master
	partition, err := bm.client.Master().QueryPartition(ctx, partitionID)
	if err != nil {
		return 0, fmt.Errorf("failed to query partition: %v", err)
	}

	if partition == nil {
		return 0, fmt.Errorf("partition not found")
	}

	return partition.LeaderID, nil
}

// isNodeHealthy checks if the node is healthy
func (bm *BackupMonitor) isNodeHealthy(nodeID entity.NodeID) bool {
	ctx := context.Background()

	failServer := bm.client.Master().QueryFailServerByNodeID(ctx, uint64(nodeID))
	if failServer != nil {
		log.Debug("Node %d is in fail server list", nodeID)
		return false
	}

	server, err := bm.client.Master().QueryServer(ctx, nodeID)
	if err != nil {
		log.Debug("Failed to query server %d: %v", nodeID, err)
		return false
	}

	if server == nil {
		log.Debug("Server %d not found", nodeID)
		return false
	}
	return bm.checkNodeHealthViaPartitionAPI(server)
}

// Verify node status via partition health check API
func (bm *BackupMonitor) checkNodeHealthViaPartitionAPI(server *entity.Server) bool {
	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.PartitionInfos(server.RpcAddr())
	if err != nil {
		return false
	}
	return true
}

// GetBackupProgress gets the backup progress for the specified space
func (bm *BackupMonitor) GetBackupProgress(spaceKey string) *entity.BackupProgressResponse {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	tasks, exists := bm.tasks[spaceKey]
	if !exists || len(tasks) == 0 {
		return &entity.BackupProgressResponse{
			TotalTasks:     0,
			CompletedTasks: 0,
			SuccessRatio:   0.0,
		}
	}

	totalTasks := len(tasks)
	completedTasks := 0

	// Only count successfully completed tasks
	for _, task := range tasks {
		if task.Status == BackupStatusCompleted {
			completedTasks++
		}
	}

	// Calculate success ratio
	var successRatio float64
	if totalTasks > 0 {
		successRatio = float64(completedTasks) / float64(totalTasks)
	}

	return &entity.BackupProgressResponse{
		TotalTasks:     totalTasks,
		CompletedTasks: completedTasks,
		SuccessRatio:   successRatio,
	}
}

// GetRestoreProgress gets the restore progress for the specified space
func (bm *BackupMonitor) GetRestoreProgress(spaceKey string) *entity.BackupProgressResponse {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	tasks, exists := bm.tasks[spaceKey]
	if !exists || len(tasks) == 0 {
		return &entity.BackupProgressResponse{
			Status:         BackupStatusStringNotFound,
			TotalTasks:     0,
			CompletedTasks: 0,
			SuccessRatio:   0.0,
		}
	}

	totalTasks := 0
	completedTasks := 0
	failedTasks := 0
	runningTasks := 0

	for _, task := range tasks {
		// Skip non-restore tasks
		if task.TaskType != BackupTaskTypeRestore {
			continue
		}

		totalTasks++
		switch task.Status {
		case BackupStatusCompleted:
			completedTasks++
		case BackupStatusFailed:
			failedTasks++
		case BackupStatusRunning:
			runningTasks++
		}
	}

	if totalTasks == 0 {
		return &entity.BackupProgressResponse{
			Status:         "not_found",
			TotalTasks:     0,
			CompletedTasks: 0,
			SuccessRatio:   0.0,
		}
	}

	var successRatio float64
	if totalTasks > 0 {
		successRatio = float64(completedTasks) / float64(totalTasks)
	}

	status := BackupStatusStringRunning
	if completedTasks == totalTasks {
		status = BackupStatusStringCompleted
	} else if failedTasks > 0 && runningTasks == 0 {
		status = BackupStatusStringFailed
	}

	return &entity.BackupProgressResponse{
		Status:         status,
		TotalTasks:     totalTasks,
		CompletedTasks: completedTasks,
		SuccessRatio:   successRatio,
	}
}

// ============================================================================
// BackupManager - backup manager on master side, main scheduler
// ============================================================================
type BackupManager struct {
	client         *client.Client
	backupMonitor  *BackupMonitor
	versionManager *VersionManager
	// Version status cache: versionId -> VersionStatusInfo
	versionCache   map[string]*VersionStatusInfo // key: versionId
	muVersionCache sync.RWMutex
	// S3 partition ID to new partition ID mapping, key is spaceKey, value is map[new partition ID]S3 partition ID
	s3PartitionMap   map[string]map[entity.PartitionID]entity.PartitionID
	muS3PartitionMap sync.RWMutex
}

// NewVersionManager creates a version manager
func NewVersionManager(client *client.Client, minioClient *minio.Client, bucketName string) *VersionManager {
	return &VersionManager{
		client:       client,
		minioClient:  minioClient,
		bucketName:   bucketName,
		versionCache: make(map[string]*VersionInfo),
		stopChan:     make(chan struct{}),
	}
}

func NewBackupManager(client *client.Client) *BackupManager {
	bm := &BackupManager{
		client:         client,
		backupMonitor:  NewBackupMonitor(client),
		versionCache:   make(map[string]*VersionStatusInfo),
		s3PartitionMap: make(map[string]map[entity.PartitionID]entity.PartitionID),
	}
	// Set BackupMonitor's update version status callback
	bm.backupMonitor.updateVersionStatus = bm.updateVersionStatus
	return bm
}

func (b *BackupManager) Start() {
	go b.backupMonitor.Start()
	if b.versionManager != nil {
		go b.versionManager.Start()
	}
}

// updateVersionStatus updates version status to versionCache
func (b *BackupManager) updateVersionStatus(versionID string, status BackupVersionStatus) {
	b.muVersionCache.Lock()
	defer b.muVersionCache.Unlock()
	if b.versionCache == nil {
		b.versionCache = make(map[string]*VersionStatusInfo)
	}
	versionInfo, ok := b.versionCache[versionID]
	if !ok {
		versionInfo = &VersionStatusInfo{
			VersionID:   versionID,
			Status:      status,
			LastUpdated: time.Now(),
		}
		b.versionCache[versionID] = versionInfo
		log.Info("Created version status in versionCache: versionID=%s, status=%d", versionID, status)
	} else {
		oldStatus := versionInfo.Status
		versionInfo.Status = status
		versionInfo.LastUpdated = time.Now()
		log.Info("Updated version status in versionCache: versionID=%s, status=%d→%d", versionID, oldStatus, status)
	}
}

// createSnapshot backup entry point called by backup manager, used to dispatch backup requests
func (b *BackupManager) createSnapshot(ctx context.Context, req *entity.BackupOrRestoreRequest) (*BackupVersion, error) {

	clusterName := config.Conf().Global.Name
	databaseName := req.Database
	spaceName := req.Space

	// Check if current spaceKey still has tasks, if exists it means the previous backup version is still being processed, return error directly
	spaceKey := buildSpaceKey(databaseName, spaceName)
	b.backupMonitor.mu.RLock()
	tasks, exists := b.backupMonitor.tasks[spaceKey]
	b.backupMonitor.mu.RUnlock()

	if exists && len(tasks) > 0 {
		return nil, fmt.Errorf("spaceKey %s still has pending tasks from previous backup, cannot create new version", spaceKey)
	}

	// Connect to S3 client
	minioClient, err := minio.New(req.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(req.S3Param.AccessKey, req.S3Param.SecretKey, ""),
		Secure: req.S3Param.UseSSL,
		Region: req.S3Param.Region,
	})
	if err != nil {
		err := fmt.Errorf("failed to create minio client: %+v", err)
		log.Error(err)
		return nil, err
	}

	if b.versionManager == nil {
		b.versionManager = &VersionManager{
			client:       b.client,
			versionCache: make(map[string]*VersionInfo),
			stopChan:     make(chan struct{}),
		}
	}

	versionID := b.versionManager.generateVersionID()
	req.VersionID = versionID

	if req.BackupID == "" || req.BackupID == "0" {
		req.BackupID = uuid.NewString()
		log.Info("Generated new BackupID (UUID): %s for space %s/%s", req.BackupID, databaseName, spaceName)
	}

	// Backup schema file
	_, err = b.backupSchema(ctx, databaseName, spaceName, req, minioClient)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	partitions, err := b.resolvePartitions(ctx, databaseName, spaceName)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	version, err := b.versionManager.CreateVersion(ctx, &CreateVersionRequest{
		ClusterName: clusterName,
		DBName:      databaseName,
		SpaceName:   spaceName,
		VersionID:   versionID,
		BackupID:    req.BackupID, // Set BackupID
		Description: BackupDescriptionBackup,
		Tags:        make(map[string]string),
		Partitions:  partitions,
	})
	if err != nil {
		log.Error(err)
		return nil, err
	}

	backupRequest := &entity.BackupOrRestoreRequest{
		Database:  databaseName,
		Space:     spaceName,
		Command:   BackupCommandBackup,
		VersionID: versionID,
		BackupID:  req.BackupID,
		S3Param:   req.S3Param,
	}

	err = b.backupMonitor.addVersionTask(ctx, version, BackupTaskTypeBackup, backupRequest)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return version, nil
}

// restoreSnapshot restore entry point called by backup manager, used to dispatch restore requests
func (b *BackupManager) restoreSnapshot(ctx context.Context, dbService *DBService, spaceService *SpaceService, configService *ConfigService, databaseName, spaceName string, req *entity.BackupOrRestoreRequest) (res *entity.BackupSpaceResponse, err error) {
	// Connect to S3 client
	minioClient, err := minio.New(req.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(req.S3Param.AccessKey, req.S3Param.SecretKey, ""),
		Secure: req.S3Param.UseSSL,
		Region: req.S3Param.Region,
	})
	if err != nil {
		err := fmt.Errorf("failed to create minio client: %+v", err)
		log.Error(err)
		return nil, err
	}

	// Verify if VersionID is provided
	versionID := req.VersionID
	if req.SourceClusterName != "" {
		if versionID == "" {
			if req.BackupID != "" && req.BackupID != "0" {
				versionID = req.BackupID
				req.VersionID = versionID
				log.Info("Using BackupID as VersionID for restore with source_cluster_name: %s", versionID)
			} else {
				return nil, fmt.Errorf("when source_cluster_name is specified, either version_id or backup_id must be provided")
			}
		}
	} else if versionID == "" {
		return nil, fmt.Errorf("VersionID is required for restore")
	}

	if req.BackupID == "" || req.BackupID == "0" {
		req.BackupID = uuid.NewString()
		log.Info("Generated new BackupID (UUID): %s for restore space %s/%s", req.BackupID, databaseName, spaceName)
	}

	_, err = b.restoreSchema(ctx, dbService, spaceService, configService, databaseName, spaceName, req, minioClient)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// Resolve partition information
	partitions, err := b.resolvePartitions(ctx, databaseName, spaceName)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// Construct spaceKey (format: dbName-spaceName)
	spaceKey := buildSpaceKey(databaseName, spaceName)

	// Construct backup request
	backupRequest := &entity.BackupOrRestoreRequest{
		Database:          databaseName,
		Space:             spaceName,
		Command:           BackupCommandRestore,
		VersionID:         versionID,
		BackupID:          req.BackupID,
		SourceClusterName: req.SourceClusterName,
		S3Param:           req.S3Param,
	}

	err = b.backupMonitor.addRestoreTasks(ctx, spaceKey, partitions, backupRequest)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &entity.BackupSpaceResponse{
		BackupID: 0,
	}, nil
}

func (b *BackupManager) resolvePartitions(ctx context.Context, databaseName,
	spaceName string) ([]*PartitionBackupInfo, error) {

	mc := b.client.Master()
	partitions := make([]*PartitionBackupInfo, 0)
	dbID, err := mc.QueryDBName2ID(ctx, databaseName)
	if err != nil {
		return nil, err
	}
	space, err := mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		log.Error("Failed to query space %s/%s: %v", databaseName, spaceName, err)
		return nil, err
	}
	if space == nil {
		return nil, fmt.Errorf("space %s/%s not found", databaseName, spaceName)
	}

	spaceKey := buildSpaceKey(databaseName, spaceName)
	b.muS3PartitionMap.RLock()
	s3PartitionMap, hasMapping := b.s3PartitionMap[spaceKey]
	b.muS3PartitionMap.RUnlock()

	for _, partition := range space.Partitions {
		nodeID := partition.LeaderID
		// If LeaderID is 0, use the first replica
		if partition.LeaderID == 0 {
			nodeID = partition.Replicas[0]
		}
		info := &PartitionBackupInfo{
			PartitionID: partition.Id,
			NodeID:      nodeID,
		}
		if hasMapping {
			if s3PartitionID, ok := s3PartitionMap[partition.Id]; ok {
				info.S3PartitionID = s3PartitionID
				log.Debug("Partition ID mapping for restore: new partition %d -> S3 partition %d", partition.Id, s3PartitionID)
			} else {
				log.Warn("No S3 partition ID mapping found for partition %d", partition.Id)
			}
		} else {
			// Backup operation: S3 partition ID is the current partition ID
			info.S3PartitionID = partition.Id
		}
		partitions = append(partitions, info)
	}
	return partitions, nil
}

func (b *BackupManager) backupSchema(ctx context.Context, dbName, spaceName string,
	req *entity.BackupOrRestoreRequest,
	minioClient *minio.Client) (res *entity.BackupSpaceResponse, err error) {
	mc := b.client.Master()
	dbID, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return nil, err
	}
	space, err := mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		return res, err
	}

	// Backup space information
	spaceJson, err := json.Marshal(space)
	if err != nil {
		log.Error("json.Marshal err: %v", err)
		return res, err
	}

	backupFileName := space.Name + ".schema"

	err = os.WriteFile(backupFileName, spaceJson, 0644)
	if err != nil {
		err := fmt.Errorf("error writing to file: %v", err)
		log.Error(err)
		return res, err
	}

	objectName := filepath.Join(config.Conf().Global.Name, "backup", dbName, space.Name, fmt.Sprintf("%s/%s.schema", req.VersionID, space.Name))

	_, err = minioClient.FPutObject(ctx, req.S3Param.BucketName, objectName, backupFileName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		err = fmt.Errorf("failed to backup space: %+v", err)
		log.Error(err)
		return res, err
	}
	log.Info("backup schema success, file is [%s]", backupFileName)
	os.Remove(backupFileName)
	return res, nil
}

func (b *BackupManager) restoreSchema(ctx context.Context,
	dbService *DBService,
	spaceService *SpaceService,
	configService *ConfigService,
	dbName, spaceName string,
	req *entity.BackupOrRestoreRequest,
	minioClient *minio.Client) (res *entity.BackupSpaceResponse, err error) {
	mc := b.client.Master()
	dbID, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		return nil, err
	}

	_, err = mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err == nil {
		err = fmt.Errorf("space duplicate: space %s already exists in database %s", spaceName, dbName)
		return res, err
	}

	backupFileName := spaceName + ".schema"
	bucketName := req.S3Param.BucketName
	sourceClusterName := req.SourceClusterName
	if sourceClusterName == "" {
		sourceClusterName = config.Conf().Global.Name
	}
	objectName := fmt.Sprintf("%s/backup/%s/%s/%s/%s.schema", sourceClusterName, dbName, spaceName, req.VersionID, spaceName)
	err = minioClient.FGetObject(ctx, bucketName, objectName, backupFileName, minio.GetObjectOptions{})
	if err != nil {
		err := fmt.Errorf("failed to download %s from S3: %+v", objectName, err)
		return res, err
	}
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			log.Error(err)
		}
	}(backupFileName)
	log.Info("downloaded backup file from S3: %s", backupFileName)

	spaceJson, err := os.ReadFile(backupFileName)
	if err != nil {
		err := fmt.Errorf("error read file:%v", err)
		log.Error(err)
		return res, err
	}

	log.Debug("%s", spaceJson)
	space := &entity.Space{}
	err = json.Unmarshal(spaceJson, space)
	if err != nil {
		err := fmt.Errorf("unmarshal file: %v", err)
		return res, err
	}

	partitionNum := len(space.Partitions)

	space.Partitions = make([]*entity.Partition, 0)

	s3Path := filepath.Join(sourceClusterName, "backup", dbName, spaceName, req.VersionID)
	objectCh := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    s3Path + "/",
		Recursive: false,
	})

	s3PartitionIDs := make([]entity.PartitionID, 0)
	partitionDirs := make(map[string]bool)
	for object := range objectCh {
		if object.Err != nil {
			log.Error("failed to list S3 objects: %v", object.Err)
			continue
		}

		relativePath := strings.TrimPrefix(object.Key, s3Path+"/")

		if relativePath == "" {
			continue
		}

		if strings.HasSuffix(relativePath, "/") {
			dirName := strings.TrimSuffix(relativePath, "/")
			if !strings.Contains(dirName, "/") && dirName != "" {
				partitionDirs[dirName] = true
				// Parse partition ID
				if partitionID, err := strconv.ParseUint(dirName, 10, 32); err == nil {
					s3PartitionIDs = append(s3PartitionIDs, entity.PartitionID(partitionID))
					log.Info("Found partition directory: %s (S3 partition ID: %d)", dirName, partitionID)
				} else {
					log.Warn("Failed to parse partition ID from directory name: %s", dirName)
				}
			}
		}
	}

	// Sort S3 partition IDs to ensure consistent order
	sort.Slice(s3PartitionIDs, func(i, j int) bool {
		return s3PartitionIDs[i] < s3PartitionIDs[j]
	})

	log.Info("Total partition directories found: %d, expected: %d", len(partitionDirs), partitionNum)
	log.Debug("Partition directories: %v, S3 partition IDs: %v", partitionDirs, s3PartitionIDs)

	if len(partitionDirs) != partitionNum {
		err = fmt.Errorf("S3 partition directory count %d does not match schema partition count %d, found directories: %v",
			len(partitionDirs), partitionNum, partitionDirs)
		return res, err
	}

	if err := spaceService.CreateSpace(ctx, dbService, dbName, space); err != nil {
		log.Error("createSpace err: %v", err)
		return res, err
	}

	// Re-query the created space to get newly allocated partition IDs
	space, err = mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		log.Error("Failed to query space after creation: %v", err)
		return res, err
	}
	if space == nil || len(space.Partitions) != partitionNum {
		err = fmt.Errorf("space partitions count mismatch after creation: expected %d, got %d", partitionNum, len(space.Partitions))
		log.Error(err)
		return res, err
	}

	// Establish mapping relationship: new partition ID -> S3 partition ID
	spaceKey := buildSpaceKey(dbName, spaceName)
	if len(s3PartitionIDs) == len(space.Partitions) {
		s3PartitionMap := make(map[entity.PartitionID]entity.PartitionID)
		for i, newPartition := range space.Partitions {
			if i < len(s3PartitionIDs) {
				s3PartitionMap[newPartition.Id] = s3PartitionIDs[i]
				log.Info("Partition ID mapping: new partition %d -> S3 partition %d", newPartition.Id, s3PartitionIDs[i])
			}
		}
		if b.s3PartitionMap == nil {
			b.s3PartitionMap = make(map[string]map[entity.PartitionID]entity.PartitionID)
		}
		b.muS3PartitionMap.Lock()
		b.s3PartitionMap[spaceKey] = s3PartitionMap
		b.muS3PartitionMap.Unlock()
		log.Info("Stored partition ID mapping for space %s: %v", spaceKey, s3PartitionMap)
	} else {
		err = fmt.Errorf("S3 partition ID count (%d) does not match new partition count (%d) for space %s/%s, cannot establish mapping. This may be caused by invalid partition directory names in S3",
			len(s3PartitionIDs), len(space.Partitions), dbName, spaceName)
		log.Error(err)
		return res, err
	}

	cfg, err := configService.GetSpaceConfigByName(ctx, dbName, spaceName)
	if err != nil {
		log.Error("get space config err: %s", err.Error())
		return res, err
	}

	err = configService.UpdateSpaceConfig(ctx, space, cfg)
	if err != nil {
		log.Error("update space config err: %s", err.Error())
		return res, err
	}
	return res, nil

}
