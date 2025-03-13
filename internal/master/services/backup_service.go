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
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
)

type BackupService struct {
	client *client.Client
}

func NewBackupService(client *client.Client) *BackupService {
	return &BackupService{client: client}
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
	})
	if err != nil {
		err = fmt.Errorf("failed to create minio client: %+v", err)
		log.Error(err)
		return res, err
	}
	bucketName := backup.S3Param.BucketName

	var objectName string
	if backup.Command == "create" {
		objectName = filepath.Join(config.Conf().Global.Name, "backup", dbName, space.Name, fmt.Sprintf("%d/%s.schema", backup.BackupID, space.Name))
	} else if backup.Command == "export" {
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
		err = fmt.Errorf("space duplicate")
		return res, err
	}
	minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
		Secure: backup.S3Param.UseSSL,
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

	patitionMap := make(map[string]string, 0)
	for object := range objectCh {
		if object.Err != nil {
			fmt.Println(object.Err)
			continue
		}
		if strings.HasSuffix(object.Key, "/") {
			patitionMap[object.Key] = object.Key
		}
	}

	if len(patitionMap) != partitionNum {
		err = fmt.Errorf("oss partition num %d not equal schema %d", len(patitionMap), partitionNum)
		return res, err
	}

	if err := spaceService.CreateSpace(ctx, dbService, dbName, space); err != nil {
		log.Error("createSpace err: %v", err)
		return res, err
	}

	cfg, err := configService.GetEngineCfg(ctx, dbName, spaceName)
	if err != nil {
		log.Error("get engine config err: %s", err.Error())
		return res, err
	}

	err = configService.UpdateEngineConfig(ctx, space, cfg)
	if err != nil {
		log.Error("update engine config err: %s", err.Error())
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
	})
	if err != nil {
		err := fmt.Errorf("failed to create minio client: %+v", err)
		log.Error(err)
		return nil, err
	}

	if req.Command == "list" {
		res.BackupIDs = list(ctx, req, minioClient, dbName, spaceName)
		return res, nil
	}

	if req.Command == "create" {
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
	if req.Command == "create" || req.Command == "export" {
		res, err = s.backupSchema(ctx, dbName, spaceName, req)
		if err != nil {
			log.Error(err)
			return nil, err
		}
	} else if req.Command == "restore" {
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
	if req.Command == "restore" {
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
	} else if req.Command == "create" || req.Command == "export" {
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

		for _, nodeID := range partition.Replicas {
			log.Debug("nodeID is [%+v], partition is [%+v], [%+v]", nodeID, partition.Id, partition.LeaderID)
			if nodeID != partition.LeaderID && req.Command != "restore" {
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
		}
	}
	return res, nil
}
