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
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
)

type BackupService struct {
	client *client.Client
}

func NewBackupService(client *client.Client) *BackupService {
	return &BackupService{client: client}
}

func (s *BackupService) BackupSpace(ctx context.Context, dbService *DBService, spaceService *SpaceService, configService *ConfigService, dbName, spaceName string, backup *entity.BackupSpace) (err error) {
	clusterName := config.Conf().Global.Name

	mc := s.client.Master()
	if backup.Command == "create" {
		dbID, err := mc.QueryDBName2Id(ctx, dbName)
		if err != nil {
			return err
		}

		space, err := mc.QuerySpaceByName(ctx, dbID, spaceName)
		if err != nil {
			return err
		}

		spaceJson, err := json.Marshal(space)
		if err != nil {
			log.Error("json.Marshal err: %v", err)
		}

		backupFileName := space.Name + ".schema"

		err = os.WriteFile(backupFileName, spaceJson, 0644)
		if err != nil {
			err := fmt.Errorf("error writing to file: %v", err)
			log.Error(err)
			return err
		}

		minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
			Secure: backup.S3Param.UseSSL,
		})
		if err != nil {
			err = fmt.Errorf("failed to create minio client: %+v", err)
			log.Error(err)
			return err
		}
		bucketName := backup.S3Param.BucketName
		objectName := fmt.Sprintf("%s/%s/%s/%s.schema", clusterName, dbName, space.Name, space.Name)
		_, err = minioClient.FPutObject(context.Background(), bucketName, objectName, backupFileName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			err = fmt.Errorf("failed to backup space: %+v", err)
			log.Error(err)
			return err
		}
		log.Info("backup schema success, file is [%s]", backupFileName)
		os.Remove(backupFileName)
	} else if backup.Command == "restore" {
		dbID, err := mc.QueryDBName2Id(ctx, dbName)
		if err != nil {
			return err
		}

		_, err = mc.QuerySpaceByName(ctx, dbID, spaceName)
		if err == nil {
			err = fmt.Errorf("space duplicate")
			return err
		}
		minioClient, err := minio.New(backup.S3Param.EndPoint, &minio.Options{
			Creds:  credentials.NewStaticV4(backup.S3Param.AccessKey, backup.S3Param.SecretKey, ""),
			Secure: backup.S3Param.UseSSL,
		})
		if err != nil {
			err := fmt.Errorf("failed to create minio client: %+v", err)
			log.Error(err)
			return err
		}

		backupFileName := spaceName + ".schema"
		bucketName := backup.S3Param.BucketName
		objectName := fmt.Sprintf("%s/%s/%s/%s.schema", clusterName, dbName, spaceName, spaceName)
		err = minioClient.FGetObject(ctx, bucketName, objectName, backupFileName, minio.GetObjectOptions{})
		if err != nil {
			err := fmt.Errorf("failed to download file from S3: %+v", err)
			log.Error(err)
			return err
		}
		defer os.Remove(backupFileName)
		log.Info("downloaded backup file from S3: %s", backupFileName)

		spaceJson, err := os.ReadFile(backupFileName)
		if err != nil {
			err := fmt.Errorf("error read file:%v", err)
			log.Error(err)
			return err
		}

		log.Debug("%s", spaceJson)
		space := &entity.Space{}
		err = json.Unmarshal(spaceJson, space)
		if err != nil {
			err := fmt.Errorf("unmarshal file: %v", err)
			log.Error(err)
			return err
		}

		partitionNum := len(space.Partitions)
		space.Partitions = make([]*entity.Partition, 0)

		objectCh := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
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
			err = fmt.Errorf("oss partition num %d not equal schema %d", len(patitionMap), partitionNum)
			return err
		}

		if err := spaceService.CreateSpace(ctx, dbService, dbName, space); err != nil {
			log.Error("createSpace err: %v", err)
			return err
		}

		cfg, err := configService.GetEngineCfg(ctx, dbName, spaceName)
		if err != nil {
			log.Error("get engine config err: %s", err.Error())
			return err
		}

		err = configService.UpdateEngineConfig(ctx, space, cfg)
		if err != nil {
			log.Error("update engine config err: %s", err.Error())
			return err
		}
	}
	// get space info
	dbId, err := mc.QueryDBName2Id(ctx, dbName)
	if err != nil {
		errutil.ThrowError(err)
	}

	space, err := mc.QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		errutil.ThrowError(err)
	}
	if space == nil || space.Partitions == nil {
		return nil
	}
	// invoke all space nodeID
	part := 0
	for _, p := range space.Partitions {
		partition, err := mc.QueryPartition(ctx, p.Id)
		if err != nil {
			log.Error(err)
			continue
		}
		if partition.Replicas != nil {
			for _, nodeID := range partition.Replicas {
				log.Debug("nodeID is [%+v],partition is [%+v], [%+v]", nodeID, partition.Id, partition.LeaderID)
				if nodeID != partition.LeaderID {
					continue
				}
				server, err := mc.QueryServer(ctx, nodeID)
				errutil.ThrowError(err)
				log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
				backup.Part = part
				err = client.BackupSpace(server.RpcAddr(), backup, partition.Id)
				errutil.ThrowError(err)
				part++
			}
			if len(partition.Replicas) == 1 && partition.LeaderID == 0 {
				server, err := mc.QueryServer(ctx, partition.Replicas[0])
				errutil.ThrowError(err)
				log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
				backup.Part = part
				err = client.BackupSpace(server.RpcAddr(), backup, partition.Id)
				errutil.ThrowError(err)
				part++
			}
		}
	}
	return nil
}
