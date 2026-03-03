package services

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
)

type ExportService struct {
	client *client.Client
}

func NewExportService(client *client.Client) *ExportService {
	return &ExportService{
		client: client,
	}
}

// ExportSpace exports space data to S3 in JSON format
func (s *ExportService) ExportSpace(ctx context.Context, dbName, spaceName string, req *entity.ExportSpaceRequest) (res *entity.ExportSpaceResponse, err error) {
	// bucket/cluster/export/db/space/json_file

	res = &entity.ExportSpaceResponse{}

	res, err = s.exportSchema(ctx, dbName, spaceName, req)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	mc := s.client.Master()
	dbID, err := mc.QueryDBName2ID(ctx, dbName)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	space, err := mc.QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	if space == nil || space.Partitions == nil {
		return nil, fmt.Errorf("space %s/%s not found or has no partitions", dbName, spaceName)
	}

	// Calculate partition mapping for S3 path
	s3PartitionMap := make(map[entity.PartitionID]entity.PartitionID)
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

	// Send export request to each partition
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
			log.Debug("export invoke nodeID [%+v], address [%+v]", partition.Id, server.RpcAddr())
			req.Part = s3PartitionMap[partition.Id]
			err = client.ExportSpace(server.RpcAddr(), req, partition.Id)
			if err != nil {
				log.Error(err)
				continue
			}
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
				log.Debug("export invoke nodeID [%v], partition [%v] address [%+v]", nodeID, partition.Id, server.RpcAddr())
				req.Part = s3PartitionMap[partition.Id]
				err = client.ExportSpace(server.RpcAddr(), req, partition.Id)
				if err != nil {
					log.Error(err)
					continue
				}
			}
		}
	}
	return res, nil
}

// exportSchema exports space schema to S3
func (s *ExportService) exportSchema(ctx context.Context, dbName, spaceName string, req *entity.ExportSpaceRequest) (res *entity.ExportSpaceResponse, err error) {
	res = &entity.ExportSpaceResponse{}
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

	exportFileName := space.Name + ".schema"
	err = os.WriteFile(exportFileName, spaceJson, 0644)
	if err != nil {
		err := fmt.Errorf("error writing to file: %v", err)
		log.Error(err)
		return res, err
	}

	minioClient, err := minio.New(req.S3Param.EndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(req.S3Param.AccessKey, req.S3Param.SecretKey, ""),
		Secure: req.S3Param.UseSSL,
		Region: req.S3Param.Region,
	})
	if err != nil {
		err = fmt.Errorf("failed to create minio client: %+v", err)
		log.Error(err)
		return res, err
	}

	// Generate export ID using timestamp
	res.ExportID = int(time.Now().Unix())
	req.ExportID = res.ExportID

	objectName := filepath.Join(config.Conf().Global.Name, "export", dbName, space.Name, fmt.Sprintf("%d/%s.schema", res.ExportID, space.Name))
	_, err = minioClient.FPutObject(ctx, req.S3Param.BucketName, objectName, exportFileName, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		err = fmt.Errorf("failed to export schema: %+v", err)
		log.Error(err)
		return res, err
	}
	log.Info("export schema success, file is [%s]", exportFileName)
	os.Remove(exportFileName)
	return res, nil
}
