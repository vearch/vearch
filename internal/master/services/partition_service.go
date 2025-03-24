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
	"strings"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
)

type PartitionService struct {
	client *client.Client
}

func NewPartitionService(client *client.Client) *PartitionService {
	return &PartitionService{client: client}
}

func (s *PartitionService) PartitionInfo(ctx context.Context, db *DBService, spaceService *SpaceService, dbName string, spaceName string, detail string) ([]map[string]any, error) {
	dbNames := make([]string, 0)
	if dbName != "" {
		dbNames = strings.Split(dbName, ",")
	}

	if len(dbNames) == 0 {
		dbs, err := db.QueryDBs(ctx)
		if err != nil {
			return nil, err
		}
		dbNames = make([]string, len(dbs))
		for i, db := range dbs {
			dbNames[i] = db.Name
		}
	}

	color := []string{"green", "yellow", "red"}

	spaceNames := make([]string, 0)
	if spaceName != "" {
		spaceNames = strings.Split(spaceName, ",")
	}

	detail_info := false
	if detail == "true" {
		detail_info = true
	}

	mc := s.client.Master()
	resultInsideDbs := make([]map[string]any, 0)
	for i := range dbNames {
		dbName := dbNames[i]
		var errors []string

		resultDb := make(map[string]any)
		resultDb["db_name"] = dbName

		dbId, err := mc.QueryDBName2ID(ctx, dbName)
		if err != nil {
			errors = append(errors, "db: "+dbName+" find dbID err: "+err.Error())
			resultDb["errors"] = errors
			resultInsideDbs = append(resultInsideDbs, resultDb)
			continue
		}

		spaces, err := mc.QuerySpaces(ctx, dbId)
		if err != nil {
			errors = append(errors, "db: "+dbName+" find spaces err: "+err.Error())
			resultDb["errors"] = errors
			resultInsideDbs = append(resultInsideDbs, resultDb)
			continue
		}

		dbStatus := 0

		resultInsideSpaces := make([]*entity.SpaceInfo, 0, len(spaces))
		if len(spaceNames) == 0 {
			for _, space := range spaces {
				spaceName := space.Name

				spaceInfo := &entity.SpaceInfo{Name: spaceName, DbName: dbName, ReplicaNum: space.ReplicaNum, PartitionNum: space.PartitionNum}
				spaceStatus, err := spaceService.DescribeSpace(ctx, space, spaceInfo, detail_info)
				if err != nil {
					log.Error(err.Error())
					errors = append(errors, "db: "+dbName+" space: "+spaceName+" describe err: "+err.Error())
					continue
				}
				resultInsideSpaces = append(resultInsideSpaces, spaceInfo)

				if spaceStatus > dbStatus {
					dbStatus = spaceStatus
				}
			}
		} else {
			for _, spaceName := range spaceNames {
				index := -1
				for i, space := range spaces {
					if space.Name == spaceName {
						index = i
						break
					}
				}

				if index < 0 {
					msg := fmt.Sprintf("db: %s space: %s not found", dbName, spaceName)
					errors = append(errors, msg)
					continue
				}

				spaceInfo := &entity.SpaceInfo{Name: spaceName, DbName: dbName, ReplicaNum: spaces[index].ReplicaNum, PartitionNum: spaces[index].PartitionNum}
				spaceStatus, err := spaceService.DescribeSpace(ctx, spaces[index], spaceInfo, detail_info)
				if err != nil {
					log.Error(err.Error())
					errors = append(errors, "db: "+dbName+" space: "+spaceName+" describe err: "+err.Error())
					continue
				}
				resultInsideSpaces = append(resultInsideSpaces, spaceInfo)

				if spaceStatus > dbStatus {
					dbStatus = spaceStatus
				}
			}
		}

		docNum := uint64(0)
		// TODO: get size
		size := int64(0)
		for _, s := range resultInsideSpaces {
			docNum += cast.ToUint64(s.DocNum)
		}
		resultDb["space_num"] = len(spaces)
		resultDb["doc_num"] = docNum
		resultDb["size"] = size
		resultDb["spaces"] = resultInsideSpaces
		resultDb["status"] = color[dbStatus]
		resultDb["errors"] = errors
		resultInsideDbs = append(resultInsideDbs, resultDb)
	}

	return resultInsideDbs, nil
}

func (s *PartitionService) RegisterPartition(ctx context.Context, partition *entity.Partition) error {
	log.Info("register partition:[%d] ", partition.Id)
	marshal, err := json.Marshal(partition)
	if err != nil {
		return err
	}
	mc := s.client.Master()
	return mc.Put(ctx, entity.PartitionKey(partition.Id), marshal)
}
