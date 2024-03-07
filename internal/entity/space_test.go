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

package entity_test

import (
	"encoding/json"
	"testing"

	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/pkg/assert"
)

func TestEngineErrorSpaceString(t *testing.T) {
	space := &entity.Space{}
	str := `{"name":"ts_space","partition_num":1,"replica_num":1,"engine":{"index_size":100000,"metric_type":"InnerProduct","retrieval_type":"ERROR_INDEX","retrieval_param":{"metric_type":"InnerProduct","ncentroids":128,"nsubvector":32,"nlinks":32,"efConstruction":200,"efSearch":64}},"properties":{"string":{"type":"string","array":true,"index":true},"float":{"type":"float","index":true},"int":{"type":"integer","index":true},"double":{"type":"double","index":true},"vector":{"type":"vector","dimension":128,"store_type":"MemoryOnly","format":"normalization"}}}`
	err := json.Unmarshal([]byte(str), &space)
	assert.NotNil(t, err)
}

func TestEngineSpaceString(t *testing.T) {
	space := &entity.Space{}
	str := `{"name":"ts_space","partition_num":1,"replica_num":1,"engine":{"index_size":100000,"metric_type":"InnerProduct","retrieval_type":"IVFPQ","retrieval_param":{"metric_type":"InnerProduct","ncentroids":128,"nsubvector":32,"nlinks":32,"efConstruction":200,"efSearch":64}},"properties":{"string":{"type":"string","array":true,"index":true},"float":{"type":"float","index":true},"int":{"type":"integer","index":true},"double":{"type":"double","index":true},"vector":{"type":"vector","dimension":128,"store_type":"MemoryOnly","format":"normalization"}}}`
	if err := json.Unmarshal([]byte(str), &space); err != nil {
		t.Errorf(err.Error())
	}
	assert.Equal(t, space.Engine.IndexSize, int64(100000), "unmarshal string to engine err")
}

func TestSpace_Validate(t *testing.T) {
	type fields struct {
		Id              entity.SpaceID
		Name            string
		ResourceName    string
		Version         entity.Version
		DBId            entity.DBID
		Enabled         *bool
		Partitions      []*entity.Partition
		PartitionNum    int
		ReplicaNum      uint8
		Properties      json.RawMessage
		Engine          *entity.Engine
		Models          json.RawMessage
		SpaceProperties map[string]*entity.SpaceProperties
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Valid space name",
			fields: fields{
				Name: "validSpaceName",
			},
			wantErr: false,
		},
		{
			name: "Empty space name",
			fields: fields{
				Name: "",
			},
			wantErr: true,
		},
		{
			name: "Space name starts with number",
			fields: fields{
				Name: "1invalidSpaceName",
			},
			wantErr: true,
		},
		{
			name: "Space name starts with underscore",
			fields: fields{
				Name: "_invalidSpaceName",
			},
			wantErr: true,
		},
		{
			name: "Space name with invalid characters",
			fields: fields{
				Name: "invalid!Space@Name",
			},
			wantErr: true,
		},
		// TODO: Add more test cases as needed.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			space := &entity.Space{
				Id:              tt.fields.Id,
				Name:            tt.fields.Name,
				ResourceName:    tt.fields.ResourceName,
				Version:         tt.fields.Version,
				DBId:            tt.fields.DBId,
				Enabled:         tt.fields.Enabled,
				Partitions:      tt.fields.Partitions,
				PartitionNum:    tt.fields.PartitionNum,
				ReplicaNum:      tt.fields.ReplicaNum,
				Properties:      tt.fields.Properties,
				Engine:          tt.fields.Engine,
				Models:          tt.fields.Models,
				SpaceProperties: tt.fields.SpaceProperties,
			}
			if err := space.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Space.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
