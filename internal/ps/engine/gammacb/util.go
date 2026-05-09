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

package gammacb

import (
	"fmt"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/engine/sdk/go/gamma"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine/mapping"
)

func buildFieldInfo(name string, dataType gamma.DataType, option vearchpb.FieldOption) gamma.FieldInfo {
	isIndex := int32(option) > 0
	fieldInfo := gamma.FieldInfo{Name: name, DataType: dataType, IsIndex: isIndex}
	if isIndex {
		fieldInfo.IndexType = int32(option)
	}
	return fieldInfo
}

func mapping2Table(cfg EngineConfig, m *mapping.IndexMapping) (*gamma.Table, error) {
	dim := make(map[string]int)

	var indexes []gamma.IndexInfo

	// New format: use Space.Indexes (merged indexes from top-level + field-level)
	if len(cfg.Space.Indexes) > 0 {
		indexes = make([]gamma.IndexInfo, len(cfg.Space.Indexes))
		for i, idx := range cfg.Space.Indexes {
			indexes[i] = gamma.IndexInfo{
				Name:       idx.Name,
				Type:       idx.Type,
				FieldName:  idx.FieldName,
				FieldNames: idx.FieldNames,
				Params:     string(idx.Params),
			}
		}
	} else if cfg.Space.SpaceProperties != nil {
		// Legacy format: extract indexes from SpaceProperties
		for name, prop := range cfg.Space.SpaceProperties {
			if prop.Index != nil {
				indexes = append(indexes, gamma.IndexInfo{
					Name:      prop.Index.Name,
					Type:      prop.Index.Type,
					FieldName: name,
					Params:    string(prop.Index.Params),
				})
			}
		}
	}

	var refreshInterval int32
	refreshInterval = int32(entity.DefaultRefreshInterval)
	enableIdCache := entity.DefaultEnableIdCache
	enableRealtime := entity.DefalutEnableRealtime
	if cfg.Space.RefreshInterval != nil {
		refreshInterval = *cfg.Space.RefreshInterval
	}
	if cfg.Space.EnableIdCache != nil {
		enableIdCache = *cfg.Space.EnableIdCache
	}
	if cfg.Space.EnableRealtime != nil {
		enableRealtime = *cfg.Space.EnableRealtime
	}
	table := &gamma.Table{
		Name:            cfg.Space.Name + "-" + cast.ToString(cfg.PartitionID),
		IndexType:       "",
		IndexParams:     "",
		RefreshInterval: refreshInterval,
		EnableIdCache:   enableIdCache,
		EnableRealtime:  enableRealtime,
		Indexes:         indexes,
	}
	fieldInfo := gamma.FieldInfo{Name: entity.IdField, DataType: gamma.STRING, IsIndex: false}
	table.Fields = append(table.Fields, fieldInfo)

	err := m.SortRangeField(func(key string, value *mapping.DocumentMapping) error {
		option := value.Field.Options()

		switch value.Field.FieldType() {
		case vearchpb.FieldType_STRING:
			table.Fields = append(table.Fields, buildFieldInfo(key, gamma.STRING, option))
		case vearchpb.FieldType_STRINGARRAY:
			table.Fields = append(table.Fields, buildFieldInfo(key, gamma.STRINGARRAY, option))
		case vearchpb.FieldType_FLOAT:
			table.Fields = append(table.Fields, buildFieldInfo(key, gamma.FLOAT, option))
		case vearchpb.FieldType_DOUBLE:
			table.Fields = append(table.Fields, buildFieldInfo(key, gamma.DOUBLE, option))
		case vearchpb.FieldType_DATE, vearchpb.FieldType_LONG:
			table.Fields = append(table.Fields, buildFieldInfo(key, gamma.LONG, option))
		case vearchpb.FieldType_INT:
			table.Fields = append(table.Fields, buildFieldInfo(key, gamma.INT, option))
		case vearchpb.FieldType_VECTOR:
			fieldMapping := value.Field.FieldMappingI.(*mapping.VectortFieldMapping)
			dim[key] = fieldMapping.Dimension
			vectorInfo := gamma.VectorInfo{
				Name:       key,
				DataType:   gamma.FLOAT,
				Dimension:  int32(fieldMapping.Dimension),
				StoreType:  fieldMapping.StoreType,
				StoreParam: string(fieldMapping.StoreParam),
			}
			vectorInfo.IsIndex = true
			table.VectorsInfos = append(table.VectorsInfos, vectorInfo)
		default:
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("space invalid field type: %s", value.Field.FieldType().String()))
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	m.DimensionMap = dim

	if len(table.VectorsInfos) == 0 {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("create table has no vector field"))
	}

	return table, nil
}
