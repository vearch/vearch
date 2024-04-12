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
	"github.com/vearch/vearch/internal/engine/sdk/go/gamma"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/ps/engine/mapping"
)

func mapping2Table(cfg EngineConfig, m *mapping.IndexMapping) (*gamma.Table, error) {
	dim := make(map[string]int)

	index := cfg.Space.Index
	indexParams := ""
	if index.Params != nil {
		indexParams = string(index.Params)
	}

	table := &gamma.Table{
		Name:        cfg.Space.Name + "-" + cast.ToString(cfg.PartitionID),
		IndexType:   index.Type,
		IndexParams: indexParams,
	}

	fieldInfo := gamma.FieldInfo{Name: mapping.IdField, DataType: gamma.STRING, IsIndex: false}
	table.Fields = append(table.Fields, fieldInfo)

	err := m.SortRangeField(func(key string, value *mapping.DocumentMapping) error {
		switch value.Field.FieldType() {
		case vearchpb.FieldType_STRING:
			value.Field.Options()
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.STRING}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_FLOAT:
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.FLOAT}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_DOUBLE:
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.DOUBLE}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_DATE, vearchpb.FieldType_LONG:
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.LONG}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
		case vearchpb.FieldType_INT:
			index := (value.Field.Options() & vearchpb.FieldOption_Index) / vearchpb.FieldOption_Index
			fieldInfo := gamma.FieldInfo{Name: key, DataType: gamma.INT}
			if index == 1 {
				fieldInfo.IsIndex = true
			} else {
				fieldInfo.IsIndex = false
			}
			table.Fields = append(table.Fields, fieldInfo)
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
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	m.DimensionMap = dim

	if len(table.VectorsInfos) == 0 {
		return nil, fmt.Errorf("create table has no vector field")
	}

	return table, nil

}
