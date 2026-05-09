/**
 * Copyright 2019 The Vearch Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vearch/vearch/v3/internal/engine/idl/fbs-gen/go/gamma_api"
)

type DataType int8

const (
	INT         DataType = 0
	LONG        DataType = 1
	FLOAT       DataType = 2
	DOUBLE      DataType = 3
	STRING      DataType = 4
	VECTOR      DataType = 5
	BOOL        DataType = 6
	DATE        DataType = 7
	STRINGARRAY DataType = 8
)

type VectorInfo struct {
	Name       string
	DataType   DataType
	IsIndex    bool
	Dimension  int32
	StoreType  string
	StoreParam string
}

type FieldInfo struct {
	Name      string
	DataType  DataType
	IsIndex   bool
	IndexType int32
}

type IndexInfo struct {
	Name       string
	Type       string
	FieldName  string
	FieldNames []string
	Params     string
}

type Table struct {
	Name            string
	Fields          []FieldInfo
	VectorsInfos    []VectorInfo
	IndexType       string
	IndexParams     string
	RefreshInterval int32
	EnableIdCache   bool
	EnableRealtime  bool
	Indexes         []IndexInfo
	table           *gamma_api.Table
}

func (table *Table) Serialize() []byte {
	builder := flatbuffers.NewBuilder(0)
	name := builder.CreateString(table.Name)

	fieldNames := make([]flatbuffers.UOffsetT, len(table.Fields))
	for i := 0; i < len(table.Fields); i++ {
		field := table.Fields[i]
		fieldNames[i] = builder.CreateString(field.Name)
	}
	fieldInfos := make([]flatbuffers.UOffsetT, len(table.Fields))
	for i := 0; i < len(table.Fields); i++ {
		field := table.Fields[i]
		indexType := field.IndexType
		if indexType == 0 && field.IsIndex {
			indexType = 1
		}
		gamma_api.FieldInfoStart(builder)
		gamma_api.FieldInfoAddName(builder, fieldNames[i])
		gamma_api.FieldInfoAddDataType(builder, int8(field.DataType))
		gamma_api.FieldInfoAddIsIndex(builder, indexType != 0)
		gamma_api.FieldInfoAddIndexType(builder, indexType)
		fieldInfos[i] = gamma_api.FieldInfoEnd(builder)
	}

	gamma_api.TableStartFieldsVector(builder, len(table.Fields))
	for i := len(table.Fields) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(fieldInfos[i])
	}
	fields := builder.EndVector(len(table.Fields))

	var names, storeTypes, storeParams []flatbuffers.UOffsetT
	names = make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	storeTypes = make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	storeParams = make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	for i := 0; i < len(table.VectorsInfos); i++ {
		vecInfo := table.VectorsInfos[i]
		names[i] = builder.CreateString(vecInfo.Name)
		storeTypes[i] = builder.CreateString(vecInfo.StoreType)
		storeParams[i] = builder.CreateString(vecInfo.StoreParam)
	}

	vectorInfos := make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	for i := 0; i < len(table.VectorsInfos); i++ {
		vecInfo := table.VectorsInfos[i]
		gamma_api.VectorInfoStart(builder)
		gamma_api.VectorInfoAddName(builder, names[i])
		gamma_api.VectorInfoAddDataType(builder, int8(vecInfo.DataType))
		gamma_api.VectorInfoAddIsIndex(builder, vecInfo.IsIndex)
		gamma_api.VectorInfoAddDimension(builder, vecInfo.Dimension)
		gamma_api.VectorInfoAddStoreType(builder, storeTypes[i])
		gamma_api.VectorInfoAddStoreParam(builder, storeParams[i])
		vectorInfos[i] = gamma_api.VectorInfoEnd(builder)
	}

	gamma_api.TableStartVectorsInfoVector(builder, len(table.VectorsInfos))
	for i := len(table.VectorsInfos) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(vectorInfos[i])
	}
	vecInfos := builder.EndVector(len(table.VectorsInfos))

	var indexesOffsets []flatbuffers.UOffsetT
	for _, idx := range table.Indexes {
		name := builder.CreateString(idx.Name)
		idxType := builder.CreateString(idx.Type)
		fieldName := builder.CreateString(idx.FieldName)
		params := builder.CreateString(idx.Params)

		fieldNameOffsets := make([]flatbuffers.UOffsetT, len(idx.FieldNames))
		for j := 0; j < len(idx.FieldNames); j++ {
			fieldNameOffsets[j] = builder.CreateString(idx.FieldNames[j])
		}
		gamma_api.IndexInfoStartFieldNamesVector(builder, len(idx.FieldNames))
		for j := len(idx.FieldNames) - 1; j >= 0; j-- {
			builder.PrependUOffsetT(fieldNameOffsets[j])
		}
		fieldNamesVec := builder.EndVector(len(idx.FieldNames))

		gamma_api.IndexInfoStart(builder)
		gamma_api.IndexInfoAddName(builder, name)
		gamma_api.IndexInfoAddType(builder, idxType)
		gamma_api.IndexInfoAddFieldName(builder, fieldName)
		gamma_api.IndexInfoAddFieldNames(builder, fieldNamesVec)
		gamma_api.IndexInfoAddParams(builder, params)
		indexesOffsets = append(indexesOffsets, gamma_api.IndexInfoEnd(builder))
	}

	var indexesVec flatbuffers.UOffsetT
	if len(indexesOffsets) > 0 {
		gamma_api.TableStartIndexesVector(builder, len(indexesOffsets))
		for i := len(indexesOffsets) - 1; i >= 0; i-- {
			builder.PrependUOffsetT(indexesOffsets[i])
		}
		indexesVec = builder.EndVector(len(indexesOffsets))
	}

	indexType := builder.CreateString(table.IndexType)
	indexParams := builder.CreateString(table.IndexParams)

	gamma_api.TableStart(builder)
	gamma_api.TableAddName(builder, name)
	gamma_api.TableAddFields(builder, fields)
	gamma_api.TableAddVectorsInfo(builder, vecInfos)
	gamma_api.TableAddIndexType(builder, indexType)
	gamma_api.TableAddIndexParams(builder, indexParams)
	gamma_api.TableAddRefreshInterval(builder, table.RefreshInterval)
	gamma_api.TableAddEnableIdCache(builder, table.EnableIdCache)
	gamma_api.TableAddEnableRealtime(builder, table.EnableRealtime)
	if len(indexesOffsets) > 0 {
		gamma_api.TableAddIndexes(builder, indexesVec)
	}
	builder.Finish(builder.EndObject())
	return builder.FinishedBytes()
}

func (table *Table) DeSerialize(buffer []byte) {
	table.table = gamma_api.GetRootAsTable(buffer, 0)
	table.Name = string(table.table.Name())
	table.Fields = make([]FieldInfo, table.table.FieldsLength())
	for i := 0; i < len(table.Fields); i++ {
		var fieldInfo gamma_api.FieldInfo
		table.table.Fields(&fieldInfo, i)
		table.Fields[i].Name = string(fieldInfo.Name())
		table.Fields[i].DataType = DataType(fieldInfo.DataType())
		if fieldInfo.IndexType() != 0 {
			table.Fields[i].IndexType = fieldInfo.IndexType()
			table.Fields[i].IsIndex = table.Fields[i].IndexType != 0
		} else {
			table.Fields[i].IsIndex = fieldInfo.IsIndex()
			if table.Fields[i].IsIndex {
				table.Fields[i].IndexType = 1
			}
		}
	}

	table.VectorsInfos = make([]VectorInfo, table.table.VectorsInfoLength())
	for i := 0; i < len(table.VectorsInfos); i++ {
		var vecInfo gamma_api.VectorInfo
		table.table.VectorsInfo(&vecInfo, i)
		table.VectorsInfos[i].Name = string(vecInfo.Name())
		table.VectorsInfos[i].DataType = DataType(vecInfo.DataType())
		table.VectorsInfos[i].IsIndex = vecInfo.IsIndex()
		table.VectorsInfos[i].Dimension = vecInfo.Dimension()
		table.VectorsInfos[i].StoreType = string(vecInfo.StoreType())
		table.VectorsInfos[i].StoreParam = string(vecInfo.StoreParam())
	}

	table.IndexType = string(table.table.IndexType())
	table.IndexParams = string(table.table.IndexParams())
	table.RefreshInterval = table.table.RefreshInterval()
	table.EnableIdCache = table.table.EnableIdCache()
	table.EnableRealtime = table.table.EnableRealtime()

	table.Indexes = make([]IndexInfo, table.table.IndexesLength())
	for i := 0; i < len(table.Indexes); i++ {
		var idx gamma_api.IndexInfo
		table.table.Indexes(&idx, i)
		table.Indexes[i].Name = string(idx.Name())
		table.Indexes[i].Type = string(idx.Type())
		table.Indexes[i].FieldName = string(idx.FieldName())
		table.Indexes[i].FieldNames = make([]string, idx.FieldNamesLength())
		for j := 0; j < len(table.Indexes[i].FieldNames); j++ {
			table.Indexes[i].FieldNames[j] = string(idx.FieldNames(j))
		}
	}
}
