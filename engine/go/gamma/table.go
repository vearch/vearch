/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

import (
	"../../idl/fbs-gen/go/gamma_api"
	flatbuffers "github.com/google/flatbuffers/go"
)

type DataType int8

const (
	INT    DataType = 0
	LONG   DataType = 1
	FLOAT  DataType = 2
	DOUBLE DataType = 3
	STRING DataType = 4
	VECTOR DataType = 5
)

type VectorInfo struct {
	Name       string
	DataType   DataType
	IsIndex    bool
	Dimension  int32
	ModelId    string
	StoreType  string
	StoreParam string
	HasSource  bool
}

type FieldInfo struct {
	Name     string
	DataType DataType
	IsIndex  bool
}

type Table struct {
	Name           string
	Fields         []FieldInfo
	VectorsInfos   []VectorInfo
	IndexingSize   int32
	CompressMode   int32
	RetrievalType  string
	RetrievalParam string
	table          *gamma_api.Table
}

func (table *Table) Serialize(out *[]byte) int {
	builder := flatbuffers.NewBuilder(0)
	name := builder.CreateString(table.Name)

	var fieldNames []flatbuffers.UOffsetT
	fieldNames = make([]flatbuffers.UOffsetT, len(table.Fields))
	for i := 0; i < len(table.Fields); i++ {
		field := table.Fields[i]
		fieldNames[i] = builder.CreateString(field.Name)
	}

	var fieldInfos []flatbuffers.UOffsetT
	fieldInfos = make([]flatbuffers.UOffsetT, len(table.Fields))
	for i := 0; i < len(table.Fields); i++ {
		field := table.Fields[i]
		gamma_api.FieldInfoStart(builder)
		gamma_api.FieldInfoAddName(builder, fieldNames[i])
		gamma_api.FieldInfoAddDataType(builder, int8(field.DataType))
		gamma_api.FieldInfoAddIsIndex(builder, field.IsIndex)
		fieldInfos[i] = gamma_api.FieldInfoEnd(builder)
	}

	gamma_api.TableStartFieldsVector(builder, len(table.Fields))
	for i := 0; i < len(table.Fields); i++ {
		builder.PrependUOffsetT(fieldInfos[i])
	}
	fields := builder.EndVector(len(table.Fields))

	var names, modelIDs, storeTypes, storeParams []flatbuffers.UOffsetT
	names = make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	modelIDs = make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	storeTypes = make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	storeParams = make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	for i := 0; i < len(table.VectorsInfos); i++ {
		vecInfo := table.VectorsInfos[i]
		names[i] = builder.CreateString(vecInfo.Name)
		modelIDs[i] = builder.CreateString(vecInfo.ModelId)
		storeTypes[i] = builder.CreateString(vecInfo.StoreType)
		storeParams[i] = builder.CreateString(vecInfo.StoreParam)
	}

	var vectorInfos []flatbuffers.UOffsetT
	vectorInfos = make([]flatbuffers.UOffsetT, len(table.VectorsInfos))
	for i := 0; i < len(table.VectorsInfos); i++ {
		vecInfo := table.VectorsInfos[i]
		gamma_api.VectorInfoStart(builder)
		gamma_api.VectorInfoAddName(builder, names[i])
		gamma_api.VectorInfoAddDataType(builder, int8(vecInfo.DataType))
		gamma_api.VectorInfoAddIsIndex(builder, vecInfo.IsIndex)
		gamma_api.VectorInfoAddDimension(builder, vecInfo.Dimension)
		gamma_api.VectorInfoAddModelId(builder, modelIDs[i])
		gamma_api.VectorInfoAddStoreType(builder, storeTypes[i])
		gamma_api.VectorInfoAddStoreParam(builder, storeParams[i])
		gamma_api.VectorInfoAddHasSource(builder, vecInfo.HasSource)
		vectorInfos[i] = gamma_api.VectorInfoEnd(builder)
	}

	gamma_api.TableStartVectorsInfoVector(builder, len(table.VectorsInfos))
	for i := 0; i < len(table.VectorsInfos); i++ {
		builder.PrependUOffsetT(vectorInfos[i])
	}
	vecInfos := builder.EndVector(len(table.VectorsInfos))

	retrievalType := builder.CreateString(table.RetrievalType)
	retrievalParam := builder.CreateString(table.RetrievalParam)
	gamma_api.TableStart(builder)
	gamma_api.TableAddName(builder, name)
	gamma_api.TableAddFields(builder, fields)
	gamma_api.TableAddVectorsInfo(builder, vecInfos)
	gamma_api.TableAddIndexingSize(builder, table.IndexingSize)
	gamma_api.TableAddCompressMode(builder, table.CompressMode)
	gamma_api.TableAddRetrievalType(builder, retrievalType)
	gamma_api.TableAddRetrievalParam(builder, retrievalParam)
	builder.Finish(builder.EndObject())
	outLen := len(builder.FinishedBytes())
	*out = make([]byte, outLen)
	copy(*out, builder.FinishedBytes())
	return outLen
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
		table.Fields[i].IsIndex = fieldInfo.IsIndex()
	}

	table.VectorsInfos = make([]VectorInfo, table.table.VectorsInfoLength())
	for i := 0; i < len(table.VectorsInfos); i++ {
		var vecInfo gamma_api.VectorInfo
		table.table.VectorsInfo(&vecInfo, i)
		table.VectorsInfos[i].Name = string(vecInfo.Name())
		table.VectorsInfos[i].DataType = DataType(vecInfo.DataType())
		table.VectorsInfos[i].IsIndex = vecInfo.IsIndex()
		table.VectorsInfos[i].Dimension = vecInfo.Dimension()
		table.VectorsInfos[i].ModelId = string(vecInfo.ModelId())
		table.VectorsInfos[i].StoreType = string(vecInfo.StoreType())
		table.VectorsInfos[i].StoreParam = string(vecInfo.StoreParam())
		table.VectorsInfos[i].HasSource = vecInfo.HasSource()
	}

	table.IndexingSize = table.table.IndexingSize()
	table.CompressMode = table.table.CompressMode()
	table.RetrievalType = string(table.table.RetrievalType())
	table.RetrievalParam = string(table.table.RetrievalParam())
}
