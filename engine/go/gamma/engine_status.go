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

type EngineStatus struct {
	IndexStatus   int32
	TableMem      int64
	VectorMem     int64
	FieldRangeMem int64
	BitmapMem     int64
	DocNum        int32
	MaxDocID      int32

	engineStatus *gamma_api.EngineStatus
}

func (status *EngineStatus) Serialize(buffer *[]byte) int {
	builder := flatbuffers.NewBuilder(0)

	gamma_api.EngineStatusStart(builder)
	gamma_api.EngineStatusAddIndexStatus(builder, status.IndexStatus)
	gamma_api.EngineStatusAddTableMem(builder, status.TableMem)
	gamma_api.EngineStatusAddVectorMem(builder, status.VectorMem)
	gamma_api.EngineStatusAddFieldRangeMem(builder, status.FieldRangeMem)
	gamma_api.EngineStatusAddBitmapMem(builder, status.BitmapMem)
	gamma_api.EngineStatusAddDocNum(builder, status.DocNum)
	gamma_api.EngineStatusAddMaxDocid(builder, status.MaxDocID)
	builder.Finish(builder.EndObject())
	bufferLen := len(builder.FinishedBytes())
	*buffer = make([]byte, bufferLen)
	copy(*buffer, builder.FinishedBytes())
	return bufferLen
}

func (status *EngineStatus) DeSerialize(buffer []byte) {
	status.engineStatus = gamma_api.GetRootAsEngineStatus(buffer, 0)
	status.IndexStatus = status.engineStatus.IndexStatus()
	status.TableMem = status.engineStatus.TableMem()
	status.VectorMem = status.engineStatus.VectorMem()
	status.FieldRangeMem = status.engineStatus.FieldRangeMem()
	status.BitmapMem = status.engineStatus.BitmapMem()
	status.DocNum = status.engineStatus.DocNum()
	status.MaxDocID = status.engineStatus.MaxDocid()
}
