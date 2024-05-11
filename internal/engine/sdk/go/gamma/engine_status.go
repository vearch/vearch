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

type EngineStatus struct {
	IndexStatus   int32
	TableMem      int64
	IndexMem      int64
	VectorMem     int64
	FieldRangeMem int64
	BitmapMem     int64
	DocNum        int32
	MaxDocid      int32
	MinIndexedNum int32

	engineStatus *gamma_api.EngineStatus
}

func (status *EngineStatus) Serialize(buffer *[]byte) int {
	builder := flatbuffers.NewBuilder(0)

	gamma_api.EngineStatusStart(builder)
	gamma_api.EngineStatusAddIndexStatus(builder, status.IndexStatus)
	gamma_api.EngineStatusAddTableMem(builder, status.TableMem)
	gamma_api.EngineStatusAddIndexMem(builder, status.IndexMem)
	gamma_api.EngineStatusAddVectorMem(builder, status.VectorMem)
	gamma_api.EngineStatusAddFieldRangeMem(builder, status.FieldRangeMem)
	gamma_api.EngineStatusAddBitmapMem(builder, status.BitmapMem)
	gamma_api.EngineStatusAddDocNum(builder, status.DocNum)
	gamma_api.EngineStatusAddMaxDocid(builder, status.MaxDocid)
	gamma_api.EngineStatusAddMinIndexedNum(builder, status.MinIndexedNum)
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
	status.IndexMem = status.engineStatus.IndexMem()
	status.VectorMem = status.engineStatus.VectorMem()
	status.FieldRangeMem = status.engineStatus.FieldRangeMem()
	status.BitmapMem = status.engineStatus.BitmapMem()
	status.DocNum = status.engineStatus.DocNum()
	status.MaxDocid = status.engineStatus.MaxDocid()
	status.MinIndexedNum = status.engineStatus.MinIndexedNum()
}
