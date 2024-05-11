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

type MemoryInfo struct {
	TableMem      int64
	IndexMem      int64
	VectorMem     int64
	FieldRangeMem int64
	BitmapMem     int64

	memoryInfo *gamma_api.MemoryInfo
}

func (status *MemoryInfo) Serialize() []byte {
	builder := flatbuffers.NewBuilder(0)

	gamma_api.MemoryInfoStart(builder)
	gamma_api.MemoryInfoAddTableMem(builder, status.TableMem)
	gamma_api.MemoryInfoAddIndexMem(builder, status.IndexMem)
	gamma_api.MemoryInfoAddVectorMem(builder, status.VectorMem)
	gamma_api.MemoryInfoAddFieldRangeMem(builder, status.FieldRangeMem)
	gamma_api.MemoryInfoAddBitmapMem(builder, status.BitmapMem)
	builder.Finish(builder.EndObject())
	return builder.FinishedBytes()
}

func (status *MemoryInfo) DeSerialize(buffer []byte) {
	status.memoryInfo = gamma_api.GetRootAsMemoryInfo(buffer, 0)
	status.TableMem = status.memoryInfo.TableMem()
	status.IndexMem = status.memoryInfo.IndexMem()
	status.VectorMem = status.memoryInfo.VectorMem()
	status.FieldRangeMem = status.memoryInfo.FieldRangeMem()
	status.BitmapMem = status.memoryInfo.BitmapMem()
}
