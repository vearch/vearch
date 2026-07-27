/**
 * Copyright 2019 The Vearch Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

/*
#cgo CFLAGS : -I../../../c_api
#cgo LDFLAGS: -L../../../../../build/gamma_build -lgamma

#include "gamma_api.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/vearch/vearch/v3/internal/engine/idl/fbs-gen/go/gamma_api"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/entity/request"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
)

type Status struct {
	Code int32
	Msg  string
}

func Init(config []byte) unsafe.Pointer {
	return C.Init((*C.char)(unsafe.Pointer(&config[0])), C.int(len(config)))
}

func Close(engine unsafe.Pointer) int {
	return int(C.Close(engine))
}

func CreateTable(engine unsafe.Pointer, table *Table) *Status {
	tableBytes := table.Serialize()
	cstatus := C.CreateTable(engine, (*C.char)(unsafe.Pointer(&tableBytes[0])), C.int(len(tableBytes)))

	status := &Status{
		Code: int32(cstatus.code),
		Msg:  C.GoString(cstatus.msg),
	}
	if status.Code != 0 {
		C.free(unsafe.Pointer(cstatus.msg))
	}
	return status
}

func AddOrUpdateDocs(engine unsafe.Pointer, buffer [][]byte) []int32 {
	num := len(buffer)
	resultCode := make([]int32, num)

	for i := range num {
		resultCode[i] = int32(C.AddOrUpdateDoc(engine, (*C.char)(unsafe.Pointer(&buffer[i][0])), C.int(len(buffer[i]))))
	}
	return resultCode
}

func DeleteDoc(engine unsafe.Pointer, docID []byte) int {
	return int(C.DeleteDoc(engine, (*C.char)(unsafe.Pointer(&docID[0])), C.int(len(docID))))
}

func GetEngineStatus(engine unsafe.Pointer) (status string) {
	if engine == nil {
		return
	}
	var CBuffer *C.char
	zero := 0
	length := &zero
	C.GetEngineStatus(engine, (**C.char)(unsafe.Pointer(&CBuffer)), (*C.int)(unsafe.Pointer(length)))
	defer C.free(unsafe.Pointer(CBuffer))
	buffer := C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))
	return string(buffer)
}

type MemoryInfo struct {
	TableMem      int64 `json:"table_mem,omitempty"`
	IndexMem      int64 `json:"index_mem,omitempty"`
	VectorMem     int64 `json:"vector_mem,omitempty"`
	FieldRangeMem int64 `json:"field_range_mem,omitempty"`
	BitmapMem     int64 `json:"bitmap_mem,omitempty"`
}

func GetEngineMemoryInfo(engine unsafe.Pointer, status *MemoryInfo) error {
	if engine == nil {
		return fmt.Errorf("engine is null")
	}
	var CBuffer *C.char
	zero := 0
	length := &zero
	C.GetMemoryInfo(engine, (**C.char)(unsafe.Pointer(&CBuffer)), (*C.int)(unsafe.Pointer(length)))
	defer C.free(unsafe.Pointer(CBuffer))
	buffer := C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))

	if err := vjson.Unmarshal(buffer, status); err != nil {
		return err
	}
	return nil
}

func GetDocByID(engine unsafe.Pointer, docID []byte, doc *Doc) int {
	var CBuffer *C.char
	zero := 0
	length := &zero
	ret := int(C.GetDocByID(engine,
		(*C.char)(unsafe.Pointer(&docID[0])),
		C.int(len(docID)),
		(**C.char)(unsafe.Pointer(&CBuffer)),
		(*C.int)(unsafe.Pointer(length))))
	defer C.free(unsafe.Pointer(CBuffer))
	buffer := C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))
	doc.DeSerialize(buffer)
	return ret
}

func GetDocByDocID(engine unsafe.Pointer, docID int, next bool, doc *Doc) int {
	var CBuffer *C.char
	zero := 0
	length := &zero

	cNext := 0
	if next {
		cNext = 1
	}
	ret := int(C.GetDocByDocID(engine,
		C.int(docID),
		C.char(cNext),
		(**C.char)(unsafe.Pointer(&CBuffer)),
		(*C.int)(unsafe.Pointer(length))))
	defer C.free(unsafe.Pointer(CBuffer))
	buffer := C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))
	doc.DeSerialize(buffer)
	return ret
}

func BuildIndex(engine unsafe.Pointer) int {
	return int(C.BuildIndex(engine))
}

func RebuildIndex(engine unsafe.Pointer, drop_before_rebuild int, limit_cpu int, describe int) int {
	return int(C.RebuildIndex(engine, C.int(drop_before_rebuild), C.int(limit_cpu), C.int(describe)))
}

func Dump(engine unsafe.Pointer) int {
	return int(C.Dump(engine))
}

func Load(engine unsafe.Pointer) int {
	return int(C.Load(engine))
}

func Search(engine unsafe.Pointer, reqByte []byte, message_id string, partition_id entity.PartitionID) ([]byte, *Status) {
	var CBuffer *C.char
	var respByte []byte
	var status *Status
	var requestStatus *request.RequestStatus
	zero := 0
	length := &zero

	requestId := request.RequestId{MessageId: message_id, PartitionId: partition_id}
	request.Rqueue.Mutex.RLock()
	elem := request.Rqueue.ReqMap[requestId]
	request.Rqueue.Mutex.RUnlock()

	requestStatus = elem.Value.(*request.RequestStatus)
	messageId := []byte(message_id)
	if requestStatus == nil || atomic.CompareAndSwapInt32(&requestStatus.Status, request.Running_1, request.Running_2) {
		cstatus := C.Search(engine,
			(*C.char)(unsafe.Pointer(&reqByte[0])), C.int(len(reqByte)),
			(**C.char)(unsafe.Pointer(&CBuffer)),
			(*C.int)(unsafe.Pointer(length)))
		defer C.free(unsafe.Pointer(CBuffer))
		respByte = C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))
		status = &Status{
			Code: int32(cstatus.code),
			Msg:  C.GoString(cstatus.msg),
		}
		if status.Code != 0 {
			C.free(unsafe.Pointer(cstatus.msg))
		}
		atomic.CompareAndSwapInt32(&requestStatus.Status, request.Running_2, request.Running_1)
		DeleteKillStatus(messageId, int(partition_id))
	} else {
		log.Debug("request cancel before search in engine")
		status = &Status{
			Code: int32(gamma_api.CodekMemoryExceeded),
			Msg:  "request canceled",
		}
	}

	return respByte, status
}

func Query(engine unsafe.Pointer, reqByte []byte, message_id string, partition_id entity.PartitionID) ([]byte, *Status) {
	var CBuffer *C.char
	var respByte []byte
	var status *Status
	var requestStatus *request.RequestStatus
	zero := 0
	length := &zero

	requestId := request.RequestId{MessageId: message_id, PartitionId: partition_id}
	request.Rqueue.Mutex.RLock()
	if elem := request.Rqueue.ReqMap[requestId]; elem != nil {
		requestStatus = elem.Value.(*request.RequestStatus)
	}
	request.Rqueue.Mutex.RUnlock()

	messageId := []byte(message_id)
	if requestStatus == nil || atomic.CompareAndSwapInt32(&requestStatus.Status, request.Running_1, request.Running_2) {
		cstatus := C.Query(engine,
			(*C.char)(unsafe.Pointer(&reqByte[0])), C.int(len(reqByte)),
			(**C.char)(unsafe.Pointer(&CBuffer)),
			(*C.int)(unsafe.Pointer(length)))
		defer C.free(unsafe.Pointer(CBuffer))

		respByte = C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))
		status = &Status{
			Code: int32(cstatus.code),
			Msg:  C.GoString(cstatus.msg),
		}
		if status.Code != 0 {
			C.free(unsafe.Pointer(cstatus.msg))
		}
		if requestStatus != nil {
			atomic.CompareAndSwapInt32(&requestStatus.Status, request.Running_2, request.Running_1)
			DeleteKillStatus(messageId, int(partition_id))
		}
	} else {
		status = &Status{
			Code: int32(gamma_api.CodekMemoryExceeded),
			Msg:  "request canceled",
		}
	}
	return respByte, status
}

func SetEngineCfg(engine unsafe.Pointer, configJson []byte) int {
	ret := int(C.SetConfig(engine, (*C.char)(unsafe.Pointer(&configJson[0])), C.int(len(configJson))))
	return ret
}

func GetEngineCfg(engine unsafe.Pointer) (configJson []byte) {
	var CBuffer *C.char
	zero := 0
	length := &zero
	C.GetConfig(engine, (**C.char)(unsafe.Pointer(&CBuffer)), (*C.int)(unsafe.Pointer(length)))
	defer C.free(unsafe.Pointer(CBuffer))
	return C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))
}

func BackupSpace(engine unsafe.Pointer, command string) *Status {
	var c int
	switch command {
	case "create":
		c = entity.Create
	case "restore":
		c = entity.Restore
	default:
		return &Status{
			Code: -1,
			Msg:  "command not support",
		}
	}

	cstatus := C.Backup(engine, C.int(c))

	status := &Status{
		Code: int32(cstatus.code),
		Msg:  C.GoString(cstatus.msg),
	}
	if status.Code != 0 {
		C.free(unsafe.Pointer(cstatus.msg))
	}
	return status
}

// AddFieldIndexWithParams adds an index identified by indexName covering the
// given field names. A single vector field => vector index, a single scalar
// field => scalar index, 2+ scalar fields => composite scalar index. The
// caller may pass a single-element slice for single-field indexes; this SDK
// normalizes that into a uniform vector<string> on the C++ side.
func AddFieldIndexWithParams(engine unsafe.Pointer, indexName string, fieldNames []string, indexType string, indexParams []byte) *Status {
	if len(fieldNames) == 0 {
		return &Status{Code: -1, Msg: "fieldNames is empty"}
	}
	for i, name := range fieldNames {
		if name == "" {
			return &Status{Code: -1, Msg: fmt.Sprintf("fieldNames[%d] is empty", i)}
		}
	}
	if len(indexParams) == 0 {
		indexParams = []byte("{}") // Default to empty JSON object if no params provided
	}

	// All buffers crossing the cgo boundary are allocated on the C heap to
	// avoid the "cgo argument has Go pointer to unpinned Go pointer" check:
	// the **char array would otherwise be a Go slice of Go-heap *C.char
	// elements, which violates the cgo pointer rule (a Go pointer must not
	// point to memory containing other Go pointers).
	cIndexName := C.CString(indexName)
	defer C.free(unsafe.Pointer(cIndexName))
	cIndexType := C.CString(indexType)
	defer C.free(unsafe.Pointer(cIndexType))
	cIndexParams := (*C.char)(C.CBytes(indexParams))
	defer C.free(unsafe.Pointer(cIndexParams))

	count := len(fieldNames)
	ptrSize := C.size_t(unsafe.Sizeof((*C.char)(nil)))
	intSize := C.size_t(unsafe.Sizeof(C.int(0)))

	cFieldNamesArr := (**C.char)(C.malloc(ptrSize * C.size_t(count)))
	defer C.free(unsafe.Pointer(cFieldNamesArr))
	cFieldNameLensArr := (*C.int)(C.malloc(intSize * C.size_t(count)))
	defer C.free(unsafe.Pointer(cFieldNameLensArr))

	cFieldNameSlots := unsafe.Slice(cFieldNamesArr, count)
	cFieldNameLenSlots := unsafe.Slice(cFieldNameLensArr, count)
	for i, name := range fieldNames {
		cFieldNameSlots[i] = C.CString(name)
		cFieldNameLenSlots[i] = C.int(len(name))
	}
	defer func() {
		for i := 0; i < count; i++ {
			C.free(unsafe.Pointer(cFieldNameSlots[i]))
		}
	}()

	cstatus := C.AddFieldIndexWithParams(
		engine,
		cIndexName,
		C.int(len(indexName)),
		cFieldNamesArr,
		cFieldNameLensArr,
		C.int(count),
		cIndexType,
		C.int(len(indexType)),
		cIndexParams,
		C.int(len(indexParams)))

	status := &Status{
		Code: int32(cstatus.code),
		Msg:  C.GoString(cstatus.msg),
	}
	if status.Code != 0 {
		C.free(unsafe.Pointer(cstatus.msg))
	}
	return status
}

// RemoveFieldIndex removes an index by its user-defined indexName.
func RemoveFieldIndex(engine unsafe.Pointer, indexName string) *Status {
	cIndexName := C.CString(indexName)
	defer C.free(unsafe.Pointer(cIndexName))
	cstatus := C.RemoveFieldIndex(engine, cIndexName, C.int(len(indexName)))

	status := &Status{
		Code: int32(cstatus.code),
		Msg:  C.GoString(cstatus.msg),
	}
	if status.Code != 0 {
		C.free(unsafe.Pointer(cstatus.msg))
	}
	return status
}

func SetMemoryLimitConfig(memory_limit int) {
	C.SetMemoryLimitConfig(C.int(memory_limit))
}

func SetKillStatus(request_id []byte, partition_id int, reason int) {
	C.SetKillStatus((*C.char)(unsafe.Pointer(&request_id[0])), C.int(partition_id), C.int(reason))
}

func DeleteKillStatus(request_id []byte, partition_id int) {
	C.DeleteKillStatus((*C.char)(unsafe.Pointer(&request_id[0])), C.int(partition_id))
}
