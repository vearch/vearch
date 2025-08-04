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
	"unsafe"

	"github.com/vearch/vearch/v3/internal/entity"
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

func Search(engine unsafe.Pointer, reqByte []byte) ([]byte, *Status) {
	var CBuffer *C.char
	zero := 0
	length := &zero

	cstatus := C.Search(engine,
		(*C.char)(unsafe.Pointer(&reqByte[0])), C.int(len(reqByte)),
		(**C.char)(unsafe.Pointer(&CBuffer)),
		(*C.int)(unsafe.Pointer(length)))
	defer C.free(unsafe.Pointer(CBuffer))
	respByte := C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))
	status := &Status{
		Code: int32(cstatus.code),
		Msg:  C.GoString(cstatus.msg),
	}
	if status.Code != 0 {
		C.free(unsafe.Pointer(cstatus.msg))
	}
	return respByte, status
}

func Query(engine unsafe.Pointer, reqByte []byte) ([]byte, *Status) {
	var CBuffer *C.char
	zero := 0
	length := &zero

	cstatus := C.Query(engine,
		(*C.char)(unsafe.Pointer(&reqByte[0])), C.int(len(reqByte)),
		(**C.char)(unsafe.Pointer(&CBuffer)),
		(*C.int)(unsafe.Pointer(length)))
	defer C.free(unsafe.Pointer(CBuffer))
	respByte := C.GoBytes(unsafe.Pointer(CBuffer), C.int(*length))
	status := &Status{
		Code: int32(cstatus.code),
		Msg:  C.GoString(cstatus.msg),
	}
	if status.Code != 0 {
		C.free(unsafe.Pointer(cstatus.msg))
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
	if command == "create" {
		c = entity.Create
	} else if command == "restore" {
		c = entity.Restore
	} else {
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

// AddFieldIndexWithParams adds index for a field with specified index parameters
// Note: This will use the new C API function when available
func AddFieldIndexWithParams(engine unsafe.Pointer, fieldName string, indexType string, indexParams []byte) *Status {
	fieldNameBytes := []byte(fieldName)
	indexTypeBytes := []byte(indexType)
	cstatus := C.AddFieldIndexWithParams(
		engine,
		(*C.char)(unsafe.Pointer(&fieldNameBytes[0])),
		C.int(len(fieldNameBytes)),
		(*C.char)(unsafe.Pointer(&indexTypeBytes[0])),
		C.int(len(indexTypeBytes)),
		(*C.char)(unsafe.Pointer(&indexParams[0])),
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

func RemoveFieldIndex(engine unsafe.Pointer, fieldName string) *Status {
	fieldNameBytes := []byte(fieldName)
	cstatus := C.RemoveFieldIndex(engine, (*C.char)(unsafe.Pointer(&fieldNameBytes[0])), C.int(len(fieldNameBytes)))

	status := &Status{
		Code: int32(cstatus.code),
		Msg:  C.GoString(cstatus.msg),
	}
	if status.Code != 0 {
		C.free(unsafe.Pointer(cstatus.msg))
	}
	return status
}
