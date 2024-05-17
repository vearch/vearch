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

import "C"
import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/engine/sdk/go/gamma"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vearchlog"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine"
)

const indexSn = "sn"

var _ engine.Reader = &readerImpl{}

type readerImpl struct {
	engine *gammaEngine
}

func (ri *readerImpl) GetDoc(ctx context.Context, doc *vearchpb.Document, getByDocId bool, next bool) error {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	var primaryKey []byte
	var docID int
	if getByDocId {
		docId, err := strconv.ParseUint(doc.PKey, 10, 32)
		if err != nil {
			msg := fmt.Sprintf("key: [%s] convert to uint32 failed, err: [%s]", doc.PKey, err.Error())
			return vearchpb.NewError(vearchpb.ErrorEnum_PRIMARY_KEY_IS_INVALID, errors.New(msg))
		}
		docID = int(docId)
	} else {
		primaryKey = []byte(doc.PKey)
	}

	docGamma := new(gamma.Doc)
	var code int
	if getByDocId {
		code = gamma.GetDocByDocID(ri.engine.gamma, docID, next, docGamma)
	} else {
		code = gamma.GetDocByID(ri.engine.gamma, primaryKey, docGamma)
	}
	if code != 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_DOCUMENT_NOT_EXIST, nil)
	}
	doc.Fields = docGamma.Fields
	return nil
}

func (ri *readerImpl) ReadSN(ctx context.Context) (int64, error) {
	ri.engine.lock.RLock()
	defer ri.engine.lock.RUnlock()
	fileName := filepath.Join(ri.engine.path, indexSn)
	b, err := os.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		} else {
			return 0, err
		}
	}
	sn, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, err
	}
	return sn, nil
}

func (ri *readerImpl) DocCount(ctx context.Context) (uint64, error) {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	gammaEngine := ri.engine.gamma
	if gammaEngine == nil {
		return 0, vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil))
	}

	var status gamma.EngineStatus
	gamma.GetEngineStatus(gammaEngine, &status)
	docNum := status.DocNum
	return uint64(docNum), nil
}

func (ri *readerImpl) Capacity(ctx context.Context) (int64, error) {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	gammaEngine := ri.engine.gamma
	if gammaEngine == nil {
		return 0, vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil))
	}

	var status gamma.MemoryInfo
	gamma.GetEngineMemoryInfo(gammaEngine, &status)
	vectorMem := status.VectorMem
	tableMem := status.TableMem
	fieldRangeMem := status.FieldRangeMem
	bitmapMem := status.BitmapMem
	memoryBytes := vectorMem + tableMem + fieldRangeMem + bitmapMem

	log.Debug("gamma use memory total:[%d], bitmap %d, range %d, table %d, vector %d",
		memoryBytes, bitmapMem, fieldRangeMem, tableMem, vectorMem)
	return int64(memoryBytes), nil
}

func (ri *readerImpl) Search(ctx context.Context, request *vearchpb.SearchRequest, response *vearchpb.SearchResponse) error {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	engine := ri.engine.gamma
	if engine == nil {
		return vearchpb.NewErrorInfo(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, "search engine is null")
	}

	if response == nil {
		response = &vearchpb.SearchResponse{}
	}

	trace := config.Trace
	if trace_info, ok := request.Head.Params["trace"]; ok {
		if trace_info == "true" {
			trace = true
			request.Trace = true
		}
	}

	if trace {
		partitionIDstr := strconv.FormatUint(uint64(ri.engine.partitionID), 10)

		startTime := time.Now()
		reqByte := gamma.SearchRequestSerialize(request)
		serialize := (time.Since(startTime).Seconds()) * 1000
		gammaStartTime := time.Now()
		respByte, status := gamma.Search(ri.engine.gamma, reqByte)
		gamma := (time.Since(gammaStartTime).Seconds()) * 1000
		response.FlatBytes = respByte
		serializeStr := strconv.FormatFloat(serialize, 'f', 4, 64)
		gammaStr := strconv.FormatFloat(gamma, 'f', 4, 64)

		if response.Head == nil {
			costTimeMap := make(map[string]string)
			costTimeMap["serialize_"+partitionIDstr] = serializeStr
			costTimeMap["gamma_"+partitionIDstr] = gammaStr
			responseHead := &vearchpb.ResponseHead{Params: costTimeMap}
			response.Head = responseHead
		} else if response.Head != nil && response.Head.Params == nil {
			costTimeMap := make(map[string]string)
			costTimeMap["serialize_"+partitionIDstr] = serializeStr
			costTimeMap["gamma_"+partitionIDstr] = gammaStr
			response.Head.Params = costTimeMap
		} else {
			response.Head.Params["serialize_"+partitionIDstr] = serializeStr
			response.Head.Params["gamma_"+partitionIDstr] = gammaStr
		}
		if status.Code != 0 {
			return vearchpb.NewErrorInfo(vearchpb.ErrorEnum_SEARCH_ENGINE_ERR, status.Msg)
		}
	} else {
		reqByte := gamma.SearchRequestSerialize(request)
		respByte, status := gamma.Search(ri.engine.gamma, reqByte)
		response.FlatBytes = respByte

		if response.Head == nil {
			responseHead := &vearchpb.ResponseHead{}
			response.Head = responseHead
		}
		if status.Code != 0 {
			return vearchpb.NewErrorInfo(vearchpb.ErrorEnum_SEARCH_ENGINE_ERR, status.Msg)
		}
	}

	return nil
}

func (ri *readerImpl) Query(ctx context.Context, request *vearchpb.QueryRequest, response *vearchpb.SearchResponse) error {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	engine := ri.engine.gamma
	if engine == nil {
		return vearchpb.NewErrorInfo(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, "search engine is null")
	}

	if response == nil {
		response = &vearchpb.SearchResponse{}
	}

	trace := config.Trace
	if trace_info, ok := request.Head.Params["trace"]; ok {
		if trace_info == "true" {
			trace = true
			request.Trace = true
		}
	}

	if trace {
		partitionIDstr := strconv.FormatUint(uint64(ri.engine.partitionID), 10)

		startTime := time.Now()
		reqByte := gamma.QueryRequestSerialize(request)
		serialize := (time.Since(startTime).Seconds()) * 1000
		gammaStartTime := time.Now()
		respByte, status := gamma.Search(ri.engine.gamma, reqByte)
		gamma := (time.Since(gammaStartTime).Seconds()) * 1000
		response.FlatBytes = respByte
		serializeStr := strconv.FormatFloat(serialize, 'f', 4, 64)
		gammaStr := strconv.FormatFloat(gamma, 'f', 4, 64)
		if response.Head == nil {
			costTimeMap := make(map[string]string)
			costTimeMap["serialize_"+partitionIDstr] = serializeStr
			costTimeMap["gamma_"+partitionIDstr] = gammaStr
			responseHead := &vearchpb.ResponseHead{Params: costTimeMap}
			response.Head = responseHead
		} else if response.Head != nil && response.Head.Params == nil {
			costTimeMap := make(map[string]string)
			costTimeMap["serialize_"+partitionIDstr] = serializeStr
			costTimeMap["gamma_"+partitionIDstr] = gammaStr
			response.Head.Params = costTimeMap
		} else {
			response.Head.Params["serialize_"+partitionIDstr] = serializeStr
			response.Head.Params["gamma_"+partitionIDstr] = gammaStr
		}

		if status.Code != 0 {
			return vearchpb.NewErrorInfo(vearchpb.ErrorEnum_QUERY_ENGINE_ERR, status.Msg)
		}
	} else {
		reqByte := gamma.QueryRequestSerialize(request)
		respByte, status := gamma.Search(ri.engine.gamma, reqByte)
		response.FlatBytes = respByte
		if response.Head == nil {
			responseHead := &vearchpb.ResponseHead{}
			response.Head = responseHead
		}

		if status.Code != 0 {
			return vearchpb.NewErrorInfo(vearchpb.ErrorEnum_QUERY_ENGINE_ERR, status.Msg)
		}
	}

	return nil
}
