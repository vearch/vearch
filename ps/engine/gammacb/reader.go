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

/*
#cgo CFLAGS : -Ilib/include
#cgo LDFLAGS: -Llib/lib -lgamma

#include "gamma_api.h"
*/
import "C"
import (
	"context"
	"fmt"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/util/ioutil2"
	"github.com/vearch/vearch/util/vearchlog"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const indexSn = "sn"

var _ engine.Reader = &readerImpl{}

var _ engine.RTReader = &readerImpl{}

type readerImpl struct {
	engine *gammaEngine
	path   string
	lock   sync.RWMutex
}

func (ri *readerImpl) RTReadDoc(ctx context.Context, docID string) *response.DocResult {
	return ri.GetDoc(ctx, docID)
}

func (ri *readerImpl) GetDoc(ctx context.Context, docID string) *response.DocResult {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	cID := byteArrayStr(docID)
	defer C.DestroyByteArray(cID)
	doc := C.GetDocByID(ri.engine.gamma, cID)
	if doc == nil {
		return response.NewNotFoundDocResult(docID)
	}
	defer C.DestroyDoc(doc)
	result := ri.engine.Doc2DocResult(doc)
	return result
}

func (ri *readerImpl) GetDocs(ctx context.Context, docIDs []string) []*response.DocResult {

	docs := make([]*response.DocResult, len(docIDs))

	for i := 0; i < len(docs); i++ {
		docs[i] = ri.GetDoc(ctx, docIDs[i])
	}

	return docs
}

func (ri *readerImpl) MSearch(ctx context.Context, request *request.SearchRequest) response.SearchResponses {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	builder := &queryBuilder{mapping: ri.engine.GetMapping()}

	req := C.MakeRequest(C.int(*request.Size),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		C.int(1), C.int(0), nil)

	defer C.DestroyRequest(req)
	if err := builder.parseQuery(request.Query, req); err != nil {
		return response.SearchResponses{response.NewSearchResponseErr(vearchlog.LogErrAndReturn(fmt.Errorf("parse query has err:[%s] query:[%s]", err.Error(), string(request.Query))))}
	}

	if len(request.Fields) > 0 {
		req.fields = C.MakeByteArrays(C.int(len(request.Fields)))
		fs := make([]*C.struct_ByteArray, len(request.Fields))
		for i, f := range request.Fields {
			C.SetByteArray(req.fields, C.int(i), byteArrayStr(f))
		}
		req.fields_num = C.int(len(fs))
	}

	if log.IsDebugEnabled() {
		log.Debug("send request:[%v]", req)
	}
	reps := C.Search(ri.engine.gamma, req)
	defer C.DestroyResponse(reps)

	result := make(response.SearchResponses, int(req.req_num))

	for index := range result {
		result[index] = ri.singleSearchResult(reps, index)
	}

	return result
}

func (ri *readerImpl) Search(ctx context.Context, request *request.SearchRequest) *response.SearchResponse {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	gamma := ri.engine.gamma
	if gamma == nil {
		return response.NewSearchResponseErr(vearchlog.LogErrAndReturn(pkg.ErrPartitionClosed))
	}

	builder := &queryBuilder{mapping: ri.engine.GetMapping()}

	req := C.MakeRequest(C.int(*request.Size),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		C.int(1), C.int(0), nil)

	defer C.DestroyRequest(req)
	if err := builder.parseQuery(request.Query, req); err != nil {
		return response.NewSearchResponseErr(vearchlog.LogErrAndReturn(fmt.Errorf("parse query has err:[%s] query:[%s]", err.Error(), string(request.Query))))
	}

	if len(request.Fields) == 0 {
		req.fields = C.MakeByteArrays(C.int(1))
		C.SetByteArray(req.fields, 0, byteArrayStr("_source"))
		req.fields_num = C.int(1)
	} else {
		req.fields = C.MakeByteArrays(C.int(len(request.Fields)))
		fs := make([]*C.struct_ByteArray, len(request.Fields))
		for i, f := range request.Fields {
			C.SetByteArray(req.fields, C.int(i), byteArrayStr(f))
		}
		req.fields_num = C.int(len(fs))
	}

	if log.IsDebugEnabled() {
		log.Debug("send request:[%v]", req)
	}
	start := time.Now()
	reps := C.Search(gamma, req)
	end := time.Now().Sub(start)
	defer C.DestroyResponse(reps)

	result := ri.singleSearchResult(reps, 0)

	result.Took = end.Nanoseconds()

	return result

}

func (ri *readerImpl) singleSearchResult(reps *C.struct_Response, index int) *response.SearchResponse {
	rep := C.GetSearchResult(reps, C.int(index))
	if rep.result_code > 0 {
		msg := string(CbArr2ByteArray(rep.msg)) + ", code:[%d]"
		return response.NewSearchResponseErr(vearchlog.LogErrAndReturn(fmt.Errorf(msg, rep.result_code)))
	}

	hits := make(response.Hits, 0, int(rep.result_num))

	var maxScore float64 = -1
	size := int(rep.result_num)
	for i := 0; i < size; i++ {
		item := C.GetResultItem(rep, C.int(i))
		result := ri.engine.ResultItem2DocResult(item)
		if maxScore < result.Score {
			maxScore = result.Score
		}
		hits = append(hits, result)
	}
	result := response.SearchResponse{
		Total:    uint64(rep.total),
		MaxScore: maxScore,
		Hits:     hits,
		Status:   &response.SearchStatus{Total: 1, Successful: 1},
	}

	if reps.online_log_message != nil {
		result.Explain = map[uint32]string{
			ri.engine.partitionID: string(CbArr2ByteArray(reps.online_log_message)),
		}
	}

	return &result
}

func (ri *readerImpl) StreamSearch(ctx context.Context, req *request.SearchRequest, resultChan chan *response.DocResult) error {
	panic("implement me")
}

func (ri *readerImpl) ReadSN(ctx context.Context) (int64, error) {
	ri.lock.RLock()
	defer ri.lock.RUnlock()
	fileName := filepath.Join(ri.path, indexSn)
	b, err := ioutil.ReadFile(fileName)
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

	gamma := ri.engine.gamma
	if gamma == nil {
		return 0, vearchlog.LogErrAndReturn(pkg.ErrPartitionClosed)
	}

	num := C.GetDocsNum(gamma)
	return uint64(num), nil
}

func (ri *readerImpl) Capacity(ctx context.Context) (int64, error) {
	return ioutil2.DirSize(ri.path)
}
