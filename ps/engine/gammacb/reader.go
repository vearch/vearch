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
	"github.com/vearch/vearch/util/cbbytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/vearch/vearch/engine/gamma/idl/fbs-gen/go/gamma_api"
	pkg "github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/util/vearchlog"
)

const indexSn = "sn"

var _ engine.Reader = &readerImpl{}

var _ engine.RTReader = &readerImpl{}

type readerImpl struct {
	engine *gammaEngine
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
	result := ri.engine.Doc2DocResultCGO(doc)
	return result
}

func (ri *readerImpl) GetDocs(ctx context.Context, docIDs []string) []*response.DocResult {

	docs := make([]*response.DocResult, len(docIDs))

	for i := 0; i < len(docs); i++ {
		docs[i] = ri.GetDoc(ctx, docIDs[i])
	}

	return docs
}

func (ri *readerImpl) MSearchIDs(ctx context.Context, request *request.SearchRequest) ([]byte, error) {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	gamma := ri.engine.gamma
	if gamma == nil {
		return nil, pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_CLOSED)
	}

	builder := &queryBuilder{mapping: ri.engine.GetMapping()}

	hasRank := C.int(1)
	if request.Quick {
		hasRank = C.int(0)
	}

	parallelBasedOnQuery := C.char(0)
	if request.Parallel {
		parallelBasedOnQuery = C.char(1)
	}

	req := C.MakeRequest(C.int(*request.Size),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		C.int(1), C.int(0),
		nil, hasRank, C.int(0),
		parallelBasedOnQuery,
	)

	defer C.DestroyRequest(req)
	if err := builder.parseQuery(request.Query, req); err != nil {
		return nil, fmt.Errorf("parse query has err:[%s] query:[%s]", err.Error(), string(request.Query))
	}

	if len(request.Fields) == 0 && request.VectorValue {
		request.Fields = make([]string, 0, 10)
		_ = ri.engine.indexMapping.RangeField(func(key string, value *mapping.DocumentMapping) error {
			request.Fields = append(request.Fields, key)
			return nil
		})

		request.Fields = append(request.Fields, mapping.IdField)
	}

	if len(request.Fields) > 0 {
		ri.setFields(request, req)
	}

	arr := C.SearchV2(ri.engine.gamma, req)

	goarr := CbArr2ByteArray(arr)
	carr := cbbytes.CloneBytes(goarr)
	defer C.DestroyByteArray(arr)

	return carr, nil

}

func (ri *readerImpl) MSearch(ctx context.Context, request *request.SearchRequest) response.SearchResponses {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	gamma := ri.engine.gamma
	if gamma == nil {
		return response.SearchResponses{response.NewSearchResponseErr(vearchlog.LogErrAndReturn(pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_CLOSED)))}
	}

	builder := &queryBuilder{mapping: ri.engine.GetMapping()}

	hasRank := C.int(1)
	if request.Quick {
		hasRank = C.int(0)
	}

	parallelBasedOnQuery := C.char(0)
	if request.Parallel {
		parallelBasedOnQuery = C.char(1)
	}

	req := C.MakeRequest(C.int(*request.Size),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		C.int(1), C.int(0),
		nil, hasRank, C.int(0),
		parallelBasedOnQuery,
	)

	defer C.DestroyRequest(req)
	if err := builder.parseQuery(request.Query, req); err != nil {
		return response.SearchResponses{response.NewSearchResponseErr(vearchlog.LogErrAndReturn(fmt.Errorf("parse query has err:[%s] query:[%s]", err.Error(), string(request.Query))))}
	}

	if len(request.Fields) == 0 && request.VectorValue {
		request.Fields = make([]string, 0, 10)
		_ = ri.engine.indexMapping.RangeField(func(key string, value *mapping.DocumentMapping) error {
			request.Fields = append(request.Fields, key)
			return nil
		})

		request.Fields = append(request.Fields, mapping.IdField)
	}

	if len(request.Fields) > 0 {
		ri.setFields(request, req)
	}

	start := time.Now()
	arr := C.SearchV2(ri.engine.gamma, req)
	defer C.DestroyByteArray(arr)

	resp := gamma_api.GetRootAsResponse(CbArr2ByteArray(arr), 0)

	wg := sync.WaitGroup{}
	result := make(response.SearchResponses, resp.ResultsLength())
	for i := 0; i < len(result); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result[i] = ri.singleSearchResult(resp, i)
			result[i].MaxTook = int64(time.Now().Sub(start) / time.Millisecond)
			result[i].MaxTookID = ri.engine.partitionID
		}(i)
	}

	wg.Wait()

	return result
}

func (ri *readerImpl) Search(ctx context.Context, request *request.SearchRequest) *response.SearchResponse {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	gamma := ri.engine.gamma
	if gamma == nil {
		return response.NewSearchResponseErr(vearchlog.LogErrAndReturn(pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_CLOSED)))
	}

	builder := &queryBuilder{mapping: ri.engine.GetMapping()}

	hasRank := C.int(1)
	if request.Quick {
		hasRank = C.int(0)
	}

	parallelBasedOnQuery := C.char(0)
	if request.Parallel {
		parallelBasedOnQuery = C.char(1)
	}

	req := C.MakeRequest(C.int(*request.Size),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		nil, C.int(0),
		C.int(1), C.int(0),
		nil, hasRank, C.int(0),
		parallelBasedOnQuery,
	)

	defer C.DestroyRequest(req)
	if err := builder.parseQuery(request.Query, req); err != nil {
		return response.NewSearchResponseErr(vearchlog.LogErrAndReturn(fmt.Errorf("parse query has err:[%s] query:[%s]", err.Error(), string(request.Query))))
	}

	if len(request.Fields) == 0 && request.VectorValue {
		request.Fields = make([]string, 0, 10)
		_ = ri.engine.indexMapping.RangeField(func(key string, value *mapping.DocumentMapping) error {
			request.Fields = append(request.Fields, key)
			return nil
		})
		request.Fields = append(request.Fields, mapping.IdField)
	}

	if len(request.Fields) > 0 {
		ri.setFields(request, req)
	}

	start := time.Now()

	arr := C.SearchV2(ri.engine.gamma, req)
	defer C.DestroyByteArray(arr)

	resp := gamma_api.GetRootAsResponse(CbArr2ByteArray(arr), 0)

	result := ri.singleSearchResult(resp, 0)
	result.MaxTook = int64(time.Now().Sub(start) / time.Millisecond)
	result.MaxTookID = ri.engine.partitionID

	return result

}

func (ri *readerImpl) singleSearchResultIDs(reps *gamma_api.Response, index int) ([]string, error) {
	searchResult := new(gamma_api.SearchResult)
	reps.Results(searchResult, index)
	if searchResult.ResultCode() > 0 {
		msg := string(searchResult.Msg()) + ", code:[%d]"
		return nil, fmt.Errorf(msg, searchResult.ResultCode())
	}

	l := searchResult.ResultItemsLength()

	ids := make([]string, 0, l)

	for i := 0; i < l; i++ {
		item := new(gamma_api.ResultItem)
		searchResult.ResultItems(item, i)
		value := string(item.Value(0))
		ids = append(ids, value)
	}

	return ids, nil
}

func (ri *readerImpl) singleSearchResult(reps *gamma_api.Response, index int) *response.SearchResponse {
	searchResult := new(gamma_api.SearchResult)
	reps.Results(searchResult, index)
	if searchResult.ResultCode() > 0 {
		msg := string(searchResult.Msg()) + ", code:[%d]"
		return response.NewSearchResponseErr(vearchlog.LogErrAndReturn(fmt.Errorf(msg, searchResult.ResultCode())))
	}

	l := searchResult.ResultItemsLength()
	hits := make(response.Hits, 0, l)

	var maxScore float64 = -1

	for i := 0; i < l; i++ {
		item := new(gamma_api.ResultItem)
		searchResult.ResultItems(item, i)
		result := ri.engine.ResultItem2DocResult(item)
		if maxScore < result.Score {
			maxScore = result.Score
		}
		hits = append(hits, result)
	}
	result := response.SearchResponse{
		Total:    uint64(searchResult.Total()),
		MaxScore: maxScore,
		Hits:     hits,
		Status:   &response.SearchStatus{Total: 1, Successful: 1},
	}

	message := reps.OnlineLogMessage()
	if len(message) > 0 {
		result.Explain = map[uint32]string{
			ri.engine.partitionID: string(message),
		}
	}

	return &result
}

func (ri *readerImpl) setFields(request *request.SearchRequest, req *C.struct_Request) {
	req.fields = C.MakeByteArrays(C.int(len(request.Fields)))
	fs := make([]*C.struct_ByteArray, len(request.Fields))

	hasID := false
	for i, f := range request.Fields {
		if !hasID && f == mapping.IdField {
			hasID = true
		}
		C.SetByteArray(req.fields, C.int(i), byteArrayStr(f))
	}

	fsLen := len(fs)
	if !hasID {
		C.SetByteArray(req.fields, C.int(fsLen), byteArrayStr(mapping.IdField))
		fsLen++
	}

	req.fields_num = C.int(fsLen)
}

func (ri *readerImpl) StreamSearch(ctx context.Context, req *request.SearchRequest, resultChan chan *response.DocResult) error {
	panic("implement me")
}

func (ri *readerImpl) ReadSN(ctx context.Context) (int64, error) {
	ri.engine.lock.RLock()
	defer ri.engine.lock.RUnlock()
	fileName := filepath.Join(ri.engine.path, indexSn)
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
		return 0, vearchlog.LogErrAndReturn(pkg.CodeErr(pkg.ERRCODE_PARTITION_IS_CLOSED))
	}

	num := C.GetDocsNum(gamma)
	return uint64(num), nil
}

func (ri *readerImpl) Capacity(ctx context.Context) (int64, error) {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()
	//ioutil2.DirSize(ri.engine.path) TODO remove it
	return int64(C.GetMemoryBytes(ri.engine.gamma)), nil
}
