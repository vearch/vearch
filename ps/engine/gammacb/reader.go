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
	pkg "github.com/vearch/vearch/proto"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine/gamma"

	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/vearchlog"
)

const indexSn = "sn"

var _ engine.Reader = &readerImpl{}

type readerImpl struct {
	engine *gammaEngine
}

func (ri *readerImpl) GetDoc(ctx context.Context, doc *vearchpb.Document) error {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	var primaryKey []byte
	idType := ri.engine.space.Engine.IdType
	if strings.EqualFold("long", idType) {
		int64Id, err := strconv.ParseInt(doc.PKey, 10, 64)
		if err != nil {
			msg := fmt.Sprintf("key: [%s] convert to long failed, err: [%s]", doc.PKey, err.Error())
			return vearchpb.NewError(vearchpb.ErrorEnum_Primary_IS_INVALID, errors.New(msg))
		}
		toByteId, _ := cbbytes.ValueToByte(int64Id)
		primaryKey = toByteId
	} else {
		primaryKey = []byte(doc.PKey)
	}

	docGamma := new(gamma.Doc)
	if code := gamma.GetDocByID(ri.engine.gamma, primaryKey, docGamma); code != 0 {
		msg := "doc not found"
		return vearchpb.NewError(vearchpb.ErrorEnum_DOCUMENT_NOT_EXIST, errors.New(msg))
	} else {
		doc.Fields = docGamma.Fields
		// ri.engine.GammaDocConvertGODoc(docGamma, doc)
		return nil
	}
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

	//ioutil2.DirSize(ri.engine.path) TODO remove it

	var status gamma.EngineStatus
	gamma.GetEngineStatus(gammaEngine, &status)
	vectorMem := status.VectorMem
	tableMem := status.TableMem
	fieldRangeMem := status.FieldRangeMem
	bitmapMem := status.BitmapMem
	memoryBytes := vectorMem + tableMem + fieldRangeMem + bitmapMem
	return int64(memoryBytes), nil
}

func (ri *readerImpl) Search(ctx context.Context, request *vearchpb.SearchRequest, response *vearchpb.SearchResponse) error {
	ri.engine.counter.Incr()
	defer ri.engine.counter.Decr()

	gammaEngine := ri.engine.gamma
	if gammaEngine == nil {
		return pkg.NewCodeErr(pkg.ERRCODE_PARTITION_IS_CLOSED, "search gamma engine is null")
	}

	if response == nil {
		response = &vearchpb.SearchResponse{}
	}

	startTime := time.Now()
	reqByte := gamma.SearchRequestSerialize(request)
	serializeCostTime := (time.Now().Sub(startTime).Seconds()) * 1000
	gammaStartTime := time.Now()
	code, respByte := gamma.Search(ri.engine.gamma, reqByte)
	gammaCostTime := (time.Now().Sub(gammaStartTime).Seconds()) * 1000
	response.FlatBytes = respByte
	costTimeMap := make(map[string]string)
	costTimeMap["serializeCostTime"] = strconv.FormatFloat(serializeCostTime, 'E', -1, 64)
	costTimeMap["gammaCostTime"] = strconv.FormatFloat(gammaCostTime, 'E', -1, 64)
	if response.Head == nil {
		responseHead := &vearchpb.ResponseHead{Params: costTimeMap}
		response.Head = responseHead
	}

	if code != 0 {
		var vearchErr *vearchpb.VearchErr
		switch code {
		case -1:
			vearchErr = vearchpb.NewErrorInfo(vearchpb.ErrorEnum_GAMMA_SEARCH_QUERY_NUM_LESS_0, "gamma return err:query num less than 0")
		case -2:
			vearchErr = vearchpb.NewErrorInfo(vearchpb.ErrorEnum_GAMMA_SEARCH_NO_CREATE_INDEX, "gamma return err:no create index")
		case -3:
			vearchErr = vearchpb.NewErrorInfo(vearchpb.ErrorEnum_GAMMA_SEARCH_INDEX_QUERY_ERR, "gamma return err:index search error")
		default:
			vearchErr = vearchpb.NewErrorInfo(vearchpb.ErrorEnum_GAMMA_SEARCH_OTHER_ERR, "gamma return err: other errr")
		}
		return vearchErr
	}

	return nil
}
