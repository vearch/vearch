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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/ps/engine/gamma"
	"github.com/vearch/vearch/util/ioutil2"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/vearchlog"
)

var _ engine.Writer = &writerImpl{}

type writerImpl struct {
	engine  *gammaEngine
	running bool
}

func (wi *writerImpl) Write(ctx context.Context, doc *vearchpb.DocCmd) (err error) {
	if doc == nil {
		return errors.New("doc is nil")
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("[Rocover] [%s]", cast.ToString(r))
		}
	}()
	wi.engine.counter.Incr()
	defer wi.engine.counter.Decr()

	gammaEngine := wi.engine.gamma
	if gammaEngine == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil)
	}

	switch doc.Type {
	case vearchpb.OpType_BULK:
		resp := gamma.AddOrUpdateDocs(gammaEngine, doc.Docs)
		var buffer bytes.Buffer
		for _, code := range resp.Codes {
			buffer.WriteString(strconv.Itoa(int(code)) + ",")
		}
		err := errors.New(buffer.String())
		return vearchpb.NewError(vearchpb.ErrorEnum_SUCCESS, err)
	case vearchpb.OpType_REPLACE:
		if resp := gamma.AddOrUpdateDoc(gammaEngine, doc.Doc); resp != 0 {
			err = fmt.Errorf("gamma create doc err code:[%d]", int(resp))
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err)
		}
	case vearchpb.OpType_DELETE:
		if resp := gamma.DeleteDoc(gammaEngine, doc.Doc); resp != 0 {
			if resp == -1 {
				return vearchpb.NewError(vearchpb.ErrorEnum_DOCUMENT_NOT_EXIST, nil)
			}
			err = fmt.Errorf("gamma delete doc err code:[%d]", int(resp))
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err)
		}
	default:
		msg := fmt.Sprintf("type: [%v] not found", doc.Type)
		err = vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, errors.New(msg))
	}
	return
}

func (wi *writerImpl) Flush(ctx context.Context, sn int64) error {
	wi.engine.counter.Incr()
	defer wi.engine.counter.Decr()

	gammaEngine := wi.engine.gamma
	if gammaEngine == nil {
		return vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil))
	}

	wi.engine.lock.Lock()
	defer wi.engine.lock.Unlock()
	//if code := C.Dump(gamma); code != 0 {
	if code := gamma.Dump(gammaEngine); code != 0 {
		return fmt.Errorf("dump index err response code :[%d]", code)
	}

	fileName := filepath.Join(wi.engine.path, indexSn)
	err := ioutil2.WriteFileAtomic(fileName, []byte(string(strconv.FormatInt(sn, 10))), os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func (wi *writerImpl) Commit(ctx context.Context, snx int64) (chan error, error) {
	wi.engine.counter.Incr()
	defer wi.engine.counter.Decr()

	gammaEngine := wi.engine.gamma
	if gammaEngine == nil {
		return nil, vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil))
	}

	flushC := make(chan error, 1)

	go func(fc chan error, sn int64) {

		wi.engine.lock.Lock()
		if wi.running {
			log.Info("commit is running , so skip this request")
			fc <- nil
			return
		}
		wi.running = true
		wi.engine.lock.Unlock()

		defer func() {
			wi.engine.lock.Lock()
			wi.running = false
			wi.engine.lock.Unlock()

			if i := recover(); i != nil {
				log.Error(string(debug.Stack()))
				log.Error(cast.ToString(i))
			}
		}()

		log.Info("begin dump data for gamma")

		//if code := C.Dump(gamma); code != 0 {
		if code := gamma.Dump(gammaEngine); code != 0 {
			fc <- vearchlog.LogErrAndReturn(fmt.Errorf("dump index err response code :[%d]", code))
		} else {
			fileName := filepath.Join(wi.engine.path, indexSn)
			err := ioutil2.WriteFileAtomic(fileName, []byte(string(strconv.FormatInt(sn, 10))), os.ModePerm)
			if err != nil {
				return
			}
			fc <- nil
		}

		log.Info("end dump data for gamma")
	}(flushC, snx)

	return flushC, nil
}
