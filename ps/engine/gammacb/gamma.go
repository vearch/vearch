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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/ps/engine/gamma"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/ps/engine/register"
	"github.com/vearch/vearch/util/atomic"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/uuid"
	"github.com/vearch/vearch/util/vearchlog"
)

var _ engine.Engine = &gammaEngine{}

var indexLocker sync.Mutex

func init() {
	register.Register("gamma", New)
}

var logInitOnce sync.Once

func New(cfg register.EngineConfig) (engine.Engine, error) {

	//set log dir
	/*logInitOnce.Do(func() {
		if rep := C.SetLogDictionary(byteArrayStr(config.Conf().GetLogDir(config.PS))); rep != 0 {
			log.Error("init gamma log has err")
		}
	})*/

	// init schema make mapping begin
	indexMapping, err := mapping.Space2Mapping(cfg.Space)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	table, e := mapping2Table(cfg, indexMapping)
	if e != nil {
		return nil, e
	}

	config := &gamma.Config{Path: cfg.Path, LogDir: config.Conf().GetLogDir()}
	ge := &gammaEngine{
		ctx:          ctx,
		cancel:       cancel,
		indexMapping: indexMapping,
		space:        cfg.Space,
		partitionID:  cfg.PartitionID,
		path:         cfg.Path,
		gamma:        gamma.Init(config),
		counter:      atomic.NewAtomicInt64(0),
		hasClosed:    false,
	}
	ge.reader = &readerImpl{engine: ge}
	ge.writer = &writerImpl{engine: ge}

	infos, _ := ioutil.ReadDir(cfg.Path)

	startTime := time.Now()
	log.Info("to create table for gamma by path:[%s]", cfg.Path)
	if resp := gamma.CreateTable(ge.gamma, table); resp != 0 {
		endTime := time.Now()
		log.Error("create table[%s] for gamma has err [%d] cost time :%v", cfg.Space.Name, resp, (endTime.Sub(startTime).Seconds())*1000)
		ge.Close()
		return nil, fmt.Errorf("create gamma table has err:[%d]", resp)
	} else {
		log.Info("to create table for gamma finish by path:[%s]", cfg.Path)
	}

	endTime := time.Now()
	log.Info("creat table: %s cost time :%v", cfg.Space.Name, (endTime.Sub(startTime).Seconds())*1000)
	if len(infos) > 0 {
		code := gamma.Load(ge.gamma)
		if code != 0 {
			vearchlog.LogErrNotNil(fmt.Errorf("load gamma data err code:[%d]", code))
			ge.Close()
		}
	}

	if log.IsDebugEnabled() {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				//log.Debug("gamma use memory is:[%d]", C.GetMemoryBytes(ge.gamma))
				var status gamma.EngineStatus
				gamma.GetEngineStatus(ge.gamma, &status)
				log.Debug("gamma use memory is:[%d]",
					status.BitmapMem+status.FieldRangeMem+status.TableMem+status.VectorMem+status.IndexMem)
				log.Debug("gamma memory usage: bitmap %d, range %d, table %d, vector %d, vector index %d",
					status.BitmapMem, status.FieldRangeMem, status.TableMem, status.VectorMem, status.IndexMem)
				time.Sleep(10 * time.Second)
			}
		}()
	}

	return ge, nil
}

type gammaEngine struct {
	ctx          context.Context
	cancel       context.CancelFunc
	path         string
	indexMapping *mapping.IndexMapping
	space        *entity.Space
	partitionID  entity.PartitionID

	gamma  unsafe.Pointer
	reader *readerImpl
	writer *writerImpl

	buildIndexOnce sync.Once
	counter        *atomic.AtomicInt64
	lock           sync.RWMutex
	hasClosed      bool
}

func (ge *gammaEngine) GetSpace() *entity.Space {
	return ge.space
}

func (ge *gammaEngine) GetPartitionID() entity.PartitionID {
	return ge.partitionID
}

func (ge *gammaEngine) Reader() engine.Reader {
	return ge.reader
}

func (ge *gammaEngine) Writer() engine.Writer {
	return ge.writer
}

func (ge *gammaEngine) UpdateMapping(space *entity.Space) error {
	var oldProperties, newProperties interface{}

	if err := json.Unmarshal([]byte(ge.space.Properties), &oldProperties); err != nil {
		return fmt.Errorf("unmarshal old space properties:[%s] has err:[%s] ", ge.space.Properties, err.Error())
	}

	if err := json.Unmarshal([]byte(space.Properties), &newProperties); err != nil {
		return fmt.Errorf("unmarshal new space properties:[%s] has err :[%s]", space.Properties, err.Error())
	}

	if !reflect.DeepEqual(oldProperties, newProperties) {
		return fmt.Errorf("gamma engine not support ")
	}

	ge.space = space

	return nil
}

func (ge *gammaEngine) GetMapping() *mapping.IndexMapping {
	return ge.indexMapping
}

func (ge *gammaEngine) Optimize() error {
	/*if _, err := ge.reader.DocCount(ge.ctx); err != nil {
		return err
	} else if int64(u) < 8192 {
		return fmt.Errorf("doc size:[%d] less than 8192 so can not to index", int64(u))
	}*/
	go func() {
		log.Info("build index:[%d] begin", ge.partitionID)
		if e1 := ge.BuildIndex(); e1 != nil {
			log.Error("build index:[%d] has err ", ge.partitionID, e1.Error())
		}
		log.Info("build index:[%d] end", ge.partitionID)
	}()
	return nil
}

func (ge *gammaEngine) IndexInfo() (int, int) {
	var status gamma.EngineStatus
	gamma.GetEngineStatus(ge.gamma, &status)
	return int(status.IndexStatus), int(status.MinIndexedNum)
}

func (ge *gammaEngine) EngineStatus(status *engine.EngineStatus) error {
	var ges gamma.EngineStatus
	gamma.GetEngineStatus(ge.gamma, &ges)
	status.IndexStatus = ges.IndexStatus
	status.DocNum = ges.DocNum
	status.MaxDocid = ges.MaxDocid
	status.MinIndexedNum = ges.MinIndexedNum
	return nil
}

func (ge *gammaEngine) BuildIndex() error {
	indexLocker.Lock()
	defer indexLocker.Unlock()
	ge.counter.Incr()
	defer ge.counter.Decr()
	gammaEngine := ge.gamma
	if gammaEngine == nil {
		return vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil))
	}

	//UNINDEXED = 0, INDEXING, INDEXED
	go func() {
		startTime := time.Now()
		if rc := gamma.BuildIndex(gammaEngine); rc != 0 {
			log.Error("build index:[%d] err response code:[%d]", ge.partitionID, rc)
		} else {
			endTime := time.Now()
			log.Info("BuildIndex cost:[%f],rc :[%d]", (endTime.Sub(startTime).Seconds())*1000, rc)
		}
	}()

	return nil
}

func (ge *gammaEngine) HasClosed() bool {
	return ge.hasClosed
}

func (ge *gammaEngine) Close() {
	closeEngine := ge.gamma
	ge.gamma = nil
	ge.cancel()
	go func(closeEngine unsafe.Pointer) {
		i := 0
		for {
			time.Sleep(3 * time.Second)
			i++
			if ge.counter.Get() > 0 {
				log.Info("wait stop gamma engine times:[%d]", i)
				continue
			}
			start, flakeUUID := time.Now(), uuid.FlakeUUID()
			log.Info("to close gamma engine begin token:[%s]", flakeUUID)
			if resp := gamma.Close(closeEngine); resp != 0 {
				log.Error("to close gamma engine fail:[%d]", resp)
			} else {
				log.Info("to close gamma engine success:[%d]", resp)
			}
			ge.hasClosed = true
			log.Info("to close gamma engine end token:[%s] use time:[%d]", flakeUUID, time.Now().Sub(start))
			break
		}
	}(closeEngine)

}

func (ge *gammaEngine) autoCreateIndex() {

	if ge.space.Engine.IndexSize <= 0 {
		return
	}

	for {
		select {
		case <-ge.ctx.Done():
			return
		default:
		}

		//s := C.GetIndexStatus(ge.gamma)
		var status gamma.EngineStatus
		gamma.GetEngineStatus(ge.gamma, &status)
		s := status.IndexStatus
		if int(s) == 2 {
			log.Info("index:[%d] ok", ge.partitionID)
			break
		}

		if u, err := ge.reader.DocCount(ge.ctx); err != nil {
			log.Error("auto create index err :[%s]", err.Error())
		} else if int64(u) >= ge.space.Engine.IndexSize {
			if err := ge.Optimize(); err != nil {
				log.Error("auto create index err :[%s]", err.Error())
			}
			break
		}
		time.Sleep(1 * time.Second)
	}

}

func (ge *gammaEngine) SetEngineCfg(config *gamma.Config) error {
	gamma.SetEngineCfg(ge.gamma, config)
	return nil
}

func (ge *gammaEngine) GetEngineCfg(config *gamma.Config) error {
	gamma.GetEngineCfg(ge.gamma, config)
	return nil
}
