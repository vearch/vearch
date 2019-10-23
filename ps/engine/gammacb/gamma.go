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
	"github.com/vearch/vearch/config"
	pkg "github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/ps/engine/mapping"
	"github.com/vearch/vearch/ps/engine/register"
	"github.com/vearch/vearch/util/atomic"
	"io/ioutil"
	"sync"
	"time"
	"unsafe"
)

const Name = "gamma"

var _ engine.Engine = &gammaEngine{}

var indexLocker sync.Mutex

func init() {
	register.Register(Name, New)
}

var logInitOnce sync.Once

func New(cfg register.EngineConfig) (engine.Engine, error) {

	//set log dir
	logInitOnce.Do(func() {
		if rep := C.SetLogDictionary(byteArrayStr(config.Conf().GetLogDir(config.PS))); rep != 0 {
			log.Error("init gamma log has err")
		}
	})

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

	defer C.DestroyFieldInfos(table.fields, table.fields_num)
	defer C.DestroyVectorInfos(table.vectors_info, table.vectors_num)

	gammaConfig := C.MakeConfig(byteArrayStr(cfg.Path), C.int(cfg.Space.Engine.MaxSize))
	defer C.DestroyConfig(gammaConfig)
	ge := &gammaEngine{
		ctx:          ctx,
		cancel:       cancel,
		indexMapping: indexMapping,
		space:        cfg.Space,
		partitionID:  cfg.PartitionID,
		path:         cfg.Path,
		gamma:        C.Init(gammaConfig),
		counter:      atomic.NewAtomicInt64(0),
	}
	ge.reader = &readerImpl{engine: ge}
	ge.writer = &writerImpl{engine: ge}

	infos, _ := ioutil.ReadDir(cfg.Path)
	if len(infos) == 0 {
		log.Info("to create table for gamma by path:[%s]", cfg.Path)
		if resp := C.CreateTable(ge.gamma, table); resp != 0 {
			return nil, fmt.Errorf("create gamma table has err:[%d]", int(resp))
		}
	} else {
		code := int(C.Load(ge.gamma))
		if code != 0 {
			return nil, fmt.Errorf("load gamma data err code:[%d]", code)
		}
	}

	go ge.autoCreateIndex()

	if log.IsDebugEnabled() {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				log.Debug("gamma use memory is:[%d]", C.GetMemoryBytes(ge.gamma))
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

func (ge *gammaEngine) RTReader() engine.RTReader {
	return ge.reader
}

func (ge *gammaEngine) Writer() engine.Writer {
	return ge.writer
}

func (ge *gammaEngine) UpdateMapping(space *entity.Space) error {
	return fmt.Errorf("not support update mapping in gamma")
}

func (ge *gammaEngine) GetMapping() *mapping.IndexMapping {
	return ge.indexMapping
}

func (ge *gammaEngine) MapDocument(doc *pspb.DocCmd) ([]*pspb.Field, map[string]pspb.FieldType, error) {
	return ge.indexMapping.MapDocument(doc.Source)
}

func (ge *gammaEngine) Optimize() error {
	go func() {
		ge.buildIndexOnce.Do(func() {
			log.Info("build index:[%d] begin", ge.partitionID)
			if e1 := ge.BuildIndex(); e1 != nil {
				log.Error("build index:[%d] has err ", ge.partitionID, e1.Error())
			}
			log.Info("build index:[%d] end", ge.partitionID)
		})
	}()
	return nil
}

func (ge *gammaEngine) BuildIndex() error {
	indexLocker.Lock()
	defer indexLocker.Unlock()
	ge.counter.Incr()
	defer ge.counter.Decr()
	gamma := ge.gamma
	if gamma == nil {
		return pkg.ErrPartitionClosed
	}

	//UNINDEXED = 0, INDEXING, INDEXED
	go func() {
		rc := C.BuildIndex(gamma)
		if rc != 0 {
			log.Error("build index:[%d] err response code:[%d]", ge.partitionID, rc)
		}
	}()
	for {
		select {
		case <-ge.ctx.Done():
			log.Error("partition:[%d] has closed so skip wait", ge.partitionID)
			return pkg.ErrPartitionClosed
		default:
		}

		s := C.GetIndexStatus(gamma)
		log.Info("index:[%d] status is %d", ge.partitionID, int(s))

		if int(s) == 2 {
			log.Info("index:[%d] ok", ge.partitionID)
			break
		}

		time.Sleep(3 * time.Second)
	}

	return nil
}

func (ge *gammaEngine) Close() {
	ge.gamma = nil
	ge.cancel()
	go func() {
		i := 0
		for {
			time.Sleep(3 * time.Second)
			i++
			if ge.counter.Get() > 0 {
				log.Info("wait stop gamma engine times:[%d]", i)
				continue
			}
			C.Close(ge.gamma)
		}
	}()

}

func (ge *gammaEngine) autoCreateIndex() {

	if ge.space.Engine.IndexSize <= 0 {
		return
	}

	for {
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
