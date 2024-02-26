// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package gammacb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/internal/engine/sdk/go/gamma"
	"github.com/vearch/vearch/internal/ps/engine"
	"github.com/vearch/vearch/internal/ps/engine/mapping"
	"github.com/vearch/vearch/internal/ps/engine/register"
	"github.com/vearch/vearch/internal/util/atomic"
	"github.com/vearch/vearch/internal/util/log"
	"github.com/vearch/vearch/internal/util/uuid"
	"github.com/vearch/vearch/internal/util/vearchlog"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
)

var _ engine.Engine = &gammaEngine{}

var indexLocker sync.Mutex

func init() {
	register.Register("gamma", New)
}

func New(cfg register.EngineConfig) (engine.Engine, error) {
	// init schema make mapping begin
	indexMapping, err := mapping.Space2Mapping(cfg.Space)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	table, e := mapping2Table(cfg, indexMapping)
	if e != nil {
		cancel()
		return nil, e
	}

	config := &gamma.Config{
		Path:      cfg.Path,
		SpaceName: cfg.Space.Name + "-" + cast.ToString(cfg.PartitionID),
		LogDir:    config.Conf().GetLogDir()}

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

	infos, _ := os.ReadDir(cfg.Path)

	startTime := time.Now()
	if resp := gamma.CreateTable(ge.gamma, table); resp != 0 {
		log.Error("create table[%s] for gamma has err [%d] cost time: [%v]", cfg.Space.Name, resp, time.Since(startTime).Seconds())
		ge.Close()
		return nil, fmt.Errorf("create gamma table has err:[%d]", resp)
	}

	gammaDirs := make([]string, 0)
	for _, info := range infos {
		gammaDirs = append(gammaDirs, info.Name())
	}

	log.Info("create table for gamma finish by path:[%s], table: %s cost: [%v]s, files [%v]", cfg.Path, cfg.Space.Name, time.Since(startTime).Seconds(), gammaDirs)

	if len(infos) > 0 {
		code := gamma.Load(ge.gamma)
		if code != 0 {
			vearchlog.LogErrNotNil(fmt.Errorf("load gamma data err code:[%d]", code))
			ge.Close()
		}
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

	counter   *atomic.AtomicInt64
	lock      sync.RWMutex
	hasClosed bool
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
	go func() {
		log.Info("build index:[%d] begin", ge.partitionID)
		if e := ge.BuildIndex(); e != nil {
			log.Error("build index:[%d] has err %v", ge.partitionID, e.Error())
			return
		}
		log.Info("build index:[%d] end", ge.partitionID)
	}()
	return nil
}

func (ge *gammaEngine) Rebuild(drop_before_rebuild int, limit_cpu int, describe int) error {
	go func() {
		log.Info("RebuildIndex index:[%d] begin", ge.partitionID)
		if e := ge.RebuildIndex(drop_before_rebuild, limit_cpu, describe); e != nil {
			log.Error("RebuildIndex index:[%d] has err %v", ge.partitionID, e.Error())
			return
		}
		log.Info("RebuildIndex index:[%d] end", ge.partitionID)
	}()
	return nil
}

func (ge *gammaEngine) IndexInfo() (int, int, int) {
	var status gamma.EngineStatus
	gamma.GetEngineStatus(ge.gamma, &status)
	return int(status.IndexStatus), int(status.MinIndexedNum), int(status.MaxDocid)
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

	// UNINDEXED = 0, INDEXING, INDEXED
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

func (ge *gammaEngine) RebuildIndex(drop_before_rebuild int, limit_cpu int, describe int) error {
	indexLocker.Lock()
	defer indexLocker.Unlock()
	ge.counter.Incr()
	defer ge.counter.Decr()
	gammaEngine := ge.gamma
	if gammaEngine == nil {
		return vearchlog.LogErrAndReturn(vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil))
	}

	// UNINDEXED = 0, INDEXING, INDEXED
	go func() {
		startTime := time.Now()
		if rc := gamma.RebuildIndex(gammaEngine, drop_before_rebuild, limit_cpu, describe); rc != 0 {
			log.Error("RebuildIndex:[%d] err response code:[%d]", ge.partitionID, rc)
		} else {
			endTime := time.Now()
			log.Info("RebuildIndex cost:[%f],rc :[%d]", (endTime.Sub(startTime).Seconds())*1000, rc)
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
			log.Info("to close gamma engine end token:[%s] use time:[%d]", flakeUUID, time.Since(start))
			break
		}
	}(closeEngine)
}

func (ge *gammaEngine) SetEngineCfg(config *gamma.Config) error {
	gamma.SetEngineCfg(ge.gamma, config)
	return nil
}

func (ge *gammaEngine) GetEngineCfg(config *gamma.Config) error {
	gamma.GetEngineCfg(ge.gamma, config)
	return nil
}
