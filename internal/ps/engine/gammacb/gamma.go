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
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/engine/sdk/go/gamma"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/atomic"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine"
	"github.com/vearch/vearch/v3/internal/ps/engine/mapping"
)

var _ engine.Engine = &gammaEngine{}

var indexLocker sync.Mutex

type EngineConfig struct {
	// Path is the data directory.
	Path string
	// ExtraOptions contains extension options using a json format ("{key1:value1,key2:value2}").
	ExtraOptions map[string]interface{}
	// Schema
	Space *entity.Space
	// partitionID
	PartitionID entity.PartitionID
}

func Build(cfg EngineConfig) (e engine.Engine, err error) {
	e, err = New(cfg)
	return e, err
}

func New(cfg EngineConfig) (engine.Engine, error) {
	// init schema make mapping begin
	indexMapping, err := mapping.Space2Mapping(cfg.Space)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	table, e := mapping2Table(cfg, indexMapping)
	if e != nil {
		return nil, e
	}

	config := struct {
		Path      string `json:"path"`
		SpaceName string `json:"space_name"`
		LogDir    string `json:"log_dir"`
	}{
		Path:      cfg.Path,
		SpaceName: cfg.Space.Name + "-" + cast.ToString(cfg.PartitionID),
		LogDir:    config.Conf().GetLogDir(),
	}

	configJson, _ := json.Marshal(&config)
	engineInstance := gamma.Init(configJson)
	if engineInstance == nil {
		log.Error("gamma engine init err [%s]", config.SpaceName)
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("init engine err"))
	}

	ge := &gammaEngine{
		ctx:          ctx,
		cancel:       cancel,
		indexMapping: indexMapping,
		space:        cfg.Space,
		partitionID:  cfg.PartitionID,
		path:         cfg.Path,
		gamma:        engineInstance,
		counter:      atomic.NewAtomicInt64(0),
		hasClosed:    false,
	}
	ge.reader = &readerImpl{engine: ge}
	ge.writer = &writerImpl{engine: ge}

	infos, _ := os.ReadDir(cfg.Path)

	startTime := time.Now()
	if status := gamma.CreateTable(ge.gamma, table); status.Code != 0 {
		log.Error("create table [%s] err [%s] cost time: [%v]", config.SpaceName, status.Msg, time.Since(startTime).Seconds())
		ge.Close()
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("create engine table err:[%s]", status.Msg))
	}

	gammaDirs := make([]string, 0)
	for _, info := range infos {
		gammaDirs = append(gammaDirs, info.Name())
	}

	log.Info("create table finish by path:[%s], table: %s cost: [%v]s, files [%v]", cfg.Path, cfg.Space.Name, time.Since(startTime).Seconds(), gammaDirs)

	if len(infos) > 0 {
		code := gamma.Load(ge.gamma)
		if code != 0 {
			log.Error("load data err code:[%d]", code)
			ge.Close()
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("load data err code:[%d]", code))
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
	var oldProperties, newProperties any

	if err := json.Unmarshal([]byte(ge.space.Fields), &oldProperties); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal old space properties:[%s] has err:[%s] ", ge.space.Fields, err.Error()))
	}

	if err := json.Unmarshal([]byte(space.Fields), &newProperties); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal new space properties:[%s] has err :[%s]", space.Fields, err.Error()))
	}

	if !reflect.DeepEqual(oldProperties, newProperties) {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("gamma engine not support "))
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
		if e := ge.RebuildIndex(drop_before_rebuild, limit_cpu, describe); e != nil {
			log.Error("RebuildIndex index:[%d] has err %v", ge.partitionID, e.Error())
			return
		}
	}()
	return nil
}

func (ge *gammaEngine) IndexInfo() (int, int, int) {
	status := &entity.EngineStatus{}
	if err := ge.GetEngineStatus(status); err != nil {
		return 0, 0, 0
	}
	return int(status.IndexStatus), int(status.MinIndexedNum), int(status.MaxDocid)
}

func (ge *gammaEngine) GetEngineStatus(status *entity.EngineStatus) error {
	enginePtr, err := ge.getEnginePtr()
	if err != nil {
		return err
	}

	ges := gamma.GetEngineStatus(enginePtr)
	if err := json.Unmarshal([]byte(ges), status); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
			fmt.Errorf("unmarshal engine status failed: %v", err))
	}
	return nil
}

func (ge *gammaEngine) BuildIndex() error {
	ge.counter.Incr()
	defer ge.counter.Decr()
	gammaEngine := ge.gamma
	if gammaEngine == nil || ge.hasClosed {
		log.Error("gammaEngine is nil or closed, partition:[%d]", ge.partitionID)
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil)
	}

	indexLocker.Lock()
	defer indexLocker.Unlock()

	// UNINDEXED = 0, INDEXING, INDEXED
	go func() {
		startTime := time.Now()
		if rc := gamma.BuildIndex(gammaEngine); rc != 0 {
			log.Error("build index:[%d] err response code:[%d]", ge.partitionID, rc)
		} else {
			log.Info("BuildIndex partition:[%d] cost:[%.2f]ms, rc:[%d]",
				ge.partitionID, time.Since(startTime).Seconds()*1000, rc)
		}
	}()

	return nil
}

func (ge *gammaEngine) RebuildIndex(drop_before_rebuild int, limit_cpu int, describe int) error {
	ge.counter.Incr()
	defer ge.counter.Decr()

	if ge.gamma == nil {
		log.Error("gammaEngine is nil, partition:[%d]", ge.partitionID)
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, nil)
	}

	indexLocker.Lock()
	defer indexLocker.Unlock()

	// UNINDEXED = 0, INDEXING, INDEXED
	go func() {
		startTime := time.Now()
		if rc := gamma.RebuildIndex(ge.gamma, drop_before_rebuild, limit_cpu, describe); rc != 0 {
			log.Error("RebuildIndex partition:[%d] err response code:[%d]", ge.partitionID, rc)
		} else {
			log.Info("RebuildIndex partition:[%d] cost:[%.2f]ms, ret:[%d]",
				ge.partitionID, time.Since(startTime).Seconds()*1000, rc)
		}
	}()

	return nil
}

func (ge *gammaEngine) Load() error {
	indexLocker.Lock()
	defer indexLocker.Unlock()
	ge.counter.Incr()
	defer ge.counter.Decr()
	cfg := EngineConfig{
		Path:        ge.path,
		Space:       ge.space,
		PartitionID: ge.partitionID,
	}
	config := struct {
		Path      string `json:"path"`
		SpaceName string `json:"space_name"`
		LogDir    string `json:"log_dir"`
	}{
		Path:      cfg.Path,
		SpaceName: cfg.Space.Name + "-" + cast.ToString(cfg.PartitionID),
		LogDir:    config.Conf().GetLogDir(),
	}

	configJson, _ := json.Marshal(&config)
	engineInstance := gamma.Init(configJson)
	if engineInstance == nil {
		log.Error("gamma engine init err [%s]", config.SpaceName)
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("init engine err"))
	}
	code := gamma.Load(engineInstance)
	if code != 0 {
		log.Error("load data err code:[%d]", code)
		ge.Close()
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("load data err code:[%d]", code))
	}
	ge.gamma = engineInstance
	ge.hasClosed = false
	return nil
}

func (ge *gammaEngine) HasClosed() bool {
	return ge.hasClosed
}

func (ge *gammaEngine) Close() {
	enginePtr := ge.gamma
	ge.gamma = nil
	ge.cancel()
	go func(enginePtr unsafe.Pointer) {
		if enginePtr == nil {
			log.Warn("gamma engine already closed, partition:[%d]", ge.partitionID)
			return
		}

		waitCount := 0
		for ge.counter.Get() > 0 {
			waitCount++
			time.Sleep(100 * time.Millisecond)
			if waitCount%30 == 0 {
				log.Info("waiting for operations to complete, partition:[%d], count:[%d], wait times:[%d]",
					ge.partitionID, ge.counter.Get(), waitCount)
			}
		}

		start := time.Now()
		log.Info("closing gamma engine partition:[%d] begin", ge.partitionID)

		if resp := gamma.Close(enginePtr); resp != 0 {
			log.Error("close gamma engine partition:[%d] failed:[%d]", ge.partitionID, resp)
		} else {
			log.Info("close gamma engine partition:[%d] success", ge.partitionID)
		}

		ge.hasClosed = true
		log.Info("close gamma engine partition:[%d] end cost:[%v]", ge.partitionID, time.Since(start))
	}(enginePtr)
}

func (ge *gammaEngine) SetEngineCfg(configJson []byte) error {
	enginePtr, err := ge.getEnginePtr()
	if err != nil {
		return err
	}
	gamma.SetEngineCfg(enginePtr, configJson)
	return nil
}

func (ge *gammaEngine) GetEngineCfg(config *entity.SpaceConfig) error {
	enginePtr, err := ge.getEnginePtr()
	if err != nil {
		return err
	}
	configJson := gamma.GetEngineCfg(enginePtr)
	return json.Unmarshal(configJson, config)
}

func (ge *gammaEngine) BackupSpace(command string) error {
	enginePtr, err := ge.getEnginePtr()
	if err != nil {
		return err
	}
	status := gamma.BackupSpace(enginePtr, command)
	if status.Code != 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("backup space err:[%s]", status.Msg))
	}
	return nil
}

func (ge *gammaEngine) getEnginePtr() (unsafe.Pointer, error) {
	ge.lock.RLock()
	defer ge.lock.RUnlock()

	if ge.hasClosed {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED,
			fmt.Errorf("engine is closed, partition:[%d]", ge.partitionID))
	}

	if ge.gamma == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
			fmt.Errorf("engine pointer is nil, partition:[%d]", ge.partitionID))
	}

	return ge.gamma, nil
}
