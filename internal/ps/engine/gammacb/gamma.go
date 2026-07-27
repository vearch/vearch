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
	"runtime"
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
	ExtraOptions map[string]any
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
	ge.lock.RLock()
	defer ge.lock.RUnlock()
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

func (ge *gammaEngine) UpdateMapping(updatedSpace *entity.Space) error {
	currentSpaceProperties, err := entity.UnmarshalPropertyJSON(ge.space.Fields)
	if err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal current space properties:[%s] has err:[%s]", ge.space.Fields, err.Error()))
	}
	log.Debug("current space properties: %v", currentSpaceProperties)
	updatedSpaceProperties, err := entity.UnmarshalPropertyJSON(updatedSpace.Fields)
	if err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("unmarshal updated space properties:[%s] has err:[%s]", updatedSpace.Fields, err.Error()))
	}
	log.Debug("updated space properties: %v", updatedSpaceProperties)
	for fieldName, currentProperty := range currentSpaceProperties {
		updatedProperty, exists := updatedSpaceProperties[fieldName]
		if !exists {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("field removal not supported for field:[%s]", fieldName))
		}
		if currentProperty.FieldType != updatedProperty.FieldType {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("field type change not supported for field:[%s]", fieldName))
		}
	}
	for fieldName := range updatedSpaceProperties {
		if _, exists := currentSpaceProperties[fieldName]; !exists {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("field addition not supported for field:[%s]", fieldName))
		}
	}

	// Index add/remove no longer happens here: it arrives as an explicit
	// INDEXCHANGE raft command handled by AddIndexes/RemoveIndex. UpdateMapping
	// now only validates field-level schema and refreshes the local snapshot.
	newIndexMapping, err := mapping.Space2Mapping(updatedSpace)
	if err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to create new index mapping: %v", err))
	}
	ge.lock.Lock()
	ge.indexMapping = newIndexMapping
	ge.space = updatedSpace
	ge.lock.Unlock()
	return nil
}

// AddIndexes applies an explicit "add these indexes" instruction (from a raft
// INDEXCHANGE command). Each index is built by the engine; the local space
// snapshot is updated so describe and a later restart stay consistent.
func (ge *gammaEngine) AddIndexes(indexes []*entity.Index) error {
	for _, idx := range indexes {
		if err := ge.addFieldIndex(idx); err != nil {
			log.Error("failed to add index name:[%s] in space:[%s], partition:[%d]: %v",
				indexName(idx), ge.space.Name, ge.partitionID, err)
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
				fmt.Errorf("failed to add index %s: %v", indexName(idx), err))
		}
		log.Info("successfully added index name:[%s] in space:[%s], partition:[%d]",
			indexName(idx), ge.space.Name, ge.partitionID)
	}
	ge.appendSpaceIndexes(indexes)
	return nil
}

// RemoveIndex applies an explicit "remove this index" instruction.
func (ge *gammaEngine) RemoveIndex(indexName string) error {
	if err := ge.removeFieldIndex(indexName); err != nil {
		log.Error("failed to remove index name:[%s] in space:[%s], partition:[%d]: %v",
			indexName, ge.space.Name, ge.partitionID, err)
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
			fmt.Errorf("failed to remove index %s: %v", indexName, err))
	}
	log.Info("successfully removed index name:[%s] in space:[%s], partition:[%d]",
		indexName, ge.space.Name, ge.partitionID)
	ge.removeSpaceIndex(indexName)
	return nil
}

// indexName returns a nil-safe display name for logging.
func indexName(idx *entity.Index) string {
	if idx == nil {
		return ""
	}
	return idx.Name
}

// appendSpaceIndexes merges added indexes into the local space snapshot under
// ge.lock so GetSpace/GetMapping readers and a restart see the current set.
func (ge *gammaEngine) appendSpaceIndexes(indexes []*entity.Index) {
	ge.lock.Lock()
	defer ge.lock.Unlock()
	have := make(map[string]struct{}, len(ge.space.Indexes))
	for _, idx := range ge.space.Indexes {
		if idx != nil {
			have[idx.Name] = struct{}{}
		}
	}
	for _, idx := range indexes {
		if idx == nil || idx.Name == "" {
			continue
		}
		if _, ok := have[idx.Name]; ok {
			continue
		}
		ge.space.Indexes = append(ge.space.Indexes, idx)
		have[idx.Name] = struct{}{}
	}
}

// removeSpaceIndex drops the named index from the local space snapshot.
func (ge *gammaEngine) removeSpaceIndex(name string) {
	ge.lock.Lock()
	defer ge.lock.Unlock()
	kept := ge.space.Indexes[:0]
	for _, idx := range ge.space.Indexes {
		if idx != nil && idx.Name == name {
			continue
		}
		kept = append(kept, idx)
	}
	ge.space.Indexes = kept
}

func (ge *gammaEngine) GetMapping() *mapping.IndexMapping {
	ge.lock.RLock()
	defer ge.lock.RUnlock()
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
			log.Error("rebuild index:[%d] has err %v", ge.partitionID, e.Error())
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
			log.Info("build index:[%d] cost:[%.2f]ms, rc:[%d]",
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
		ge.counter.Incr()
		defer ge.counter.Decr()
		startTime := time.Now()
		if rc := gamma.RebuildIndex(ge.gamma, drop_before_rebuild, limit_cpu, describe); rc != 0 {
			log.Error("rebuild index partition:[%d] err response code:[%d]", ge.partitionID, rc)
		} else {
			log.Info("rebuild index partition:[%d] cost:[%.2f]ms, ret:[%d]",
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
	// Print stack trace to show where Close is called from
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	log.Info("Close called for partition:[%d], stack trace:\n%s", ge.partitionID, string(buf[:n]))

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

func (ge *gammaEngine) addFieldIndex(idx *entity.Index) error {
	enginePtr, err := ge.getEnginePtr()
	if err != nil {
		return err
	}
	if idx == nil || idx.Name == "" {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
			fmt.Errorf("addFieldIndex: index or index.Name is empty"))
	}

	fieldNames := idx.FieldNames
	if len(fieldNames) == 0 && idx.FieldName != "" {
		fieldNames = []string{idx.FieldName}
	}
	if len(fieldNames) == 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
			fmt.Errorf("addFieldIndex: index [%s] has no fields", idx.Name))
	}

	indexType := idx.Type
	indexParams := idx.Params
	if indexType == "" {
		// Best-effort default for older specs that omit the type.
		indexType = "SCALAR"
	}

	log.Info("adding index name:[%s] type:[%s] fields:%v params:[%s] partition:[%d]",
		idx.Name, indexType, fieldNames, string(indexParams), ge.partitionID)

	status := gamma.AddFieldIndexWithParams(enginePtr, idx.Name, fieldNames, indexType, indexParams)
	if status.Code != 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
			fmt.Errorf("add field index failed: %s", status.Msg))
	}

	log.Info("successfully added index name:[%s] in partition:[%d]", idx.Name, ge.partitionID)
	return nil
}

func (ge *gammaEngine) removeFieldIndex(indexName string) error {
	enginePtr, err := ge.getEnginePtr()
	if err != nil {
		return err
	}
	if indexName == "" {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
			fmt.Errorf("removeFieldIndex: indexName is empty"))
	}

	status := gamma.RemoveFieldIndex(enginePtr, indexName)
	if status.Code != 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
			fmt.Errorf("remove field index failed: %s", status.Msg))
	}

	log.Info("successfully removed index name:[%s] in partition:[%d]", indexName, ge.partitionID)
	return nil
}
