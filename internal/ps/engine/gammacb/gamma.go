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
	// Parse current and updated space fields using SpaceProperties
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

	// Check for field changes and index option changes
	needIndexRebuild := false
	var indexChanges []string

	// Check existing fields for changes
	for fieldName, currentProperty := range currentSpaceProperties {
		if updatedProperty, exists := updatedSpaceProperties[fieldName]; exists {
			// Compare field types and basic properties
			if currentProperty.FieldType != updatedProperty.FieldType {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
					fmt.Errorf("field type change not supported for field:[%s]", fieldName))
			}

			// Check for index option changes
			currentIsIndexed := (currentProperty.Option & vearchpb.FieldOption_Index) != 0
			updatedIsIndexed := (updatedProperty.Option & vearchpb.FieldOption_Index) != 0
			log.Debug("field:[%s] current indexed:[%t], updated indexed:[%t]", fieldName, currentIsIndexed, updatedIsIndexed)

			if currentIsIndexed != updatedIsIndexed {
				needIndexRebuild = true
				if updatedIsIndexed {
					indexChanges = append(indexChanges, fmt.Sprintf("field:[%s] index enabled", fieldName))
				} else {
					indexChanges = append(indexChanges, fmt.Sprintf("field:[%s] index disabled", fieldName))
				}
			}
		} else {
			// Field removed - not supported
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
				fmt.Errorf("field removal not supported for field:[%s]", fieldName))
		}
	}

	// Check for new fields
	for fieldName, updatedProperty := range updatedSpaceProperties {
		if _, exists := currentSpaceProperties[fieldName]; !exists {
			// New field added
			isIndexed := (updatedProperty.Option & vearchpb.FieldOption_Index) != 0
			if isIndexed {
				needIndexRebuild = true
				indexChanges = append(indexChanges, fmt.Sprintf("new indexed field:[%s] added", fieldName))
			}
		}
	}

	// Log changes
	if len(indexChanges) > 0 {
		log.Info("field index changes detected for space:[%s], partition:[%d]: %v",
			ge.space.Name, ge.partitionID, indexChanges)
	}

	// If index changes are detected, apply targeted index operations
	if needIndexRebuild {
		log.Info("applying field index changes for space:[%s], partition:[%d]",
			ge.space.Name, ge.partitionID)

		// Update the index mapping first
		newIndexMapping, err := mapping.Space2Mapping(updatedSpace)
		if err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("failed to create new index mapping: %v", err))
		}

		// Apply targeted index changes synchronously to ensure consistency
		// Use the original space properties for comparison during operations
		for fieldName, currentProperty := range currentSpaceProperties {
			updatedProperty, exists := updatedSpaceProperties[fieldName]
			if !exists {
				continue // Field removed, skip
			}
			currentIsIndexed := (currentProperty.Option & vearchpb.FieldOption_Index) != 0
			updatedIsIndexed := (updatedProperty.Option & vearchpb.FieldOption_Index) != 0

			if currentIsIndexed == updatedIsIndexed {
				continue // No change in index status, skip
			}

			if updatedIsIndexed {
				// Add index for this field
				if err := ge.addFieldIndex(fieldName, updatedSpace); err != nil {
					log.Error("failed to add index for field:[%s] in space:[%s], partition:[%d]: %v",
						fieldName, ge.space.Name, ge.partitionID, err)
					return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
						fmt.Errorf("failed to add index for field %s: %v", fieldName, err))
				} else {
					log.Info("successfully added index for field:[%s] in space:[%s], partition:[%d]",
						fieldName, ge.space.Name, ge.partitionID)
				}
			} else {
				// Remove index for this field
				if err := ge.removeFieldIndex(fieldName); err != nil {
					log.Error("failed to remove index for field:[%s] in space:[%s], partition:[%d]: %v",
						fieldName, ge.space.Name, ge.partitionID, err)
					return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
						fmt.Errorf("failed to remove index for field %s: %v", fieldName, err))
				} else {
					log.Info("successfully removed index for field:[%s] in space:[%s], partition:[%d]",
						fieldName, ge.space.Name, ge.partitionID)
				}
			}
		}

		// Process new fields
		for fieldName, updatedProperty := range updatedSpaceProperties {
			if _, exists := currentSpaceProperties[fieldName]; !exists {
				isIndexed := (updatedProperty.Option & vearchpb.FieldOption_Index) != 0
				if isIndexed {
					// Add index for new field
					if err := ge.addFieldIndex(fieldName, updatedSpace); err != nil {
						log.Error("failed to add index for new field:[%s] in space:[%s], partition:[%d]: %v",
							fieldName, ge.space.Name, ge.partitionID, err)
						return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
							fmt.Errorf("failed to add index for new field %s: %v", fieldName, err))
					} else {
						log.Info("successfully added index for new field:[%s] in space:[%s], partition:[%d]",
							fieldName, ge.space.Name, ge.partitionID)
					}
				}
			}
		}

		// Update index mapping and space only after successful index operations
		ge.indexMapping = newIndexMapping
		log.Info("field index changes completed for space:[%s], partition:[%d]", ge.space.Name, ge.partitionID)
	}

	// Update the space after all operations are successful
	ge.space = updatedSpace

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

func (ge *gammaEngine) addFieldIndex(fieldName string, newSpace *entity.Space) error {
	enginePtr, err := ge.getEnginePtr()
	if err != nil {
		return err
	}

	// Get field properties from current space to extract index parameters
	spaceProperties, err := entity.UnmarshalPropertyJSON(newSpace.Fields)
	if err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
			fmt.Errorf("failed to unmarshal space properties: %v", err))
	}

	fieldProperty, exists := spaceProperties[fieldName]
	if !exists {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR,
			fmt.Errorf("field [%s] not found in space properties", fieldName))
	}

	// Extract index parameters
	var indexType string
	var indexParams []byte

	if fieldProperty.Index != nil {
		indexType = fieldProperty.Index.Type
		indexParams = fieldProperty.Index.Params
		log.Info("adding index for field:[%s] with type:[%s] and params:[%s] in partition:[%d]",
			fieldName, indexType, string(indexParams), ge.partitionID)
	} else {
		// Use default index type based on field type
		switch fieldProperty.FieldType {
		case vearchpb.FieldType_VECTOR:
			indexType = "FLAT" // default vector index type
		case vearchpb.FieldType_STRING, vearchpb.FieldType_INT, vearchpb.FieldType_LONG, vearchpb.FieldType_FLOAT, vearchpb.FieldType_DOUBLE:
			indexType = "SCALAR" // default scalar index type
		default:
			indexType = "SCALAR"
		}
		log.Info("adding index for field:[%s] with default type:[%s] in partition:[%d]",
			fieldName, indexType, ge.partitionID)
	}

	// Use the new function that can handle index parameters
	status := gamma.AddFieldIndexWithParams(enginePtr, fieldName, indexType, indexParams)
	if status.Code != 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
			fmt.Errorf("add field index failed: %s", status.Msg))
	}

	log.Info("successfully added index for field:[%s] in partition:[%d]", fieldName, ge.partitionID)
	return nil
}

func (ge *gammaEngine) removeFieldIndex(fieldName string) error {
	enginePtr, err := ge.getEnginePtr()
	if err != nil {
		return err
	}

	status := gamma.RemoveFieldIndex(enginePtr, fieldName)
	if status.Code != 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
			fmt.Errorf("remove field index failed: %s", status.Msg))
	}

	log.Info("successfully removed index for field:[%s] in partition:[%d]", fieldName, ge.partitionID)
	return nil
}
