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

package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/ps/engine"
)

// StoreBase is the base class of partition store.
type StoreBase struct {
	sync.RWMutex
	Ctx       context.Context
	CtxCancel context.CancelFunc

	NodeID    entity.NodeID
	DataPath  string
	MetaPath  string
	Space     *entity.Space
	Partition *entity.Partition

	Engine engine.Engine
}

func NewStoreBase(ctx context.Context, PID entity.PartitionID, NodeID entity.NodeID, path, DataPath, MetaPath string, space *entity.Space) (*StoreBase, error) {
	ctx, cancel := context.WithCancel(ctx)

	base := &StoreBase{
		Ctx:       ctx,
		CtxCancel: cancel,
		NodeID:    NodeID,
		DataPath:  DataPath,
		MetaPath:  MetaPath,
		Space:     space,
	}

	partition := space.GetPartition(PID)
	if partition == nil {
		return nil, fmt.Errorf("can not found partition by id:[%d]", PID)
	}

	base.Partition = partition
	base.Partition.Path = path

	return base, nil
}

func (s *StoreBase) GetSpace() entity.Space {
	s.RLock()
	space := *s.Space
	s.RUnlock()
	return space
}

func (s *StoreBase) SetSpace(space *entity.Space) {
	s.Lock()
	defer s.Unlock()
	s.Space = space
	/*if s.Space.Version > space.Version {
		log.Error("new space[%s] version is less than old space, new:[%d]/old:[%d]", space.Name, space.Version, s.Space.Version)
	}*/
}

func (s *StoreBase) GetVersion() entity.Version {
	s.RLock()
	defer s.RUnlock()
	return s.Space.Version
}

func (s *StoreBase) GetEngine() engine.Engine {
	return s.Engine
}
