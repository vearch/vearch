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

package register

import (
	"fmt"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"

	"github.com/vearch/vearch/ps/engine"
)

var (
	engines map[string]EngineBuilder
)

func init() {
	engines = make(map[string]EngineBuilder, 8)
}

// EngineBuilder is used to build the engine.
type EngineBuilder func(cfg EngineConfig) (engine.Engine, error)

type EngineConfig struct {
	// Path is the data directory.
	Path string
	// ExtraOptions contains extension options using a json format ("{key1:value1,key2:value2}").
	ExtraOptions map[string]interface{}
	// Schema
	Space *entity.Space
	//partitionID
	PartitionID entity.PartitionID

	DWPTNum uint64
}

// Register is used to register the engine implementers in the initialization phase.
func Register(name string, builder EngineBuilder) {
	if name == "" || builder == nil {
		panic("Registration name and builder cannot be empty")
	}
	if _, ok := engines[name]; ok {
		panic(fmt.Sprintf("Duplicate registration engine name for %s", name))
	}

	engines[name] = builder
}

// Build create an engine based on the specified name.
func Build(name string, cfg EngineConfig) (e engine.Engine, err error) {
	if builder := engines[name]; builder != nil {
		e, err = builder(cfg)
	} else {
		err = pkg.CodeErr(pkg.ERRCODE_PARTITON_ENGINENAME_INVALID)
	}
	return
}

// Exist return whether the engine exists.
func Exist(name string) bool {
	if builder := engines[name]; builder != nil {
		return true
	}

	return false
}
