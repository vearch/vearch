/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

import (
	"../../idl/fbs-gen/go/gamma_api"
	flatbuffers "github.com/google/flatbuffers/go"
)

type Config struct {
	Path       string
	LogDir     string
	config     *gamma_api.Config
}

func (conf *Config) Serialize(buffer *[]byte) int {
	builder := flatbuffers.NewBuilder(0)
	path := builder.CreateString(conf.Path)
	logDir := builder.CreateString(conf.LogDir)
	gamma_api.ConfigStart(builder)
	gamma_api.ConfigAddPath(builder, path)
	gamma_api.ConfigAddLogDir(builder, logDir)
	builder.Finish(builder.EndObject())
	bufferLen := len(builder.FinishedBytes())
	*buffer = make([]byte, bufferLen)
	copy(*buffer, builder.FinishedBytes())
	return bufferLen
}

func (conf *Config) DeSerialize(buffer []byte) {
	conf.config = gamma_api.GetRootAsConfig(buffer, 0)
	conf.Path = string(conf.config.Path())
	conf.LogDir = string(conf.config.LogDir())
}
