/**
 * Copyright 2019 The Vearch Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

package gamma

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vearch/vearch/engine/idl/fbs-gen/go/gamma_api"
)

type CacheInfo struct {
	Name      string
	CacheSize int32
}

type Config struct {
	Path       string
	LogDir     string
	CacheInfos []*CacheInfo

	config *gamma_api.Config
}

func (conf *Config) Serialize(buffer *[]byte) int {
	builder := flatbuffers.NewBuilder(0)
	path := builder.CreateString(conf.Path)
	logDir := builder.CreateString(conf.LogDir)

	var names []flatbuffers.UOffsetT
	var cacheSizes []int32
	names = make([]flatbuffers.UOffsetT, len(conf.CacheInfos))
	cacheSizes = make([]int32, len(conf.CacheInfos))
	i := 0
	for _, cacheInfo := range conf.CacheInfos {
		names[i] = builder.CreateString(cacheInfo.Name)
		cacheSizes[i] = cacheInfo.CacheSize
		i++
	}

	var caches []flatbuffers.UOffsetT
	caches = make([]flatbuffers.UOffsetT, len(conf.CacheInfos))
	for i := 0; i < len(conf.CacheInfos); i++ {
		gamma_api.CacheInfoStart(builder)
		gamma_api.CacheInfoAddFieldName(builder, names[i])
		gamma_api.CacheInfoAddCacheSize(builder, cacheSizes[i])
		caches[i] = gamma_api.CacheInfoEnd(builder)
	}

	gamma_api.ConfigStartCacheInfosVector(builder, len(conf.CacheInfos))
	for i := 0; i < len(conf.CacheInfos); i++ {
		builder.PrependUOffsetT(caches[i])
	}
	f := builder.EndVector(len(conf.CacheInfos))

	gamma_api.ConfigStart(builder)
	gamma_api.ConfigAddPath(builder, path)
	gamma_api.ConfigAddLogDir(builder, logDir)
	gamma_api.ConfigAddCacheInfos(builder, f)
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
	conf.CacheInfos = make([]*CacheInfo, conf.config.CacheInfosLength())
	for i := 0; i < len(conf.CacheInfos); i++ {
		var cache gamma_api.CacheInfo
		conf.config.CacheInfos(&cache, i)
		conf.CacheInfos[i] = &CacheInfo{}
		conf.CacheInfos[i].Name = string(cache.FieldName())
		conf.CacheInfos[i].CacheSize = cache.CacheSize()
	}
}
