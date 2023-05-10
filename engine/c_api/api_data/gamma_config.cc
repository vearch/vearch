/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_config.h"

namespace tig_gamma {

int Config::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;

  std::vector<flatbuffers::Offset<gamma_api::CacheInfo>>
                                        cache_vector(cache_infos_.size());
  int i = 0;
  for (auto &c : cache_infos_) {
    auto cache = gamma_api::CreateCacheInfo(builder, 
                                builder.CreateString(c.field_name),
                                c.cache_size);
    cache_vector[i++] = cache;
  }
  auto cache_vec = builder.CreateVector(cache_vector);
  auto config =
      gamma_api::CreateConfig(builder, builder.CreateString(path_),
                              builder.CreateString(log_dir_),
                              cache_vec);

  builder.Finish(config);
  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);
  return 0;
}

void Config::Deserialize(const char *data, int len) {
  config_ = const_cast<gamma_api::Config *>(gamma_api::GetConfig(data));

  path_ = config_->path()->str();
  log_dir_ = config_->log_dir()->str();

  size_t cache_num = config_->cache_infos()->size();
  cache_infos_.resize(cache_num);
  for (size_t i = 0; i < cache_num; ++i) {
    auto c = config_->cache_infos()->Get(i);
    struct CacheInfo cache_info;
    cache_info.field_name = c->field_name()->str();
    cache_info.cache_size = c->cache_size();
    cache_infos_[i] = cache_info;
  }
}

const std::string &Config::Path() {
  assert(config_);
  return path_;
}

void Config::SetPath(std::string &path) { path_ = path; }

const std::string &Config::LogDir() {
  assert(config_);
  return log_dir_;
}

void Config::SetLogDir(std::string &log_dir) { log_dir_ = log_dir; }

void Config::AddCacheInfo(const struct CacheInfo &cache) {
  cache_infos_.push_back(cache);
}

void Config::AddCacheInfo(struct CacheInfo &&cache) {
  cache_infos_.emplace_back(std::forward<struct CacheInfo>(cache));
}

void Config::AddCacheInfo(std::string name, int cache_size) {
  struct CacheInfo c;
  c.field_name = name;
  c.cache_size = cache_size;
  cache_infos_.push_back(c);
}

}  // namespace tig_gamma