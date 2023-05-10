/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "idl/fbs-gen/c/config_generated.h"
#include "gamma_raw_data.h"


namespace tig_gamma {

struct CacheInfo {
  std::string field_name;
  int cache_size;
  CacheInfo() {}

  CacheInfo(const CacheInfo &other) { *this = other; }

  CacheInfo &operator=(const CacheInfo &other) {
    field_name = other.field_name;
    cache_size = other.cache_size;
    return *this;
  }

  CacheInfo(CacheInfo &&other) { *this = std::move(other); }

  CacheInfo &operator=(CacheInfo &&other) {
    field_name = std::move(other.field_name);
    cache_size = other.cache_size;
    return *this;
  }
};

class Config : public RawData {
 public:
  Config() { config_ = nullptr; }

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  const std::string &Path();

  void SetPath(std::string &path);

  const std::string &LogDir();

  void SetLogDir(std::string &log_dir);

  void AddCacheInfo(const struct CacheInfo &cache);

  void AddCacheInfo(struct CacheInfo &&cache);

  void AddCacheInfo(std::string name, int cache_size);

  std::vector<CacheInfo> &CacheInfos() { return cache_infos_; }

  void ClearCacheInfos() { cache_infos_.resize(0); }

 private:
  gamma_api::Config *config_;

  std::string path_;
  std::string log_dir_;
  std::vector<CacheInfo> cache_infos_;
};

}  // namespace tig_gamma
