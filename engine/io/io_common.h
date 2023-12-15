/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include "util/utils.h"

struct IOMeta {
  struct StoreType {
    static constexpr const char *Name = "store_type";
    static constexpr const char *RocksDB = "rocksdb";
    static constexpr const char *File = "file";
  };
};

struct DumpConfig {
  std::string name;

  DumpConfig() {}
  DumpConfig(std::string name_) : name(name_) {}
  virtual ~DumpConfig() {}
  virtual int ToJson(utils::JsonParser &jp) { return 0; };
};
