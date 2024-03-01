/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace vearch {

struct StorageManagerOptions {
  int fixed_value_bytes;

  StorageManagerOptions() { fixed_value_bytes = -1; }

  StorageManagerOptions(const StorageManagerOptions &options) {
    fixed_value_bytes = options.fixed_value_bytes;
  }

  bool IsValid() {
    if (fixed_value_bytes == -1) return false;
    return true;
  }

  std::string ToStr() {
    std::stringstream ss;
    ss << "{fixed_value_bytes=" << fixed_value_bytes << "}";
    return ss.str();
  }
};

class StorageManager {
 public:
  StorageManager(const std::string &root_path,
                 const StorageManagerOptions &options);
  ~StorageManager();
  int Init(const std::string &name, int cache_size);

  int Add(int id, const uint8_t *value, int len);

  int AddString(int id, std::string field_name, const char *value, int len);

  int Update(int id, uint8_t *value, int len);

  int UpdateString(int id, std::string field_name, const char *value, int len);

  // warning: vec can't be free
  int Get(int id, const uint8_t *&value);

  int GetString(int id, std::string &field_name, std::string &value);

  int Size() { return size_; }

  void GetCacheSize(int &cache_size);

  const StorageManagerOptions &GetStorageManagerOptions() { return options_; }

 private:
  std::string root_path_;
  std::string name_;
  size_t size_;  // The total number of doc.
  StorageManagerOptions options_;
  rocksdb::DB *db_;
  rocksdb::BlockBasedTableOptions table_options_;
};

}  // namespace vearch
