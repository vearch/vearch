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

namespace tig_gamma {

struct StorageManagerOptions {
  int segment_size;
  int fixed_value_bytes;
  uint32_t seg_block_capacity;

  StorageManagerOptions() {
    segment_size = -1;
    fixed_value_bytes = -1;
    seg_block_capacity = 0;
  }

  StorageManagerOptions(const StorageManagerOptions &options) {
    segment_size = options.segment_size;
    fixed_value_bytes = options.fixed_value_bytes;
    seg_block_capacity = options.seg_block_capacity;
  }

  bool IsValid() {
    if (segment_size == -1 || fixed_value_bytes == -1 ||
        seg_block_capacity == 0)
      return false;
    return true;
  }

  std::string ToStr() {
    std::stringstream ss;
    ss << "{segment_size=" << segment_size
       << ", fixed_value_bytes=" << fixed_value_bytes
       << ", seg_block_capacity=" << seg_block_capacity << "}";
    return ss.str();
  }
};

class StorageManager {
 public:
  StorageManager(const std::string &root_path,
                 const StorageManagerOptions &options);
  ~StorageManager();
  int Init(std::string name, int cache_size);

  int Add(int doc_id, const uint8_t *value, int len);

  int AddString(int docid, std::string field_name, const char *value, int len);

  int Update(int id, uint8_t *value, int len);

  int UpdateString(int docid, std::string field_name, const char *value,
                   int len);

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

}  // namespace tig_gamma
