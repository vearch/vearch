/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sstream>
#include <utility>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "util/status.h"

namespace vearch {

class StorageManager {
 public:
  StorageManager(const std::string &root_path);
  ~StorageManager();
  Status Init(int cache_size);

  Status Add(int cf_id, int id, const uint8_t *value, int len);

  Status AddString(int cf_id, int id, std::string field_name, const char *value,
                   int len);

  Status Update(int cf_id, int id, uint8_t *value, int len);
  Status Put(int cf_id, const std::string &key, std::string &value);
  Status Delete(int cf_id, const std::string &key);

  Status UpdateString(int cf_id, int id, std::string field_name,
                      const char *value, int len);

  std::pair<Status, std::string> Get(int cf_id, int id);
  std::pair<Status, std::string> Get(int cf_id, const std::string &key);

  Status GetString(int cf_id, int id, std::string &field_name,
                   std::string &value);

  int Size() { return size_; }

  void GetCacheSize(size_t &cache_size);

  void AlterCacheSize(size_t cache_size);

  void Close();

  int CreateColumnFamily(std::string name) {
    column_families_.push_back(
        rocksdb::ColumnFamilyDescriptor(name, rocksdb::ColumnFamilyOptions()));
    return column_families_.size() - 1;
  }
  void ToRowKey(int key, std::string &key_str) {
    char data[12];
    int length = snprintf(data, 12, "%010d", key);
    key_str.assign(data, length);
  }

  //  private:
  std::string root_path_;
  size_t size_;  // The total number of doc.
  rocksdb::DB *db_;
  rocksdb::BlockBasedTableOptions table_options_;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families_;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
};

}  // namespace vearch
