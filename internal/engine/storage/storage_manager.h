/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
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
  Status Init(size_t cache_size);

  Status Add(int cf_id, int64_t id, const uint8_t *value, int len);

  Status AddString(int cf_id, int64_t id, std::string field_name,
                   const char *value, int len);

  Status Update(int cf_id, int64_t id, uint8_t *value, int len);
  Status Put(int cf_id, const std::string &key, std::string &value);
  Status Delete(int cf_id, const std::string &key);
  Status DeleteString(int cf_id, int64_t id, std::string field_name);

  Status UpdateString(int cf_id, int64_t id, std::string field_name,
                      const char *value, int len);

  std::pair<Status, std::string> Get(int cf_id, int64_t id);
  std::pair<Status, std::string> Get(int cf_id, const std::string &key);

  Status Get(int cf_id, const std::string &key, std::string &value);

  Status GetString(int cf_id, int64_t id, std::string &field_name,
                   std::string &value);

  std::vector<rocksdb::Status> MultiGet(int cf_id,
                                        const std::vector<int64_t> &vids,
                                        std::vector<std::string> &values);

  std::unique_ptr<rocksdb::Iterator> NewIterator(int cf_id) {
    return std::unique_ptr<rocksdb::Iterator>(
        db_->NewIterator(rocksdb::ReadOptions(), cf_handles_[cf_id]));
  }

  int64_t Size() { return size_; }

  int SetSize(int64_t size);

  Status SetVectorIndexCount(int64_t vector_index_count);

  void GetVectorIndexCount(int64_t &vector_index_count);

  void GetCacheSize(size_t &cache_size);

  void AlterCacheSize(size_t cache_size);

  void Close();

  int CreateColumnFamily(std::string name) {
    column_families_.push_back(
        rocksdb::ColumnFamilyDescriptor(name, rocksdb::ColumnFamilyOptions()));
    return column_families_.size() - 1;
  }

  std::unique_ptr<rocksdb::DB> &GetDB() { return db_; }

  rocksdb::ColumnFamilyHandle *GetColumnFamilyHandle(int cf_id) {
    return cf_handles_[cf_id];
  }

  Status Backup(const std::string &path);

 private:
  std::string root_path_;
  int64_t size_;  // The total number of doc.
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::BlockBasedTableOptions table_options_;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families_;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
};

}  // namespace vearch
