/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "rocksdb_wrapper.h"

#include "util/log.h"

using std::string;

namespace vearch {

RocksDBWrapper::RocksDBWrapper() : db_(nullptr) {}

RocksDBWrapper::~RocksDBWrapper() {
  delete db_;
  db_ = nullptr;
}

Status RocksDBWrapper::Open(string db_path, size_t block_cache_size) {
  rocksdb::Options options;
  if (block_cache_size) {
    std::shared_ptr<rocksdb::Cache> cache =
        rocksdb::NewLRUCache(block_cache_size);
    table_options_.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options_));
  }
  options.IncreaseParallelism();
  // options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  // open DB
  rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db_);
  if (!s.ok()) {
    std::string msg = std::string("open rocks db error: ") + s.ToString();
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }
  return Status::OK();
}

Status RocksDBWrapper::Put(int key, const char *v, size_t len) {
  string key_str;
  ToRowKey(key, key_str);
  return Put(key_str, v, len);
}

Status RocksDBWrapper::Put(const string &key, const char *v, size_t len) {
  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key),
                               rocksdb::Slice(v, len));
  if (!s.ok()) {
    std::string msg =
        std::string("rocksdb put error:") + s.ToString() + ", key=" + key;
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }
  return Status::OK();
}

Status RocksDBWrapper::Put(const string &key, const string &value) {
  return Put(key, value.c_str(), value.size());
}

void RocksDBWrapper::ToRowKey(int key, string &key_str) {
  char data[11];
  snprintf(data, 11, "%010d", key);
  key_str.assign(data, 10);
}

}  // namespace vearch
