/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "storage_manager.h"

#include "common/error_code.h"
#include "util/log.h"
#include "util/utils.h"

namespace tig_gamma {

static void ToRowKey(int key, std::string &key_str) {
  char data[11];
  snprintf(data, 11, "%010d", key);
  key_str.assign(data, 10);
}

StorageManager::StorageManager(const std::string &root_path,
                               const StorageManagerOptions &options)
    : root_path_(root_path), options_(options) {
  size_ = 0;
  db_ = nullptr;
}

StorageManager::~StorageManager() {
  delete db_;
  db_ = nullptr;
}

void StorageManager::GetCacheSize(int &cache_size) { cache_size = 0; }

int StorageManager::Init(const std::string &name, int cache_size) {
  root_path_ += "/" + name;
  name_ = name;

  if (!options_.IsValid()) {
    LOG(ERROR) << "invalid options=" << options_.ToStr();
    return PARAM_ERR;
  }
  if (utils::make_dir(root_path_.c_str())) {
    LOG(ERROR) << "mkdir error, path=" << root_path_;
    return IO_ERR;
  }

  rocksdb::Options options;
  if (cache_size) {
    std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(cache_size);
    table_options_.block_cache = cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options_));
  }
  options.IncreaseParallelism();
  // options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  // open DB
  rocksdb::Status s = rocksdb::DB::Open(options, root_path_, &db_);
  if (!s.ok()) {
    LOG(ERROR) << "open rocks db error: " << s.ToString();
    return IO_ERR;
  }

  std::string key_str = "_total";
  std::string size_str;

  s = db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_str), &size_str);
  if (s.ok()) {
    size_ = std::stoll(size_str);
  } else {
    size_ = 0;
  }

  LOG(INFO) << "init storage [" << name_
            << "] success! options=" << options_.ToStr() << " size " << size_
            << " cache_size [" << cache_size << "]M";
  return 0;
}

int StorageManager::Add(int id, const uint8_t *value, int len) {
  if (len != options_.fixed_value_bytes) {
    LOG(ERROR) << "Add len error [" << len << "] != options_.fixed_value_bytes["
               << options_.fixed_value_bytes << "]";
    return PARAM_ERR;
  }

  std::string key_str;
  ToRowKey(id, key_str);

  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
                               rocksdb::Slice((char *)value, len));
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    return IO_ERR;
  }
  size_++;
  key_str = "_total";
  s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
               rocksdb::Slice(std::to_string(size_)));
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    return IO_ERR;
  }
  return 0;
}

int StorageManager::AddString(int id, std::string field_name, const char *value,
                              int len) {
  std::string key_str;
  ToRowKey(id, key_str);
  key_str = field_name + ":" + key_str;

  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
                               rocksdb::Slice((char *)value, len));
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    return IO_ERR;
  }
  return 0;
}

int StorageManager::Update(int id, uint8_t *value, int len) {
  if (len != options_.fixed_value_bytes) {
    LOG(ERROR) << "Add len error [" << len << "] != options_.fixed_value_bytes["
               << options_.fixed_value_bytes << "]";
    return PARAM_ERR;
  }

  std::string key_str;
  ToRowKey(id, key_str);

  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
                               rocksdb::Slice((char *)value, len));
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    return IO_ERR;
  }
  return 0;
}

int StorageManager::UpdateString(int id, std::string field_name,
                                 const char *value, int len) {
  return AddString(id, field_name, value, len);
}

int StorageManager::Get(int id, const uint8_t *&v) {
  std::string key, value;
  ToRowKey((int)id, key);
  rocksdb::Status s =
      db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key), &value);
  if (!s.ok()) {
    LOG(DEBUG) << "rocksdb get error:" << s.ToString() << ", key=" << key;
    return IO_ERR;
  }

  v = new uint8_t[value.size()];
  memcpy((void *)v, value.c_str(), value.size());

  return 0;
}

int StorageManager::GetString(int id, std::string &field_name,
                              std::string &value) {
  std::string key_str;
  ToRowKey(id, key_str);
  key_str = field_name + ":" + key_str;

  rocksdb::Status s =
      db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_str), &value);
  if (!s.ok()) {
    LOG(DEBUG) << "rocksdb get error:" << s.ToString() << ", key=" << key_str;
    return IO_ERR;
  }

  return 0;
}

}  // namespace tig_gamma
