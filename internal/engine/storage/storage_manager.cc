/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "storage_manager.h"

#include <sstream>

#include "util/log.h"
#include "util/utils.h"

namespace vearch {

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

Status StorageManager::Init(const std::string &name, int cache_size) {
  root_path_ += "/" + name;
  name_ = name;

  if (!options_.IsValid()) {
    std::string msg = std::string("invalid options=") + options_.ToStr();
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
  }
  if (utils::make_dir(root_path_.c_str())) {
    std::string msg = std::string("mkdir error, path=") + root_path_;
    LOG(ERROR) << msg;
    return Status::IOError(msg);
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
    std::string msg = std::string("open rocks db error: ") + s.ToString();
    LOG(ERROR) << msg;
    return Status::IOError(msg);
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
  return Status::OK();
}

Status StorageManager::Add(int id, const uint8_t *value, int len) {
  if (len != options_.fixed_value_bytes) {
    std::stringstream msg;
    msg << "Add len error [" << len << "] != options_.fixed_value_bytes["
        << options_.fixed_value_bytes << "]";
    LOG(ERROR) << msg.str();
    return Status::ParamError(msg.str());
  }

  std::string key_str;
  ToRowKey(id, key_str);

  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
                               rocksdb::Slice((char *)value, len));
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    LOG(ERROR) << msg.str();
    return Status::IOError(msg.str());
  }
  size_++;
  key_str = "_total";
  s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
               rocksdb::Slice(std::to_string(size_)));
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    LOG(ERROR) << msg.str();
    return Status::IOError(msg.str());
  }
  return Status::OK();
}

Status StorageManager::AddString(int id, std::string field_name,
                                 const char *value, int len) {
  std::string key_str;
  ToRowKey(id, key_str);
  key_str = field_name + ":" + key_str;

  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
                               rocksdb::Slice((char *)value, len));
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    LOG(ERROR) << msg.str();
    return Status::IOError(msg.str());
  }
  return Status::OK();
}

Status StorageManager::Update(int id, uint8_t *value, int len) {
  if (len != options_.fixed_value_bytes) {
    std::stringstream msg;
    msg << "Add len error [" << len << "] != options_.fixed_value_bytes["
        << options_.fixed_value_bytes << "]";
    LOG(ERROR) << msg.str();
    return Status::ParamError(msg.str());
  }

  std::string key_str;
  ToRowKey(id, key_str);

  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
                               rocksdb::Slice((char *)value, len));
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    LOG(ERROR) << msg.str();
    return Status::IOError(msg.str());
  }
  return Status::OK();
}

Status StorageManager::UpdateString(int id, std::string field_name,
                                    const char *value, int len) {
  return AddString(id, field_name, value, len);
}

std::pair<Status, std::string> StorageManager::Get(int id) {
  std::string key, value;
  ToRowKey((int)id, key);
  rocksdb::Status s =
      db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key), &value);
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb get error:" << s.ToString() << " key=" << key;
    LOG(DEBUG) << msg.str();
    return {Status::IOError(msg.str()), std::string()};
  }

  return {Status::OK(), std::move(value)};
}

Status StorageManager::GetString(int id, std::string &field_name,
                                 std::string &value) {
  std::string key_str;
  ToRowKey(id, key_str);
  key_str = field_name + ":" + key_str;

  rocksdb::Status s =
      db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_str), &value);
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb get error:" << s.ToString() << ", key=" << key_str;
    LOG(DEBUG) << msg.str();
    return Status::IOError(msg.str());
  }

  return Status::OK();
}

}  // namespace vearch
