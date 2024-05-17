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

StorageManager::StorageManager(const std::string &root_path)
    : root_path_(root_path) {
  column_families_.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
  size_ = 0;
  db_ = nullptr;
}

StorageManager::~StorageManager() { Close(); }

void StorageManager::Close() {
  for (auto handle : cf_handles_) {
    db_->DestroyColumnFamilyHandle(handle);
  }
  delete db_;
  db_ = nullptr;
  LOG(INFO) << "db closed";
}

void StorageManager::GetCacheSize(int &cache_size) { cache_size = 0; }

Status StorageManager::Init(int cache_size) {
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
  rocksdb::Status s;

  std::vector<std::string> cf_names;
  s = rocksdb::DB::ListColumnFamilies(options, root_path_, &cf_names);

  if (!s.ok()) {
    rocksdb::Options options;
    options.create_if_missing = true;
    // create db
    s = rocksdb::DB::Open(options, root_path_, &db_);
    if (!s.ok()) {
      std::string msg = std::string("open rocks db error: ") + s.ToString();
      LOG(ERROR) << msg;
      return Status::IOError(msg);
    }

    // create column family
    for (auto &cfs : column_families_) {
      if (cfs.name == rocksdb::kDefaultColumnFamilyName) {
        continue;
      }
      rocksdb::ColumnFamilyHandle *cf;
      s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cfs.name,
                                  &cf);
      if (!s.ok()) {
        LOG(ERROR) << "Error creating column family: " << s.ToString();
      } else {
        db_->DestroyColumnFamilyHandle(cf);
      }
    }

    delete db_;
    db_ = nullptr;
  }

  // open DB
  s = rocksdb::DB::Open(options, root_path_, column_families_, &cf_handles_,
                        &db_);
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

  LOG(INFO) << "init storage [" << root_path_ << "] success! size " << size_
            << " cache_size [" << cache_size << "]M";
  return Status::OK();
}

Status StorageManager::Add(int cf_id, int id, const uint8_t *value, int len) {
  std::string key_str;
  ToRowKey(id, key_str);

  rocksdb::Status s =
      db_->Put(rocksdb::WriteOptions(), cf_handles_[cf_id],
               rocksdb::Slice(key_str), rocksdb::Slice((char *)value, len));
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

Status StorageManager::AddString(int cf_id, int id, std::string field_name,
                                 const char *value, int len) {
  std::string key_str;
  ToRowKey(id, key_str);
  key_str = field_name + ":" + key_str;

  rocksdb::Status s =
      db_->Put(rocksdb::WriteOptions(), cf_handles_[cf_id],
               rocksdb::Slice(key_str), rocksdb::Slice((char *)value, len));
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    LOG(ERROR) << msg.str();
    return Status::IOError(msg.str());
  }
  return Status::OK();
}

Status StorageManager::Update(int cf_id, int id, uint8_t *value, int len) {
  std::string key_str;
  ToRowKey(id, key_str);

  rocksdb::Status s =
      db_->Put(rocksdb::WriteOptions(), cf_handles_[cf_id],
               rocksdb::Slice(key_str), rocksdb::Slice((char *)value, len));
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    LOG(ERROR) << msg.str();
    return Status::IOError(msg.str());
  }
  return Status::OK();
}

Status StorageManager::Put(int cf_id, const std::string &key,
                           std::string &value) {
  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), cf_handles_[cf_id],
                               rocksdb::Slice(key), rocksdb::Slice(value));
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb put error:" << s.ToString() << ", key=" << key;
    LOG(ERROR) << msg.str();
    return Status::IOError(msg.str());
  }
  return Status::OK();
}

Status StorageManager::Delete(int cf_id, const std::string &key) {
  rocksdb::WriteOptions write_options;
  rocksdb::Status s = db_->Delete(write_options, cf_handles_[cf_id], key);
  if (!s.ok()) {
    LOG(ERROR) << "delelte rocksdb failed: " << key << " " << s.ToString();
    return Status::IOError(s.ToString());
  }
  return Status::OK();
}

Status StorageManager::UpdateString(int cf_id, int id, std::string field_name,
                                    const char *value, int len) {
  return AddString(cf_id, id, field_name, value, len);
}

std::pair<Status, std::string> StorageManager::Get(int cf_id, int id) {
  std::string key, value;
  ToRowKey((int)id, key);
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), cf_handles_[cf_id],
                               rocksdb::Slice(key), &value);
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb get error:" << s.ToString() << " key=" << key;
    return {Status::IOError(msg.str()), std::string()};
  }

  return {Status::OK(), std::move(value)};
}

std::pair<Status, std::string> StorageManager::Get(int cf_id,
                                                   const std::string &key) {
  std::string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), cf_handles_[cf_id],
                               rocksdb::Slice(key), &value);
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb get error:" << s.ToString() << " key=" << key;
    return {Status::IOError(msg.str()), std::string()};
  }

  return {Status::OK(), std::move(value)};
}

Status StorageManager::GetString(int cf_id, int id, std::string &field_name,
                                 std::string &value) {
  std::string key_str;
  ToRowKey(id, key_str);
  key_str = field_name + ":" + key_str;

  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), cf_handles_[cf_id],
                               rocksdb::Slice(key_str), &value);
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb get error:" << s.ToString() << ", key=" << key_str;
    return Status::IOError(msg.str());
  }

  return Status::OK();
}

}  // namespace vearch
