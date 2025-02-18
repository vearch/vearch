/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "storage_manager.h"

#include <rocksdb/advanced_cache.h>

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
    if (handle) {
      auto s = db_->DestroyColumnFamilyHandle(handle);
      if (!s.ok()) {
        LOG(ERROR) << "DestroyColumnFamilyHandle error: " << s.ToString();
      }
    }
  }
  db_.reset();
  LOG(INFO) << "db closed";
}

int StorageManager::SetSize(int64_t size) {
  size_ = size;
  std::string key_str = "_total";
  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_str),
                               rocksdb::Slice(std::to_string(size_)));
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb put error:" << s.ToString() << ", key=" << key_str;
    return -1;
  }

  return 0;
}

void StorageManager::GetCacheSize(size_t &cache_size) {
  cache_size = table_options_.block_cache->GetCapacity();
  cache_size = cache_size / 1024 / 1024;
}

void StorageManager::AlterCacheSize(size_t cache_size) {
  cache_size = cache_size * 1024 * 1024;
  table_options_.block_cache->SetCapacity(cache_size);
}

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

  std::vector<std::string> existing_cfs;
  s = rocksdb::DB::ListColumnFamilies(options, root_path_, &existing_cfs);

  if (s.ok()) {
    LOG(INFO) << "existing cfs: ";

    for (const auto &cf_name : existing_cfs) {
      if (cf_name == rocksdb::kDefaultColumnFamilyName) {
        continue;
      }
      // if column_families_ does not contain cf_name, add it
      if (std::find_if(column_families_.begin(), column_families_.end(),
                       [&cf_name](const rocksdb::ColumnFamilyDescriptor &cf) {
                         return cf.name == cf_name;
                       }) == column_families_.end()) {
        column_families_.emplace_back(cf_name, rocksdb::ColumnFamilyOptions());
        LOG(INFO) << "add cf: " << cf_name;
      }
    }

    std::vector<std::string> cfs_to_create;

    for (const auto &cf_desc : column_families_) {
      if (std::find(existing_cfs.begin(), existing_cfs.end(), cf_desc.name) ==
          existing_cfs.end()) {
        cfs_to_create.push_back(cf_desc.name);
        LOG(INFO) << "cf to create: " << cf_desc.name;
      }
    }

    rocksdb::DB *db;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    for (const auto &cf_name : existing_cfs) {
      column_families.emplace_back(cf_name, rocksdb::ColumnFamilyOptions());
      LOG(INFO) << "existing cf: " << cf_name;
    }
    s = rocksdb::DB::Open(options, root_path_, column_families, &cf_handles_,
                          &db);
    db_.reset(db);

    if (s.ok() && !cfs_to_create.empty()) {
      // create column family
      for (const auto &cf_name : cfs_to_create) {
        if (cf_name == rocksdb::kDefaultColumnFamilyName) {
          continue;
        }
        rocksdb::ColumnFamilyHandle *cf_handle;
        s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cf_name,
                                    &cf_handle);
        if (!s.ok()) {
          LOG(ERROR) << "create cf failed: " << cf_name
                     << ", error: " << s.ToString();
        } else {
          cf_handles_.push_back(cf_handle);
          LOG(INFO) << "create cf: " << cf_name;
        }
      }
    } else if (!s.ok()) {
      LOG(ERROR) << "open db error: " << s.ToString();
      return Status::IOError(s.ToString());
    }

    cf_handles_.clear();
    db_.reset();
  } else {
    LOG(INFO) << "no existing cfs, create all";
    rocksdb::Options options;
    options.create_if_missing = true;
    // create db
    rocksdb::DB *db;
    s = rocksdb::DB::Open(options, root_path_, &db);
    db_.reset(db);
    if (!s.ok()) {
      std::string msg = std::string("open rocksdb error: ") + s.ToString();
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

    db_.reset();
  }

  // open DB
  rocksdb::DB *db;
  s = rocksdb::DB::Open(options, root_path_, column_families_, &cf_handles_,
                        &db);
  db_.reset(db);
  if (!s.ok()) {
    std::string msg = std::string("open rocksdb error: ") + s.ToString();
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

Status StorageManager::Add(int cf_id, int64_t id, const uint8_t *value,
                           int len) {
  std::string key_str = utils::ToRowKey(id);

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

Status StorageManager::AddString(int cf_id, int64_t id, std::string field_name,
                                 const char *value, int len) {
  std::string key_str = utils::ToRowKey(id);
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

Status StorageManager::Update(int cf_id, int64_t id, uint8_t *value, int len) {
  std::string key_str = utils::ToRowKey(id);

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

Status StorageManager::UpdateString(int cf_id, int64_t id,
                                    std::string field_name, const char *value,
                                    int len) {
  return AddString(cf_id, id, field_name, value, len);
}

std::pair<Status, std::string> StorageManager::Get(int cf_id, int64_t id) {
  std::string key, value;
  key = utils::ToRowKey(id);
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), cf_handles_[cf_id],
                               rocksdb::Slice(key), &value);
  if (!s.ok()) {
    return {Status::IOError("rocksdb get error: " + s.ToString() +
                            " key=" + utils::ToRowKey(id)),
            std::string()};
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

Status StorageManager::Get(int cf_id, const std::string &key,
                           std::string &value) {
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), cf_handles_[cf_id],
                               rocksdb::Slice(key), &value);
  if (!s.ok()) {
    std::stringstream msg;
    msg << "rocksdb get error:" << s.ToString() << " key=" << key;
    return Status::IOError(msg.str());
  }

  return Status::OK();
}

Status StorageManager::GetString(int cf_id, int64_t id, std::string &field_name,
                                 std::string &value) {
  std::string key_str = utils::ToRowKey(id);
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

std::vector<rocksdb::Status> StorageManager::MultiGet(
    int cf_id, const std::vector<int64_t> &vids,
    std::vector<std::string> &values) {
  size_t k = vids.size();
  std::vector<rocksdb::Slice> keys;
  std::vector<std::string> keys_data(k);
  std::vector<rocksdb::ColumnFamilyHandle *> column_families;
  keys.reserve(k);
  column_families.reserve(k);

  for (size_t i = 0; i < k; i++) {
    assert(vids[i] >= 0);
    keys_data[i] = utils::ToRowKey(vids[i]);
    keys.emplace_back(std::move(keys_data[i]));
    column_families.emplace_back(cf_handles_[cf_id]);
  }

  std::vector<rocksdb::Status> statuses =
      db_->MultiGet(rocksdb::ReadOptions(), column_families, keys, &values);
  return statuses;
}

}  // namespace vearch
