/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "rocksdb_raw_vector.h"

#include <stdio.h>

#include "rocksdb/table.h"
#include "util/log.h"
#include "util/utils.h"

namespace vearch {

RocksDBRawVector::RocksDBRawVector(VectorMetaInfo *meta_info,
                                   const StoreParams &store_params,
                                   bitmap::BitmapManager *docids_bitmap,
                                   StorageManager *storage_mgr, int cf_id)
    : RawVector(meta_info, docids_bitmap, store_params) {
  storage_mgr_ = storage_mgr;
  cf_id_ = cf_id;
  compact_if_need_ = false;
}

RocksDBRawVector::~RocksDBRawVector() {}

int RocksDBRawVector::GetDiskVecNum(int64_t &vec_num) {
  if (vec_num <= 0) return 0;
  auto max_id_in_disk = vec_num - 1;
  for (auto i = max_id_in_disk; i >= 0; --i) {
    auto result = storage_mgr_->Get(cf_id_, i);
    if (result.first.ok()) {
      vec_num = i + 1;
      LOG(INFO) << desc_ << "in the disk rocksdb vec_num=" << vec_num;
      return 0;
    }
  }
  vec_num = 0;
  LOG(INFO) << desc_ << "in the disk rocksdb vec_num=" << vec_num;
  return 0;
}

Status RocksDBRawVector::Load(int64_t vec_num) {
  if (vec_num == 0) return Status::OK();
  MetaInfo()->size_ = vec_num;
  LOG(INFO) << desc_ << "rocksdb load success! vec_num=" << vec_num;
  return Status::OK();
}

int RocksDBRawVector::InitStore(std::string &vec_name) {
  block_cache_size_ = (size_t)store_params_.cache_size * 1024 * 1024;

  // std::shared_ptr<rocksdb::Cache> cache =
  //     rocksdb::NewLRUCache(block_cache_size_);
  // table_options_.block_cache = cache;
  // rocksdb::Options options;
  // options.table_factory.reset(NewBlockBasedTableFactory(table_options_));

  // options.IncreaseParallelism();
  // // options.OptimizeLevelStyleCompaction();
  // // create the DB if it's not already present
  // options.create_if_missing = true;

  // std::string db_path = this->root_path_ + "/" + meta_info_->Name();
  // if (!utils::isFolderExist(db_path.c_str())) {
  //   mkdir(db_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  // }

  // // open DB
  // rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db_);
  // if (!s.ok()) {
  //   LOG(ERROR) << "open rocksdb error: " << s.ToString();
  //   return -1;
  // }
  // LOG(INFO) << "rocks raw vector init success! name=" << meta_info_->Name()
  //           << ", block cache size=" << block_cache_size_ << "Bytes";

  return 0;
}

int RocksDBRawVector::GetVector(int64_t vid, const uint8_t *&vec,
                                bool &deletable) const {
  if (vid >= meta_info_->Size() || vid < 0) {
    return 1;
  }
  auto result = storage_mgr_->Get(cf_id_, vid);
  if (!result.first.ok()) {
    LOG(ERROR) << desc_ << "rocksdb get error:" << result.first.ToString()
               << ", vid=" << vid;
    return result.first.code();
  }
  vec = new uint8_t[vector_byte_size_];
  if ((size_t)vector_byte_size_ == result.second.size()) {
    memcpy((void *)vec, result.second.c_str(), vector_byte_size_);
  } else {
    LOG(ERROR) << desc_ << "rocksdb get error: invalid vector size="
               << result.second.size() << ", expect=" << vector_byte_size_;
    return 2;
  }

  deletable = true;
  return 0;
}

int RocksDBRawVector::Gets(const std::vector<int64_t> &vids,
                           ScopeVectors &vecs) const {
  size_t k = vids.size();

  std::vector<std::string> values(k);
  std::vector<rocksdb::Status> statuses =
      storage_mgr_->MultiGet(cf_id_, vids, values);
  if (statuses.size() != k) {
    LOG(ERROR) << desc_ << "rocksdb multiget error: statuses size=" << statuses.size()
               << ", vids size=" << k;
    return 1;
  }

  for (size_t i = 0; i < k; ++i) {
    if (vids[i] < 0) {
      vecs.Add(nullptr, true);
      continue;
    }
    if (!statuses[i].ok()) {
      vecs.Add(nullptr, true);
      continue;
    }
    uint8_t *vector = new uint8_t[vector_byte_size_];
    if ((size_t)vector_byte_size_ != values[i].size()) {
      LOG(ERROR) << desc_ << "rocksdb multiget error: invalid vector size="
                 << values[i].size() << ", expect=" << vector_byte_size_;
      vecs.Add(nullptr, true);
      continue;  
    }
    memcpy(vector, values[i].c_str(), vector_byte_size_);
    vecs.Add(vector, true);
  }
  return 0;
}

int RocksDBRawVector::AddToStore(uint8_t *v, int len) {
  return UpdateToStore(meta_info_->Size(), v, len);
}

size_t RocksDBRawVector::GetStoreMemUsage() {
  //   size_t cache_mem = table_options_.block_cache->GetUsage();
  //   std::string index_mem;
  //   db_->GetProperty("rocksdb.estimate-table-readers-mem", &index_mem);
  //   std::string memtable_mem;
  //   db_->GetProperty("rocksdb.cur-size-all-mem-tables", &memtable_mem);
  //   size_t pin_mem = table_options_.block_cache->GetPinnedUsage();
  // #ifdef DEBUG
  //   LOG(INFO) << "rocksdb mem usage: block cache=" << cache_mem
  //             << ", index and filter=" << index_mem
  //             << ", memtable=" << memtable_mem
  //             << ", iterators pinned=" << pin_mem;
  // #endif
  //   return cache_mem + pin_mem;
  return 0;
}

int RocksDBRawVector::UpdateToStore(int64_t vid, uint8_t *v, int len) {
  if (v == nullptr || len != meta_info_->Dimension() * meta_info_->DataSize())
    return -1;

  std::string key = utils::ToRowKey(vid);
  std::string value = std::string((const char *)v, this->vector_byte_size_);
  Status s = storage_mgr_->Put(cf_id_, key, value);
  if (!s.ok()) {
    LOG(ERROR) << desc_ << "rocksdb update error:" << s.ToString() << ", key=" << key;
    return -1;
  }
  return 0;
}

int RocksDBRawVector::DeleteFromStore(int64_t vid) {
  std::string key = utils::ToRowKey(vid);
  Status s = storage_mgr_->Delete(cf_id_, key);
  if (!s.ok()) {
    LOG(ERROR) << desc_ << "rocksdb update error:" << s.ToString() << ", key=" << key;
    return -1;
  }
  return 0;
}

// get n valid vectors from start
int RocksDBRawVector::GetVectorHeader(int64_t start, int n, ScopeVectors &vecs,
                                      std::vector<int> &lens) {
  if (start < 0 || start + n > meta_info_->Size()) {
    return -1;
  }

  std::unique_ptr<rocksdb::Iterator> it = storage_mgr_->NewIterator(cf_id_);
  std::string start_key, end_key;
  start_key = utils::ToRowKey(start);
  end_key = utils::ToRowKey(meta_info_->Size());
  it->Seek(rocksdb::Slice(start_key));
  int dimension = meta_info_->Dimension();
  uint8_t *vectors = new uint8_t[(uint64_t)dimension * n * data_size_];
  uint8_t *dst = vectors;
  int c = 0;
  for (; c < n; c++, it->Next()) {
    rocksdb::Slice current_key = it->key();
    if (current_key.compare(end_key) >= 0) {
        break;
    }
    if (!it->Valid()) {
      LOG(WARNING) << desc_ << "rocksdb iterator error, current_key=" << current_key.ToString()
                   << ", start_key=" << start_key << ", end_key=" << end_key << ", c=" << c;
      break;
    }

    rocksdb::Slice value = it->value();
    std::string vstr = value.ToString();
    if((size_t)vector_byte_size_ == vstr.size()) {
      memcpy(dst, vstr.c_str(), vector_byte_size_);
    } else {
      LOG(ERROR) << desc_ << "rocksdb get error: invalid vector size="
                 << vstr.size() << ", expect=" << vector_byte_size_;
      continue;
    }

#ifdef DEBUG
    std::string expect_key = utils::ToRowKey(c + start);
    std::string key = it->key().ToString();
    if (key != expect_key) {
      LOG(ERROR) << "vid=" << c + start << ", invalid key=" << key
                 << ", expect=" << expect_key;
    }
#endif
    dst += (size_t)dimension * data_size_;
  }
  vecs.Add(vectors);
  lens.push_back(c);
  return 0;
}

}  // namespace vearch
