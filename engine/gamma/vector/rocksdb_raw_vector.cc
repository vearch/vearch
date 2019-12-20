/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifdef WITH_ROCKSDB

#include "rocksdb_raw_vector.h"
#include <stdio.h>
#include "log.h"
#include "rocksdb/table.h"
#include "utils.h"

using namespace std;
using namespace rocksdb;

namespace tig_gamma {

RocksDBRawVector::RocksDBRawVector(const std::string &name, int dimension,
                                   int max_vector_size,
                                   const std::string &root_path,
                                   const StoreParams &store_params)
    : RawVector(name, dimension, max_vector_size, root_path) {
  root_path_ = root_path;
  db_ = nullptr;
  store_params_ = new StoreParams(store_params);
}

RocksDBRawVector::~RocksDBRawVector() {
  if (db_) {
    delete db_;
  }
  if (store_params_) delete store_params_;
}

int RocksDBRawVector::Init() {
  block_cache_size_ = store_params_->cache_size_;

  std::shared_ptr<Cache> cache = NewLRUCache(block_cache_size_);
  // BlockBasedTableOptions table_options_;
  table_options_.block_cache = cache;
  Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options_));

  options.IncreaseParallelism();
  // options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  string db_path = root_path_ + "/" + vector_name_;
  if (!utils::isFolderExist(db_path.c_str())) {
    mkdir(db_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  // open DB
  Status s = DB::Open(options, db_path, &db_);
  if (!s.ok()) {
    LOG(ERROR) << "open rocks db error: " << s.ToString();
    return -1;
  }
  LOG(INFO) << "rocks raw vector init success! name=" << vector_name_
            << ", block cache size=" << block_cache_size_ << "Bytes";

  return 0;
}

int RocksDBRawVector::GetVector(long vid, const float *&vec,
                                bool &deletable) const {
  if (vid >= ntotal_ || vid < 0) {
    return 1;
  }
  string key, value;
  ToRowKey((int)vid, key);
  Status s = db_->Get(ReadOptions(), Slice(key), &value);
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb get error:" << s.ToString() << ", key=" << key;
    return 2;
  }
  float *vector = new float[dimension_];
  assert((size_t)vector_byte_size_ == value.size());
  memcpy((void *)vector, value.data(), vector_byte_size_);
  vec = vector;
  deletable = true;
  return 0;
}

int RocksDBRawVector::AddToStore(float *v, int len) {
  if (v == nullptr || len != dimension_) return -1;

  string key;
  ToRowKey(ntotal_, key);
  Status s =
      db_->Put(WriteOptions(), Slice(key), Slice((char *)v, vector_byte_size_));
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb put error:" << s.ToString() << ", key=" << key;
    return -2;
  }
  return 0;
}

int RocksDBRawVector::GetVectorHeader(int start, int end, ScopeVector &vec) {
  if (start < 0 || start >= ntotal_ || start >= end) {
    return 1;
  }

  rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());
  string start_key, end_key;
  ToRowKey(start, start_key);
  ToRowKey(end, end_key);
  it->Seek(Slice(start_key));
  int num = end - start;
  float *vectors = new float[(uint64_t)dimension_ * num];
  for (int c = 0; c < num; c++, it->Next()) {
    if (!it->Valid()) {
      LOG(ERROR) << "rocksdb iterator error, count=" << c;
      delete it;
      return 2;
    }
    Slice value = it->value();
    assert(value.size_ == (size_t)vector_byte_size_);
    memcpy((void *)(vectors + (uint64_t)c * dimension_), value.data_,
           vector_byte_size_);
#ifdef DEBUG
    string expect_key;
    ToRowKey(c + start, expect_key);
    string key = it->key().ToString();
    if (key != expect_key) {
      LOG(ERROR) << "vid=" << c + start << ", invalid key=" << key
                 << ", expect=" << expect_key;
    }
#endif
  }
  delete it;
  vec.Set(vectors);
  return 0;
}

void RocksDBRawVector::ToRowKey(int vid, string &key) const {
  char data[11];
  snprintf(data, 11, "%010d", vid);
  key.assign(data, 10);
}

}  // namespace tig_gamma

#endif  // WITH_ROCKSDB
