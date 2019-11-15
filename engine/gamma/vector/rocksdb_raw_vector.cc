/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifdef WITH_ROCKSDB

#include "rocksdb_raw_vector.h"
#include "log.h"
#include "rocksdb/table.h"
#include "utils.h"
#include <stdio.h>

using namespace std;
using namespace rocksdb;

namespace tig_gamma {

RocksDBRawVector::RocksDBRawVector(const std::string &name, int dimension,
                                   int max_vector_size,
                                   const std::string &root_path,
                                   const StoreParams &store_params)
    : RawVector(name, dimension, max_vector_size, root_path),
      AsyncFlusher(name) {
  root_path_ = root_path;
  db_ = nullptr;
  raw_vector_io_ = nullptr;
  store_params_ = new StoreParams(store_params);
}

RocksDBRawVector::~RocksDBRawVector() {
  if (db_) {
    delete db_;
  }
  if (raw_vector_io_)
    delete raw_vector_io_;
  if (store_params_)
    delete store_params_;
}

int RocksDBRawVector::Init() {
  block_cache_size_ = store_params_->cache_size_;

  raw_vector_io_ = new RawVectorIO(this);
  if (raw_vector_io_->Init(true)) {
    LOG(ERROR) << "init raw vector io error";
    return -1;
  }
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
  nflushed_ = ntotal_;
  LOG(INFO) << "rocks raw vector init success! name=" << vector_name_
            << ", flushed num=" << nflushed_
            << ", block cache size=" << block_cache_size_ << "m";

  return 0;
}

const float *RocksDBRawVector::GetVector(long vid) const {
  if (vid >= ntotal_ || vid < 0) {
    return nullptr;
  }
  string key, value;
  ToRowKey((int)vid, key);
  Status s = db_->Get(ReadOptions(), Slice(key), &value);
  if (!s.ok()) {
    LOG(ERROR) << "rocksdb get error:" << s.ToString() << ", key=" << key;
    return nullptr;
  }
  float *vector = new float[dimension_];
  assert((size_t)vector_byte_size_ == value.size());
  memcpy((void *)vector, value.data(), vector_byte_size_);
  return vector;
}

int RocksDBRawVector::FlushOnce() {
  int num = ntotal_ - nflushed_;
  if (num > 0) {
    raw_vector_io_->Dump(nflushed_, num);
  }
  return num;
}

int RocksDBRawVector::AddToStore(float *v, int len) {
  if (v == nullptr || len != dimension_)
    return -1;

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

const float *RocksDBRawVector::GetVectorHeader(int start, int end) {
  if (start < 0 || start >= ntotal_ || start >= end) {
    return nullptr;
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
      return nullptr;
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
  return vectors;
}

void RocksDBRawVector::Destroy(std::vector<const float *> &results) {
  for (const float *p : results) {
    delete[] p;
  }
}

void RocksDBRawVector::Destroy(const float *result, bool header) {
  delete[] result;
}

void RocksDBRawVector::ToRowKey(int vid, string &key) const {
  char data[11];
  snprintf(data, 11, "%010d", vid);
  key.assign(data, 10);
}

} // namespace tig_gamma

#endif // WITH_ROCKSDB
