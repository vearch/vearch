/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifdef WITH_ROCKSDB

#ifndef ROCKSDB_RAW_VECTOR_H_
#define ROCKSDB_RAW_VECTOR_H_

#include <string>
#include <vector>
#include "raw_vector.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace tig_gamma {

class RocksDBRawVector : public RawVector {
 public:
  RocksDBRawVector(const std::string &name, int dimension, int max_vector_size,
                   const std::string &root_path,
                   const StoreParams &store_params);
  ~RocksDBRawVector();
  /* RawVector */
  int Init() override;
  int AddToStore(float *v, int len) override;
  int GetVectorHeader(int start, int end, ScopeVector &vec) override;

 protected:
  int GetVector(long vid, const float *&vec, bool &deletable) const override;

 private:
  void ToRowKey(int vid, std::string &key) const;

 private:
  rocksdb::DB *db_;
  rocksdb::BlockBasedTableOptions table_options_;
  size_t block_cache_size_;
  RawVectorIO *raw_vector_io_;
  StoreParams *store_params_;
};
}  // namespace tig_gamma

#endif  // ROCKSDB_RAW_VECTOR_H_

#endif  // WITH_ROCKSDB
