/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifdef WITH_ROCKSDB

#ifndef ROCKSDB_RAW_VECTOR_H_
#define ROCKSDB_RAW_VECTOR_H_

#include "raw_vector.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include <string>
#include <vector>

namespace tig_gamma {

class RocksDBRawVector : public RawVector, public AsyncFlusher {
public:
  RocksDBRawVector(const std::string &name, int dimension, int max_vector_size,
                   const std::string &root_path,
                   const StoreParams &store_params);
  ~RocksDBRawVector();
  /* RawVector */
  int Init() override;
  const float *GetVector(long vid) const override;
  int AddToStore(float *v, int len) override;
  const float *GetVectorHeader(int start, int end) override;
  void Destroy(std::vector<const float *> &results) override;
  void Destroy(const float *result, bool header = false) override;

  /* AsyncFlusher */
  int FlushOnce() override;

private:
  void ToRowKey(int vid, std::string &key) const;

private:
  rocksdb::DB *db_;
  rocksdb::BlockBasedTableOptions table_options_;
  size_t block_cache_size_;
  RawVectorIO *raw_vector_io_;
  StoreParams *store_params_;
};
} // namespace tig_gamma

#endif // ROCKSDB_RAW_VECTOR_H_

#endif // WITH_ROCKSDB
