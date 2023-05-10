/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifdef WITH_ROCKSDB

#pragma once

#include <string>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "vector/raw_vector.h"

namespace tig_gamma {

class RocksDBRawVectorIO;

class RocksDBRawVector : public RawVector {
 public:
  RocksDBRawVector(VectorMetaInfo *meta_info, const std::string &root_path,
                   const StoreParams &store_params,
                   bitmap::BitmapManager *docids_bitmap);
  ~RocksDBRawVector();
  /* RawVector */
  int InitStore(std::string &vec_name) override;
  int AddToStore(uint8_t *v, int len) override;
  int GetVectorHeader(int start, int n, ScopeVectors &vecs,
                      std::vector<int> &lens) override;
  int UpdateToStore(int vid, uint8_t *v, int len);

  size_t GetStoreMemUsage();

  int Gets(const std::vector<int64_t> &vids, ScopeVectors &vecs) const override;

 protected:
  int GetVector(long vid, const uint8_t *&vec, bool &deletable) const override;

 private:
  void ToRowKey(int vid, std::string &key) const;
  int Decompress(std::string &cmprs_data, uint8_t *&vec) const;

 private:
  friend class RocksDBRawVectorIO;

  rocksdb::DB *db_;
  rocksdb::BlockBasedTableOptions table_options_;
  size_t block_cache_size_;
  RawVectorIO *raw_vector_io_;
};
}  // namespace tig_gamma

#endif  // WITH_ROCKSDB
