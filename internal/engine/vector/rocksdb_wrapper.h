/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace vearch {

struct RocksDBWrapper {
  rocksdb::DB *db_;
  rocksdb::BlockBasedTableOptions table_options_;

  RocksDBWrapper();
  ~RocksDBWrapper();
  int Open(std::string db_path, size_t block_cache_size = 0);
  int Put(int key, const char *v, size_t len);
  int Put(const std::string &key, const char *v, size_t len);
  int Put(const std::string &key, const std::string &value);
  void ToRowKey(int key, std::string &key_str);
};

}  // namespace vearch
