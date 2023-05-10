/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifdef WITH_ROCKSDB

#include "rocksdb_raw_vector_io.h"

namespace tig_gamma {

using std::string;
using namespace rocksdb;

int RocksDBRawVectorIO::GetDiskVecNum(int &vec_num) {
  if (vec_num <= 0) return 0;
  int max_id_in_disk = vec_num - 1;
  string key, value;
  for (int i = max_id_in_disk; i >= 0; --i) {
    raw_vector->ToRowKey(i, key);
    Status s = raw_vector->db_->Get(ReadOptions(), Slice(key), &value);
    if (s.ok()) {
      vec_num = i + 1;
      LOG(INFO) << "In the disk rocksdb vec_num=" << vec_num;
      return 0;
    }
  }
  vec_num = 0;
  LOG(INFO) << "In the disk rocksdb vec_num=" << vec_num;
  return 0;
}

int RocksDBRawVectorIO::Load(int vec_num) {
  if (vec_num == 0) return 0;
  string key, value;
  raw_vector->ToRowKey(vec_num - 1, key);
  Status s = raw_vector->db_->Get(ReadOptions(), Slice(key), &value);
  if (!s.ok()) {
    LOG(ERROR) << "load vectors, get error:" << s.ToString() << ", expected key=" << key;
    return INTERNAL_ERR;
  }
  raw_vector->MetaInfo()->size_ = vec_num;
  LOG(INFO) << "rocksdb load success! vec_num=" << vec_num;
  return 0;
}

}  // namespace tig_gamma

#endif // WITH_ROCKSDB
