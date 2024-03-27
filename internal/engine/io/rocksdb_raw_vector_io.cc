/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "rocksdb_raw_vector_io.h"

namespace vearch {

int RocksDBRawVectorIO::GetDiskVecNum(int &vec_num) {
  if (vec_num <= 0) return 0;
  int max_id_in_disk = vec_num - 1;
  std::string key, value;
  for (int i = max_id_in_disk; i >= 0; --i) {
    raw_vector->ToRowKey(i, key);
    rocksdb::Status s = raw_vector->db_->Get(rocksdb::ReadOptions(),
                                             rocksdb::Slice(key), &value);
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

Status RocksDBRawVectorIO::Load(int vec_num) {
  if (vec_num == 0) return Status::OK();
  std::string key, value;
  raw_vector->ToRowKey(vec_num - 1, key);
  rocksdb::Status s =
      raw_vector->db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key), &value);
  if (!s.ok()) {
    std::string msg = std::string("load vectors, get error:") + s.ToString() +
                      ", expected key=" + key;
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }
  raw_vector->MetaInfo()->size_ = vec_num;
  LOG(INFO) << "rocksdb load success! vec_num=" << vec_num;
  return Status::OK();
}

}  // namespace vearch
