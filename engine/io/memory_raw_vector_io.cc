/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "memory_raw_vector_io.h"
#include "common/error_code.h"
#include "io_common.h"

namespace tig_gamma {

using std::string;
using std::vector;
using namespace rocksdb;

int MemoryRawVectorIO::Init() {
  const std::string &name = raw_vector->MetaInfo()->AbsoluteName();
  string db_path = raw_vector->RootPath() + "/" + name;
  if (rdb.Open(db_path)) {
    LOG(ERROR) << "open rocks db error, path=" << db_path;
    return IO_ERR;
  }
  return 0;
}

int MemoryRawVectorIO::Dump(int start_vid, int end_vid) {
  int ret = Until(end_vid, 1000 * 10);
  if (ret == TIMEOUT_ERR) {
    LOG(ERROR) << "dump wait async flusher timeout!";
    return TIMEOUT_ERR;
  }
  return 0;
}

int MemoryRawVectorIO::GetDiskVecNum(int &vec_num) {
  if (vec_num <= 0) return 0;
  int disk_vec_num = vec_num - 1;
  string key, value;
  for (int i = disk_vec_num; i >= 0; --i) {
    rdb.ToRowKey(i, key);
    Status s = rdb.db_->Get(ReadOptions(), Slice(key), &value);
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

int MemoryRawVectorIO::Load(int vec_num) {
  rocksdb::Iterator *it = rdb.db_->NewIterator(rocksdb::ReadOptions());
  utils::ScopeDeleter1<rocksdb::Iterator> del1(it);
  string start_key;
  rdb.ToRowKey(0, start_key);
  it->Seek(Slice(start_key));
  for (int c = 0; c < vec_num; c++, it->Next()) {
    if (!it->Valid()) {
      LOG(ERROR) << "load vectors error, expected num=" << vec_num
                 << ", current=" << c;
      return INTERNAL_ERR;
    }
    Slice value = it->value();
    raw_vector->AddToMem((uint8_t *)value.data_, raw_vector->VectorByteSize());
  }

  raw_vector->MetaInfo()->size_ = vec_num;
  Reset(vec_num);

  return 0;
}

int MemoryRawVectorIO::FlushOnce() {
  int size = raw_vector->MetaInfo()->size_;
  if (nflushed_ == size) return 0;
  for (int vid = nflushed_; vid < size; vid++) {
    if (Put(vid)) return -1;
  }
  nflushed_ = size;
  return 0;
}

int MemoryRawVectorIO::Put(int vid) {
  const uint8_t *vec = raw_vector->GetFromMem(vid);
  return rdb.Put(vid, (const char *)vec, raw_vector->VectorByteSize());
}

int MemoryRawVectorIO::Update(int vid) {
  int ret = Until(vid + 1, 1000 * 3);  // waiting until vid is flushed
  if (ret == TIMEOUT_ERR) {
    LOG(ERROR) << "update vector, wait async flush timeout";
    return TIMEOUT_ERR;
  }
  return Put(vid);
}

}  // namespace tig_gamma
