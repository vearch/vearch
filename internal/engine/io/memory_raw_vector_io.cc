/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "memory_raw_vector_io.h"

#include "io_common.h"

namespace vearch {

using std::string;
using std::vector;

Status MemoryRawVectorIO::Init() {
  const std::string &name = raw_vector->MetaInfo()->AbsoluteName();
  string db_path = raw_vector->RootPath() + "/" + name;
  Status status = rdb.Open(db_path);
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

Status MemoryRawVectorIO::Dump(int start_vid, int end_vid) {
  Status status = Until(end_vid, 1000 * 10);
  if (!status.ok()) {
    LOG(ERROR) << status.ToString();
    return status;
  }
  return Status::OK();
}

int MemoryRawVectorIO::GetDiskVecNum(int &vec_num) {
  if (vec_num <= 0) return 0;
  int disk_vec_num = vec_num - 1;
  string key, value;
  for (int i = disk_vec_num; i >= 0; --i) {
    rdb.ToRowKey(i, key);
    rocksdb::Status s =
        rdb.db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key), &value);
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

Status MemoryRawVectorIO::Load(int vec_num) {
  rocksdb::Iterator *it = rdb.db_->NewIterator(rocksdb::ReadOptions());
  utils::ScopeDeleter1<rocksdb::Iterator> del1(it);
  string start_key;
  rdb.ToRowKey(0, start_key);
  it->Seek(rocksdb::Slice(start_key));
  for (int c = 0; c < vec_num; c++, it->Next()) {
    if (!it->Valid()) {
      std::string msg = std::string("load vectors error, expected num=") +
                        std::to_string(vec_num) +
                        ", current=" + std::to_string(c);
      LOG(ERROR) << msg;
      return Status::IOError(msg);
    }
    rocksdb::Slice value = it->value();
    raw_vector->AddToMem((uint8_t *)value.data_, raw_vector->VectorByteSize());
  }

  raw_vector->MetaInfo()->size_ = vec_num;
  Reset(vec_num);

  return Status::OK();
}

Status MemoryRawVectorIO::FlushOnce() {
  int size = raw_vector->MetaInfo()->size_;
  if (nflushed_ == size) return Status::OK();
  for (int vid = nflushed_; vid < size; vid++) {
    Status status = Put(vid);
    if (!status.ok()) return status;
  }
  nflushed_ = size;
  return Status::OK();
}

Status MemoryRawVectorIO::Put(int vid) {
  const uint8_t *vec = raw_vector->GetFromMem(vid);
  return rdb.Put(vid, (const char *)vec, raw_vector->VectorByteSize());
}

Status MemoryRawVectorIO::Update(int vid) {
  Status status = Until(vid + 1, 1000 * 3);  // waiting until vid is flushed
  if (status.code() == status::kTimedOut) {
    LOG(ERROR) << "update vector, wait async flush timeout";
    return status;
  }
  return Put(vid);
}

}  // namespace vearch
