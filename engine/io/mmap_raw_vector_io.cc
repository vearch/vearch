/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "mmap_raw_vector_io.h"

#include "io/io_common.h"
#include "search/error_code.h"
#include "vector/memory_raw_vector.h"

namespace tig_gamma {

int MmapRawVectorIO::Init() { return 0; }

int MmapRawVectorIO::Dump(int start_vid, int end_vid) {
  int ret = raw_vector->storage_mgr_->Sync();
  LOG(INFO) << "MmapRawVector sync, doc num["
            << raw_vector->storage_mgr_->Size() << "]";
  return ret;
}

int MmapRawVectorIO::GetDiskVecNum(int &vec_num) {
  if (raw_vector->storage_mgr_ == nullptr) {
    vec_num = 0;
    LOG(ERROR) << "Mmap_raw_vector storage_mgr_ = nullptr";
    return 0;
  }
  vec_num = raw_vector->storage_mgr_->Size();
  LOG(INFO) << "Mmap_raw_vector storage_mgr_ vec_num=" << vec_num;
  return 0;
}

int MmapRawVectorIO::Load(int vec_num) {
  if (raw_vector->storage_mgr_->Truncate(vec_num)) {
    LOG(ERROR) << "truncate gamma db error, vec_num=" << vec_num;
    return INTERNAL_ERR;
  }
  raw_vector->MetaInfo()->size_ = vec_num;

  if (dynamic_cast<MemoryRawVector *>(raw_vector)) {
    MemoryRawVector *memory_vec = dynamic_cast<MemoryRawVector *>(raw_vector);
    std::vector<const uint8_t *> values;
    std::vector<int> lens;
    int ret = memory_vec->storage_mgr_->GetHeaders(0, vec_num, values, lens);
    if (ret != 0) {
      LOG(ERROR) << "Load mmap vector failed";
      return ret;
    }

    const StorageManagerOptions opt =
        raw_vector->storage_mgr_->GetStorageManagerOptions();

    int fixed_value_bytes = opt.fixed_value_bytes;
    for (size_t i = 0; i < lens.size(); ++i) {
      for (size_t j = 0; j < (size_t)lens[i]; ++j) {
        memory_vec->AddToStore(
            const_cast<uint8_t *>(values[i] + j * fixed_value_bytes),
            fixed_value_bytes);
      }
    }
  }
  LOG(INFO) << "mmap load success! vec num=" << vec_num;
  return 0;
}

int MmapRawVectorIO::Update(int vid) {
  return 0;  // do nothing
}

}  // namespace tig_gamma
