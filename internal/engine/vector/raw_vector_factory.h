/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include "common/gamma_common_data.h"
#include "memory_raw_vector.h"
#include "raw_vector.h"
#include "vector/rocksdb_raw_vector.h"

namespace vearch {

class RawVectorFactory {
 public:
  static RawVector *Create(VectorMetaInfo *meta_info, VectorStorageType type,
                           const std::string &root_path,
                           StoreParams &store_params,
                           bitmap::BitmapManager *docids_bitmap) {
    RawVector *raw_vector = nullptr;
    switch (type) {
      case VectorStorageType::MemoryOnly:
        raw_vector = new MemoryRawVector(meta_info, root_path, store_params,
                                         docids_bitmap);
        break;
      case VectorStorageType::RocksDB:
        raw_vector = new RocksDBRawVector(meta_info, root_path, store_params,
                                          docids_bitmap);
        break;
      default:
        LOG(ERROR) << "invalid raw feature type:" << static_cast<int>(type);
        return nullptr;
    }
    if (meta_info->with_io_) {
      if (!raw_vector->InitIO().ok()) {
        LOG(ERROR) << "init raw vector io error";
        delete raw_vector;
        raw_vector = nullptr;
        return nullptr;
      }
    }
    return raw_vector;
  }
};

}  // namespace vearch
