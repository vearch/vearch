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
#include "vector/memory_buffer_raw_vector.h"
#include "raw_vector.h"
#include "vector/rocksdb_raw_vector.h"

namespace vearch {

class RawVectorFactory {
 public:
  static RawVector *Create(VectorMetaInfo *meta_info, VectorStorageType type,
                           StoreParams &store_params,
                           bitmap::BitmapManager *docids_bitmap, int cf_id,
                           StorageManager *storage_mgr) {
    RawVector *raw_vector = nullptr;
    switch (type) {
      case VectorStorageType::MemoryOnly:
        raw_vector = new MemoryRawVector(meta_info, store_params, docids_bitmap,
                                         storage_mgr, cf_id);
        break;
      case VectorStorageType::MemoryBuffer:
        raw_vector = new MemoryBufferRawVector(meta_info, store_params, docids_bitmap,
                                         storage_mgr, cf_id);
        break;
      case VectorStorageType::RocksDB:
        raw_vector = new RocksDBRawVector(meta_info, store_params,
                                          docids_bitmap, storage_mgr, cf_id);
        break;
      default:
        LOG(ERROR) << "invalid raw feature type:" << static_cast<int>(type);
        return nullptr;
    }
    return raw_vector;
  }
};

}  // namespace vearch
