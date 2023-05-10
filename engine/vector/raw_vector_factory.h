/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "memory_raw_vector.h"
#include "mmap_raw_vector.h"
#include "raw_vector.h"

#ifdef WITH_ROCKSDB
#include "io/rocksdb_raw_vector_io.h"
#include "vector/rocksdb_raw_vector.h"
#endif  // WITH_ROCKSDB

#include <string>

#include "common/gamma_common_data.h"
#include "io/mmap_raw_vector_io.h"

namespace tig_gamma {

static void Fail(RawVector *raw_vector, RawVectorIO *vio, std::string err_msg) {
  LOG(ERROR) << err_msg;
  if (raw_vector) delete raw_vector;
  if (vio) delete vio;
}

class RawVectorFactory {
 public:
  static RawVector *Create(VectorMetaInfo *meta_info, VectorStorageType type,
                           const std::string &root_path,
                           StoreParams &store_params,
                           bitmap::BitmapManager *docids_bitmap) {
    RawVector *raw_vector = nullptr;
    RawVectorIO *vio = nullptr;
    switch (type) {
      case VectorStorageType::MemoryOnly:
        raw_vector = new MemoryRawVector(meta_info, root_path, store_params,
                                         docids_bitmap);
        vio = new MmapRawVectorIO(raw_vector);
        break;
      case VectorStorageType::Mmap:
        raw_vector = new MmapRawVector(meta_info, root_path, store_params,
                                       docids_bitmap);
        vio = new MmapRawVectorIO(raw_vector);
        break;
#ifdef WITH_ROCKSDB
      case VectorStorageType::RocksDB:
        raw_vector = new RocksDBRawVector(meta_info, root_path, store_params,
                                          docids_bitmap);
        if (meta_info->with_io_)
            vio = new RocksDBRawVectorIO((RocksDBRawVector *)raw_vector);
        break;
#endif  // WITH_ROCKSDB
      default:
        LOG(ERROR) << "invalid raw feature type:" << static_cast<int>(type);
        return nullptr;
    }
    if (meta_info->with_io_) {
        if (vio && vio->Init()) {
            Fail(raw_vector, vio, "init raw vector io error");
            return nullptr;
        }
        raw_vector->SetIO(vio);
    }
    return raw_vector;
  }
};

}  // namespace tig_gamma
