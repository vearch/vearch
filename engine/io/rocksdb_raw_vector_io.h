/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifdef WITH_ROCKSDB

#pragma once

#include <string>
#include "raw_vector_io.h"
#include "vector/rocksdb_raw_vector.h"

namespace tig_gamma {

struct RocksDBRawVectorIO : public RawVectorIO {
  RocksDBRawVector *raw_vector;

  RocksDBRawVectorIO(RocksDBRawVector *raw_vector_) : raw_vector(raw_vector_) {}
  ~RocksDBRawVectorIO() {}
  int Init() override { return 0; };
  int Dump(int start_vid, int end_vid) override { return 0; };
  int GetDiskVecNum(int &vec_num) override;
  int Load(int vec_num) override;
  int Update(int vid) override { return 0; };
};

}  // namespace tig_gamma

#endif // WITH_ROCKSDB
