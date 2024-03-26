/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include "raw_vector_io.h"
#include "vector/rocksdb_raw_vector.h"

namespace vearch {

struct RocksDBRawVectorIO : public RawVectorIO {
  RocksDBRawVector *raw_vector;

  RocksDBRawVectorIO(RocksDBRawVector *raw_vector_) : raw_vector(raw_vector_) {}
  ~RocksDBRawVectorIO() {}
  Status Init() override { return Status::OK(); };
  Status Dump(int start_vid, int end_vid) override { return Status::OK(); };
  int GetDiskVecNum(int &vec_num) override;
  Status Load(int vec_num) override;
  Status Update(int vid) override { return Status::OK(); };
};

}  // namespace vearch
