/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include "vector/mmap_raw_vector.h"
#include "raw_vector_io.h"

namespace tig_gamma {

struct MmapRawVectorIO : public RawVectorIO {
  RawVector *raw_vector;

  MmapRawVectorIO(RawVector *raw_vector_) : raw_vector(raw_vector_) {}
  ~MmapRawVectorIO() {}

  int Init() override;
  // [start_vid, end_vid)
  int Dump(int start_vid, int end_vid) override;
  int GetDiskVecNum(int &vec_num) override;
  int Load(int vec_num) override;
  int Update(int vid) override;
};

}  // namespace tig_gamma

