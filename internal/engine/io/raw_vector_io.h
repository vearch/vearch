/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include "util/status.h"

namespace vearch {

struct RawVectorIO {
  virtual ~RawVectorIO(){};

  virtual Status Init() { return Status::OK(); }
  // [start_vid, end_vid)
  virtual Status Dump(int start_vid, int end_vid) = 0;
  virtual int GetDiskVecNum(int &vec_num) = 0;
  virtual Status Load(int vec_num) = 0;
  virtual Status Update(int vid) = 0;
};

}  // namespace vearch
