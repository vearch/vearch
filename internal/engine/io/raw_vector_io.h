/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace tig_gamma {

struct RawVectorIO {
  virtual ~RawVectorIO(){};
  
  virtual int Init() { return 0; }
  // [start_vid, end_vid)
  virtual int Dump(int start_vid, int end_vid) = 0;
  virtual int GetDiskVecNum(int &vec_num) = 0;
  virtual int Load(int vec_num) = 0;
  virtual int Update(int vid) = 0;
};

}  // namespace tig_gamma
