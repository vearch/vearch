/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace tig_gamma {

class RawData {
 public:
  RawData() {}
  virtual ~RawData() {}
  virtual int Serialize(char **out, int *out_len) = 0;
  virtual void Deserialize(const char *data, int len) = 0;
};

}  // namespace tig_gamma
