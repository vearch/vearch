/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "idl/fbs-gen/c/batch_result_generated.h"
#include "gamma_raw_data.h"

namespace tig_gamma {

class BatchResult : public RawData {
 public:
  BatchResult() {};

  explicit BatchResult(int size) {
    codes_.resize(size);
    std::fill(codes_.begin(), codes_.end(), 0);
    msgs_.resize(size);
  }

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  void SetResult(int i, int code, const std::string &msg) {
    codes_[i] = code;
    msgs_[i] = msg;
  }

  int Code(int i) const { return codes_[i]; }

  std::string Msg(int i) const { return msgs_[i]; }

 private:
  gamma_api::BatchResult *batch_result_;
  std::vector<int> codes_;
  std::vector<std::string> msgs_;
};

}  // namespace tig_gamma
