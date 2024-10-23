/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace vearch {

class RawData {
 public:
  RawData() {}
  virtual ~RawData() {}
  virtual int Serialize(char **out, int *out_len) = 0;
  virtual void Deserialize(const char *data, int len) = 0;
  virtual std::string RequestId() { return request_id_; }
  virtual void SetRequestId(const std::string &request_id) {
    request_id_ = request_id;
  }

 protected:
  std::string request_id_;
};

}  // namespace vearch
