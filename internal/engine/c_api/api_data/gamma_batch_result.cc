/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_batch_result.h"

#include "util/log.h"
#include "util/utils.h"

namespace tig_gamma {

int BatchResult::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<int32_t> codes_vector(codes_.size());
  std::vector<flatbuffers::Offset<flatbuffers::String>> msgs_vector(
      msgs_.size());

  int i = 0;
  for (int code : codes_) {
    codes_vector[i++] = code;
  }

  i = 0;
  for (std::string &msg : msgs_) {
    msgs_vector[i++] = builder.CreateString(msg);
  }

  auto batch_result =
      gamma_api::CreateBatchResult(builder, builder.CreateVector(codes_vector),
                                   builder.CreateVector(msgs_vector));
  builder.Finish(batch_result);
  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);
  return 0;
}

void BatchResult::Deserialize(const char *data, int len) {
  batch_result_ =
      const_cast<gamma_api::BatchResult *>(gamma_api::GetBatchResult(data));
  size_t codes_num = batch_result_->codes()->size();
  size_t msgs_num = batch_result_->msgs()->size();
  if (codes_num != msgs_num) {
    LOG(ERROR) << "codes_num [" << codes_num << "] != msgs_num [" << msgs_num
               << "]";
    return;
  }

  codes_.resize(codes_num);
  msgs_.resize(msgs_num);

  for (size_t i = 0; i < codes_num; ++i) {
    codes_[i] = batch_result_->codes()->Get(i);
    msgs_[i] = batch_result_->msgs()->Get(i)->str();
  }
}

}  // namespace tig_gamma