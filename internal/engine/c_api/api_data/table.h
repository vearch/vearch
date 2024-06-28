/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "idl/fbs-gen/c/doc_generated.h"
#include "idl/fbs-gen/c/response_generated.h"
#include "idl/fbs-gen/c/table_generated.h"
#include "idl/fbs-gen/c/types_generated.h"
#include "raw_data.h"

namespace vearch {

struct VectorInfo {
  std::string name;
  DataType data_type;
  bool is_index;
  int dimension;
  std::string store_type;
  std::string store_param;
};

struct FieldInfo {
  std::string name;
  DataType data_type;
  bool is_index;
};

class TableInfo : public RawData {
 public:
  TableInfo() { table_ = nullptr; }

  virtual int Serialize(char **out, int *out_len);
  virtual void Deserialize(const char *data, int len);

  std::string &Name();

  void SetName(std::string &name);

  std::vector<struct FieldInfo> &Fields();

  void AddField(struct FieldInfo &field);

  std::vector<struct VectorInfo> &VectorInfos();

  void AddVectorInfo(struct VectorInfo &vector_info);

  int TrainingThreshold();

  void SetTrainingThreshold(int training_threshold);

  std::string &IndexType();

  void SetIndexType(std::string &index_type);

  std::string &IndexParams();

  void SetIndexParams(std::string &index_params);

  int Read(const std::string &path);

  int Write(const std::string &path);

 private:
  gamma_api::Table *table_;

  std::string name_;
  std::vector<struct FieldInfo> fields_;
  std::vector<struct VectorInfo> vectors_infos_;

  int training_threshold_ = 0;
  std::string index_type_;
  std::string index_params_;
};

}  // namespace vearch
