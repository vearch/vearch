/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "idl/fbs-gen/c/config_generated.h"
#include "idl/fbs-gen/c/doc_generated.h"
#include "idl/fbs-gen/c/response_generated.h"
#include "idl/fbs-gen/c/table_generated.h"
#include "raw_data.h"

namespace vearch {

enum class DataType : std::uint16_t {
  INT = 0,
  LONG,
  FLOAT,
  DOUBLE,
  STRING,
  VECTOR
};

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
  TableInfo() {
    table_ = nullptr;
    compress_mode_ = 0;
  }

  virtual int Serialize(char **out, int *out_len);
  virtual void Deserialize(const char *data, int len);

  std::string &Name();

  void SetName(std::string &name);

  bool IsCompress() { return compress_mode_; }

  void SetCompress(int compress_mode) { compress_mode_ = compress_mode; }

  std::vector<struct FieldInfo> &Fields();

  void AddField(struct FieldInfo &field);

  std::vector<struct VectorInfo> &VectorInfos();

  void AddVectorInfo(struct VectorInfo &vector_info);

  int IndexingSize();

  void SetIndexingSize(int indexing_size);

  std::string &RetrievalType();

  void SetRetrievalType(std::string &retrieval_type);

  std::string &RetrievalParam();

  void SetRetrievalParam(std::string &retrieval_param);

  int Read(const std::string &path);

  int Write(const std::string &path);

 private:
  gamma_api::Table *table_;

  std::string name_;
  std::vector<struct FieldInfo> fields_;
  std::vector<struct VectorInfo> vectors_infos_;

  int indexing_size_;
  std::string retrieval_type_;
  std::string retrieval_param_;
  int compress_mode_;
};

}  // namespace vearch
