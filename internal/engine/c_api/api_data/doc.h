/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <unordered_map>
#include <vector>

#include "idl/fbs-gen/c/doc_generated.h"
#include "raw_data.h"
#include "table.h"

namespace vearch {

class Engine;

struct Field {
  std::string name;
  std::string value;
  DataType datatype;

  Field() = default;

  Field(const Field &other) = default;
  Field &operator=(const Field &other) = default;

  Field(Field &&other) noexcept = default;
  Field &operator=(Field &&other) noexcept = default;

  ~Field() = default;
};

class Doc : public RawData {
 public:
  Doc() : doc_(nullptr), engine_(nullptr) {}

  Doc(const Doc &other) = default;
  Doc &operator=(const Doc &other) = default;

  Doc(Doc &&other) noexcept = default;
  Doc &operator=(Doc &&other) noexcept = default;

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  void AddField(const Field &field);
  void AddField(Field &&field);

  std::string &Key() { return key_; }
  void SetKey(const std::string &key) { key_ = key; }

  std::unordered_map<std::string, Field> &TableFields() {
    return table_fields_;
  }
  std::unordered_map<std::string, Field> &VectorFields() {
    return vector_fields_;
  }

  void SetEngine(Engine *engine) { engine_ = engine; }

  std::string ToJson();

 private:
  gamma_api::Doc *doc_;
  std::string key_;
  std::unordered_map<std::string, Field> table_fields_;
  std::unordered_map<std::string, Field> vector_fields_;
  Engine *engine_;
};

}  // namespace vearch
