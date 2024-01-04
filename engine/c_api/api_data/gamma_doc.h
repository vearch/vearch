/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <unordered_map>
#include <vector>

#include "gamma_raw_data.h"
#include "gamma_table.h"
#include "idl/fbs-gen/c/doc_generated.h"

namespace tig_gamma {

class GammaEngine;

struct Field {
  std::string name;
  std::string value;
  std::string source;
  DataType datatype;

  Field() = default;

  Field(const Field &other)
      : name(other.name),
        value(other.value),
        source(other.source),
        datatype(other.datatype) {}

  Field &operator=(const Field &other) {
    name = other.name;
    value = other.value;
    source = other.source;
    datatype = other.datatype;
    return *this;
  }

  Field(Field &&other) noexcept
      : name(std::move(other.name)),
        value(std::move(other.value)),
        source(std::move(other.source)),
        datatype(std::move(other.datatype)) {}

  Field &operator=(Field &&other) {
    name = std::move(other.name);
    value = std::move(other.value);
    source = std::move(other.source);
    datatype = other.datatype;
    return *this;
  }

  ~Field() = default;
};

class Doc : public RawData {
 public:
  Doc() {
    doc_ = nullptr;
    engine_ = nullptr;
  }

  Doc(const Doc &other) { *this = other; }

  Doc &operator=(const Doc &other) {
    key_ = other.key_;

    table_fields_ = other.table_fields_;
    vector_fields_ = other.vector_fields_;
    return *this;
  }

  Doc(Doc &&other) { *this = std::move(other); }

  Doc &operator=(Doc &&other) {
    key_ = std::move(other.key_);

    table_fields_ = other.table_fields_;
    other.table_fields_.clear();
    vector_fields_ = other.vector_fields_;
    other.vector_fields_.clear();

    return *this;
  }

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  void AddField(const struct Field &field);

  void AddField(struct Field &&field);

  std::string &Key() { return key_; }

  void SetKey(const std::string &key) { key_ = key; }

  std::unordered_map<std::string, struct Field> &TableFields() {
    return table_fields_;
  }

  std::unordered_map<std::string, struct Field> &VectorFields() {
    return vector_fields_;
  }

  void SetEngine(GammaEngine *engine) { engine_ = engine; }

 private:
  gamma_api::Doc *doc_;
  std::string key_;

  std::unordered_map<std::string, struct Field> table_fields_;
  std::unordered_map<std::string, struct Field> vector_fields_;

  GammaEngine *engine_;
};

}  // namespace tig_gamma
