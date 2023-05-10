/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "idl/fbs-gen/c/doc_generated.h"
#include "gamma_raw_data.h"
#include "gamma_table.h"

namespace tig_gamma {

class GammaEngine;

struct Field {
  std::string name;
  std::string value;
  std::string source;
  DataType datatype;

  Field() {}

  Field(const Field &other) { *this = other; }

  Field &operator=(const Field &other) {
    name = other.name;
    value = other.value;
    source = other.source;
    datatype = other.datatype;
    return *this;
  }

  Field(Field &&other) { *this = std::move(other); }

  Field &operator=(Field &&other) {
    name = std::move(other.name);
    value = std::move(other.value);
    source = std::move(other.source);
    datatype = other.datatype;
    return *this;
  }
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

    table_fields_.reserve(other.table_fields_.size());
    for (const auto &f : other.table_fields_) {
      table_fields_.push_back(f);
    }

    vector_fields_.reserve(other.vector_fields_.size());
    for (const auto &f : other.vector_fields_) {
      vector_fields_.push_back(f);
    }
    return *this;
  }

  Doc(Doc &&other) { *this = std::move(other); }

  Doc &operator=(Doc &&other) {
    key_ = std::move(other.key_);

    table_fields_.resize(other.table_fields_.size());
    for (size_t i = 0; i < other.table_fields_.size(); ++i) {
      table_fields_[i] = std::move(other.table_fields_[i]);
    }
    other.table_fields_.clear();

    vector_fields_.resize(other.vector_fields_.size());
    for (size_t i = 0; i < other.vector_fields_.size(); ++i) {
      vector_fields_[i] = std::move(other.vector_fields_[i]);
    }
    other.vector_fields_.clear();

    return *this;
  }

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  void AddField(const struct Field &field);

  void AddField(struct Field &&field);

  std::string &Key() { return key_; }
  
  void SetKey(const std::string &key) { key_ = key; }

  std::vector<struct Field> &TableFields() { return table_fields_; }

  std::vector<struct Field> &VectorFields() { return vector_fields_; }

  void SetEngine(GammaEngine *engine) { engine_ = engine; }

 private:
  gamma_api::Doc *doc_;
  std::string key_;

  std::vector<struct Field> table_fields_;
  std::vector<struct Field> vector_fields_;

  GammaEngine *engine_;
};

}  // namespace tig_gamma
