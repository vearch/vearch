/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_doc.h"

#include "search/gamma_engine.h"
#include "table/table.h"

namespace tig_gamma {

int Doc::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<gamma_api::Field>> field_vector(
      table_fields_.size() + vector_fields_.size());

  int i = 0;
  for (const auto &fields : {table_fields_, vector_fields_}) {
    for (const struct Field &f : fields) {
      std::vector<uint8_t> value(f.value.size());
      memcpy(value.data(), f.value.data(), f.value.size());

      auto field = gamma_api::CreateField(
          builder, builder.CreateString(f.name), builder.CreateVector(value),
          builder.CreateString(f.source), static_cast<::DataType>(f.datatype));
      field_vector[i++] = field;
    }
  }
  auto field_vec = builder.CreateVector(field_vector);
  auto doc = gamma_api::CreateDoc(builder, field_vec);
  builder.Finish(doc);
  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);
  return 0;
}

void Doc::Deserialize(const char *data, int len) {
  if (engine_ == nullptr) {
    LOG(ERROR) << "engine is null";
    return;
  }

  doc_ = const_cast<gamma_api::Doc *>(gamma_api::GetDoc(data));
  Table *table = engine_->GetTable();
  VectorManager *vec_manager = engine_->GetVectorManager();
  const std::map<std::string, int> &field_map = table->FieldMap();
  int table_field_num = table->FieldsNum();
  int vector_field_num = vec_manager->RawVectors().size();
  size_t fields_num = doc_->fields()->size();

  if (fields_num != (size_t)(table_field_num + vector_field_num)) {
    LOG(WARNING) << "Add Doc fields num [" << fields_num
                 << "], not equal to table_field_num [" << table_field_num
                 << "] + vector_field_num [" << vector_field_num << "]";
    return;
  }

  table_fields_.resize(table_field_num);
  vector_fields_.resize(vector_field_num);

  int vector_idx = 0;

  for (size_t i = 0; i < fields_num; ++i) {
    auto f = doc_->fields()->Get(i);
    struct Field field;
    field.name = f->name()->str();
    field.value = std::string(
        reinterpret_cast<const char *>(f->value()->Data()), f->value()->size());

    if (field.name == "_id") {
      key_ = field.value;
    }
    field.source = f->source()->str();
    field.datatype = static_cast<DataType>(f->data_type());

    if (field.datatype != DataType::VECTOR) {
      auto it = field_map.find(field.name);
      if (it == field_map.end()) {
        LOG(ERROR) << "Unknown field " << field.name;
        continue;
      }
      int field_idx = it->second;
      table_fields_[field_idx] = std::move(field);
    } else {
      vector_fields_[vector_idx++] = std::move(field);
    }
  }
}

void Doc::AddField(const struct Field &field) {
  if (field.datatype == DataType::VECTOR) {
    vector_fields_.push_back(field);
  } else {
    table_fields_.push_back(field);
  }
}

void Doc::AddField(struct Field &&field) {
  if (field.datatype == DataType::VECTOR) {
    vector_fields_.emplace_back(std::forward<struct Field>(field));
  } else {
    table_fields_.emplace_back(std::forward<struct Field>(field));
  }
}

}  // namespace tig_gamma
