/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "doc.h"

#include "search/engine.h"
#include "table/table.h"
#include "third_party/nlohmann/json.hpp"
#include "util/utils.h"

namespace vearch {
int Doc::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<gamma_api::Field>> field_vector;
  field_vector.reserve(table_fields_.size() + vector_fields_.size());

  for (const auto &fields : {table_fields_, vector_fields_}) {
    for (const auto &[name, f] : fields) {
      auto value = builder.CreateVector(
          reinterpret_cast<const uint8_t *>(f.value.data()), f.value.size());
      auto field =
          gamma_api::CreateField(builder, builder.CreateString(f.name), value,
                                 static_cast<::DataType>(f.datatype));
      field_vector.push_back(field);
    }
  }

  auto field_vec = builder.CreateVector(field_vector);
  auto doc = gamma_api::CreateDoc(builder, field_vec);
  builder.Finish(doc);

  *out_len = builder.GetSize();
  *out = static_cast<char *>(malloc(*out_len));
  memcpy(*out, builder.GetBufferPointer(), *out_len);

  return 0;
}

void Doc::Deserialize(const char *data, int len) {
  if (engine_ == nullptr) {
    LOG(ERROR) << "engine is null";
    return;
  }

  doc_ = const_cast<gamma_api::Doc *>(gamma_api::GetDoc(data));
  Table *table = engine_->GetTable();
  const auto &field_map = table->FieldMap();
  const auto fields_num = doc_->fields()->size();

  for (size_t i = 0; i < fields_num; ++i) {
    auto f = doc_->fields()->Get(i);
    Field field;
    field.name = f->name()->str();
    field.value = std::string(
        reinterpret_cast<const char *>(f->value()->Data()), f->value()->size());
    field.datatype = static_cast<DataType>(f->data_type());

    if (field.name == "_id") {
      key_ = field.value;
    }

    if (field.datatype != DataType::VECTOR) {
      if (field_map.find(field.name) == field_map.end()) {
        LOG(ERROR) << "Unknown field " << field.name;
        continue;
      }
      table_fields_[field.name] = std::move(field);
    } else {
      vector_fields_[field.name] = std::move(field);
    }
  }
}

void Doc::AddField(const Field &field) {
  if (field.datatype == DataType::VECTOR) {
    vector_fields_[field.name] = field;
  } else {
    table_fields_[field.name] = field;
  }
}

void Doc::AddField(Field &&field) {
  if (field.datatype == DataType::VECTOR) {
    vector_fields_[field.name] = std::move(field);
  } else {
    table_fields_[field.name] = std::move(field);
  }
}

std::string Doc::ToJson() {
  nlohmann::json j;
  j["_id"] = key_;
  for (const auto &fields : {table_fields_, vector_fields_}) {
    for (const auto &[name, f] : fields) {
      if (f.datatype == DataType::STRING) {
        j[name] = f.value;
      } else if (f.datatype == DataType::STRINGARRAY) {
        std::vector<std::string> items = utils::split(f.value, "\001");
        j[name] = items;
      } else if (f.datatype == DataType::INT) {
        int v;
        memcpy(&v, f.value.data(), sizeof(v));
        j[name] = v;
      } else if (f.datatype == DataType::LONG) {
        long v;
        memcpy(&v, f.value.data(), sizeof(v));
        j[name] = v;
      } else if (f.datatype == DataType::FLOAT) {
        float v;
        memcpy(&v, f.value.data(), sizeof(v));
        j[name] = v;
      } else if (f.datatype == DataType::DOUBLE) {
        double v;
        memcpy(&v, f.value.data(), sizeof(v));
        j[name] = v;
      } else if (f.datatype == DataType::VECTOR) {
        std::vector<float> v;
        v.resize(f.value.size() / sizeof(float));
        memcpy(v.data(), f.value.data(), f.value.size());
        j[name] = v;
      }
    }
  }
  return j.dump();
}

int Doc::FromJson(std::string &raw,
                  std::map<std::string, DataType> &attr_type_map,
                  std::unordered_set<std::string> &raw_vectors) {
  try {
    nlohmann::json json_object = nlohmann::json::parse(raw);
    for (auto &element : json_object.items()) {
      std::string key = element.key();
      if (key == "_id") {
        key_ = element.value();
      }
      auto it = attr_type_map.find(key);
      auto vec_it = raw_vectors.find(key);
      DataType type;
      if (it != attr_type_map.end()) {
        type = it->second;
      } else if (vec_it != raw_vectors.end()) {
        type = DataType::VECTOR;
      } else {
        LOG(ERROR) << "cannot find key " << key;
        continue;
      }

      if (type == DataType::STRING) {
        if (!element.value().is_string()) {
          continue;
        }
        Field f;
        f.datatype = DataType::STRING;
        f.name = key;
        f.value = element.value();
        table_fields_[f.name] = std::move(f);
      } else if (type == DataType::STRINGARRAY) {
        if (!element.value().is_array()) {
          continue;
        }
        Field f;
        f.datatype = DataType::STRINGARRAY;
        f.name = key;

        for (std::size_t i = 0; i < element.value().size(); ++i) {
          if (i == 0) {
            f.value = element.value()[i];
          } else {
            f.value = "\001" + element.value()[i].get<std::string>();
          }
        }
        table_fields_[f.name] = std::move(f);
      } else if (type == DataType::INT) {
        if (!element.value().is_number_integer()) {
          continue;
        }
        int value = element.value().get<int>();
        Field f;
        f.datatype = DataType::INT;
        f.name = key;
        f.value =
            std::string(reinterpret_cast<const char *>(&value), sizeof(value));
        table_fields_[f.name] = std::move(f);
      } else if (type == DataType::LONG) {
        if (!element.value().is_number_integer()) {
          continue;
        }
        long value = element.value().get<long>();
        Field f;
        f.datatype = DataType::LONG;
        f.name = key;
        f.value =
            std::string(reinterpret_cast<const char *>(&value), sizeof(value));
        table_fields_[f.name] = std::move(f);
      } else if (type == DataType::FLOAT) {
        if (!element.value().is_number_float()) {
          continue;
        }
        float value = element.value().get<float>();
        Field f;
        f.datatype = DataType::FLOAT;
        f.name = key;
        f.value =
            std::string(reinterpret_cast<const char *>(&value), sizeof(value));
        table_fields_[f.name] = std::move(f);
      } else if (type == DataType::DOUBLE) {
        if (!element.value().is_number_float()) {
          continue;
        }
        double value = element.value().get<double>();
        Field f;
        f.datatype = DataType::DOUBLE;
        f.name = key;
        f.value =
            std::string(reinterpret_cast<const char *>(&value), sizeof(value));
        table_fields_[f.name] = std::move(f);
      } else if (type == DataType::VECTOR) {
        if (!element.value().is_array()) {
          continue;
        }
        Field f;
        f.datatype = DataType::VECTOR;
        f.name = key;
        bool all_floats =
            std::all_of(element.value().begin(), element.value().end(),
                        [](const nlohmann::json &element) {
                          return element.is_number_float();
                        });

        if (all_floats) {
          std::vector<float> float_vector =
              element.value().get<std::vector<float>>();
          f.value =
              std::string(reinterpret_cast<const char *>(float_vector.data()),
                          sizeof(float) * float_vector.size());
        } else {
          LOG(ERROR) << "Not all elements are floats.";
          continue;
        }
        vector_fields_[key] = std::move(f);
      }
    }
  } catch (nlohmann::json::parse_error &e) {
    LOG(ERROR) << "JSON parse error: " << e.what();
    return 1;
  }

  return 0;
}
}  // namespace vearch
