/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "table_io.h"

#include <fstream>

#include "third_party/nlohmann/json.hpp"
#include "util/log.h"

namespace vearch {

TableSchemaIO::TableSchemaIO(std::string &file_path) : file_path(file_path) {}

TableSchemaIO::~TableSchemaIO() {}

struct SchemaInfo {
  int training_threshold;
  std::vector<struct FieldInfo> fields;
  std::vector<struct VectorInfo> vectors;
  std::string index_type;
  std::string index_params;
};

int TableSchemaIO::Write(TableInfo &table) {
  SchemaInfo s{
      .training_threshold = table.TrainingThreshold(),
      .fields = table.Fields(),
      .vectors = table.VectorInfos(),
      .index_type = table.IndexType(),
      .index_params = table.IndexParams(),
  };

  nlohmann::json j;
  j["training_threshold"] = s.training_threshold;
  for (auto &f : s.fields) {
    nlohmann::json jf;
    jf["name"] = f.name;
    jf["data_type"] = f.data_type;
    jf["is_index"] = f.is_index;
    j["fields"].push_back(jf);
  }
  for (auto &v : s.vectors) {
    nlohmann::json jv;
    jv["name"] = v.name;
    jv["data_type"] = v.data_type;
    jv["is_index"] = v.is_index;
    jv["dimension"] = v.dimension;
    jv["store_type"] = v.store_type;
    jv["store_param"] = v.store_param;
    j["vectors"].push_back(jv);
  }
  j["index_type"] = s.index_type;
  j["index_params"] = s.index_params;

  std::string schema_str = j.dump(2);
  std::ofstream schema_file(file_path);
  if (!schema_file.is_open()) {
    LOG(INFO) << "open error, file path=" << file_path;
    return -1;
  }
  schema_file.write(schema_str.c_str(), schema_str.length());
  schema_file.close();
  return 0;
}

int TableSchemaIO::Read(std::string &name, TableInfo &table) {
  std::string schema_str;
  std::ifstream schema_file(file_path, std::ios::ate);
  std::streamsize size = schema_file.tellg();
  schema_file.seekg(0, std::ios::beg);
  schema_file.read((char *)schema_str.c_str(), size);
  nlohmann::json j;
  j = nlohmann::json::parse(schema_str);
  std::string table_name = j["name"];
  table.SetName(table_name);
  table.SetTrainingThreshold(j["training_threshold"]);
  for (auto &f : j["fields"]) {
    struct FieldInfo fi;
    fi.name = f["name"];
    fi.data_type = f["data_type"];
    fi.is_index = f["is_index"];
    table.AddField(fi);
  }
  for (auto &v : j["vectors"]) {
    struct VectorInfo vi;
    vi.name = v["name"];
    vi.data_type = v["data_type"];
    vi.is_index = v["is_index"];
    vi.dimension = v["dimension"];
    vi.store_type = v["store_type"];
    vi.store_param = v["store_param"];
    table.AddVectorInfo(vi);
  }

  std::string index_type = j["index_type"];
  table.SetIndexType(index_type);
  std::string index_params = j["index_params"];
  table.SetIndexParams(index_params);
  return 0;
}

}  // namespace vearch
