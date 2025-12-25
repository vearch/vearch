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
  int refresh_interval;
  std::vector<struct FieldInfo> fields;
  std::vector<struct VectorInfo> vectors;
  std::string index_type;
  std::string index_params;
};

int TableSchemaIO::Write(TableInfo &table) {
  SchemaInfo s{
      .training_threshold = table.TrainingThreshold(),
      .refresh_interval = table.RefreshInterval(),
      .fields = table.Fields(),
      .vectors = table.VectorInfos(),
      .index_type = table.IndexType(),
      .index_params = table.IndexParams(),
  };

  nlohmann::json j;
  j["training_threshold"] = s.training_threshold;
  j["refresh_interval"] = s.refresh_interval;
  j["enable_id_cache"] = table.EnableIdCache();
  j["enable_realtime"] = table.EnableRealtime();
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
  std::ifstream schema_file(file_path, std::ios::ate);
  if (!schema_file.is_open()) {
    LOG(ERROR) << "Failed to open schema file: " << file_path;
    return -1;
  }

  std::streamsize size = schema_file.tellg();
  if (size <= 0) {
    LOG(ERROR) << "Empty schema file: " << file_path;
    return -1;
  }

  schema_file.seekg(0, std::ios::beg);
  std::string schema_str(size, '\0');
  schema_file.read(&schema_str[0], size);
  schema_file.close();
  LOG(DEBUG) << "schema_str=" << schema_str;

  try {
    nlohmann::json j = nlohmann::json::parse(schema_str);
    table.SetName(name);
    if (j.contains("training_threshold")) {
      table.SetTrainingThreshold(j["training_threshold"]);
    }
    if (j.contains("refresh_interval")) {
      table.SetRefreshInterval(j["refresh_interval"]);
    }
    if (j.contains("enable_id_cache")) {
      table.SetEnableIdCache(j["enable_id_cache"]);
    }
    if (j.contains("enable_realtime")) {
      table.SetEnableRealtime(j["enable_realtime"]);
    }
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
  } catch (const nlohmann::json::exception &e) {
    LOG(ERROR) << "JSON parse error: " << e.what()
               << ", content: " << schema_str;
    return -1;
  }
  return 0;
}

}  // namespace vearch
