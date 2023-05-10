/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_response.h"
#include "c_api/api_data/gamma_doc.h"
#include "table/table.h"
#include "vector/vector_manager.h"
#include "util/log.h"
#include "util/utils.h"

namespace tig_gamma {

Response::Response() { 
  response_ = nullptr;
#ifdef PERFORMANCE_TESTING
  perf_tool_ = (void *)new PerfTool();
#endif
}

Response::~Response() { 
#ifdef PERFORMANCE_TESTING
  if(perf_tool_) {
    PerfTool *perf_tool = static_cast<PerfTool *>(perf_tool_);
    delete perf_tool;
    perf_tool_ = nullptr; 
  }
#endif
}

int Response::Serialize(std::vector<std::string> &fields_name, char **out,
                        int *out_len) {
  std::vector<std::string> vec_fields;
  std::map<std::string, int> attr_idx;
  Table *table = static_cast<Table *>(table_);
  VectorManager *vector_mgr = static_cast<VectorManager *>(vector_mgr_);
  const auto &attr_idx_map = table->FieldMap();

  if (fields_name.size()) {
    for (size_t i = 0; i < fields_name.size(); ++i) {
      std::string &name = fields_name[i];
      if (vector_mgr->Contains(name)) {
        vec_fields.emplace_back(name);
      } else {
        const auto &it = attr_idx_map.find(name);
        if (it != attr_idx_map.end()) {
          attr_idx.insert(std::make_pair(name, it->second));
        }
      }
    }
  } else {
    attr_idx = attr_idx_map;
  }

  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<gamma_api::SearchResult>> search_results;
  for (int i = 0; i < req_num_; ++i) {
    std::vector<flatbuffers::Offset<gamma_api::ResultItem>> result_items;
    for (int j = 0; j < gamma_results_[i].results_count; ++j) {
      VectorDoc *vec_doc = gamma_results_[i].docs[j];
      int docid = vec_doc->docid;
      double score = vec_doc->score;

      const uint8_t *doc_buffer = table->GetDocBuffer(docid);
      if (doc_buffer == nullptr) { 
        LOG(ERROR) << "docid[" << docid << "] table doc_buffer is nullptr";
        continue;
      }

      std::vector<flatbuffers::Offset<gamma_api::Attribute>> attributes;
      for (auto &it : attr_idx) {
        std::vector<uint8_t> val;
        table->GetFieldRawValue(docid, it.second, val, doc_buffer);

        attributes.emplace_back(gamma_api::CreateAttribute(
            builder, builder.CreateString(it.first),
            builder.CreateVector(val)));
      }
      delete[] doc_buffer;
      for (uint32_t k = 0; k < vec_fields.size(); ++k) {
        std::vector<uint8_t> vec;
        if (vector_mgr->GetDocVector(docid, vec_fields[k], vec) == 0) {
          attributes.emplace_back(gamma_api::CreateAttribute(
              builder, builder.CreateString(vec_fields[k]),
              builder.CreateVector(vec)));
        }
      }
      std::string extra;
      if (vec_fields.size() > 0 && vec_doc->fields_len > 0) {
        cJSON *extra_json = cJSON_CreateObject();
        cJSON *vec_result_json = cJSON_CreateArray();
        cJSON_AddItemToObject(extra_json, EXTRA_VECTOR_RESULT.c_str(),
                              vec_result_json);
        for (int k = 0; k < vec_doc->fields_len; ++k) {
          VectorDocField *vec_field = vec_doc->fields + k;
          cJSON *vec_field_json = cJSON_CreateObject();

          cJSON_AddStringToObject(vec_field_json, EXTRA_VECTOR_FIELD_NAME.c_str(),
                                  vec_field->name.c_str());
          string source = string(vec_field->source, vec_field->source_len);
          cJSON_AddStringToObject(vec_field_json, EXTRA_VECTOR_FIELD_SOURCE.c_str(),
                                  source.c_str());
          cJSON_AddNumberToObject(vec_field_json, EXTRA_VECTOR_FIELD_SCORE.c_str(),
                                  vec_field->score);
          cJSON_AddItemToArray(vec_result_json, vec_field_json);
        }

        char *extra_data = cJSON_PrintUnformatted(extra_json);
        extra = std::string(extra_data, std::strlen(extra_data));
        free(extra_data);
        cJSON_Delete(extra_json);
      }

      result_items.emplace_back(gamma_api::CreateResultItem(
          builder, score, builder.CreateVector(attributes),
          builder.CreateString(extra)));
    }

    std::string str_msg = "Success";
    auto item_vec = builder.CreateVector(result_items);
    auto msg = builder.CreateString(str_msg);

    gamma_api::SearchResultCode result_code = gamma_api::SearchResultCode::SUCCESS;
    auto results = gamma_api::CreateSearchResult(builder, gamma_results_[i].total,
                                                 result_code, msg, item_vec);
    search_results.push_back(results);
  }

  auto result_vec = builder.CreateVector(search_results);

  flatbuffers::Offset<flatbuffers::String> message =
      builder.CreateString(online_log_message_);
  auto res = gamma_api::CreateResponse(builder, result_vec, message);
  builder.Finish(res);

  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);
  delete[] gamma_results_;
  gamma_results_ = nullptr;
#ifdef PERFORMANCE_TESTING
  PerfTool *perf_tool = static_cast<PerfTool *>(perf_tool_);
  perf_tool->Perf("serialize total");
  LOG(INFO) << perf_tool->OutputPerf().str();
#endif
  return 0;
}

void Response::Deserialize(const char *data, int len) {
  assert(response_ == nullptr);
  response_ = const_cast<gamma_api::Response *>(gamma_api::GetResponse(data));
  size_t result_num = response_->results()->size();
  results_.resize(result_num);

  for (size_t i = 0; i < result_num; ++i) {
    SearchResult result;
    auto fbs_result = response_->results()->Get(i);
    result.total = fbs_result->total();
    result.result_code =
        static_cast<SearchResultCode>(fbs_result->result_code());
    result.msg = fbs_result->msg()->str();

    size_t items_num = fbs_result->result_items()->size();
    result.result_items.resize(items_num);

    for (size_t j = 0; j < items_num; ++j) {
      auto fbs_result_item = fbs_result->result_items()->Get(j);
      struct ResultItem item;
      item.score = fbs_result_item->score();
      size_t attr_num = fbs_result_item->attributes()->size();
      item.names.resize(attr_num);
      item.values.resize(attr_num);

      for (size_t k = 0; k < attr_num; ++k) {
        auto attr = fbs_result_item->attributes()->Get(k);
        item.names[k] = attr->name()->str();

        std::string item_value =
            std::string(reinterpret_cast<const char *>(attr->value()->Data()),
                        attr->value()->size());
        item.values[k] = std::move(item_value);
      }

      result.result_items[j] = std::move(item);
    }
    results_[i] = std::move(result);
  }

  online_log_message_ = response_->online_log_message()->str();
}

void Response::AddResults(const struct SearchResult &result) {
  results_.emplace_back(result);
}

void Response::AddResults(struct SearchResult &&result) {
  results_.emplace_back(std::forward<struct SearchResult>(result));
}

std::vector<struct SearchResult> &Response::Results() { return results_; }

void Response::SetOnlineLogMessage(const std::string &msg) {
  online_log_message_ = msg;
}

void Response::SetEngineInfo(void *table, void *vector_mgr,
                          GammaResult *gamma_results, int req_num) {
  gamma_results_ = gamma_results;
  req_num_ = req_num;
  table_ = table;
  vector_mgr_ = vector_mgr;
}

int Response::PackResultItem(const VectorDoc *vec_doc, 
                             std::vector<std::string> &fields_name,
                             struct ResultItem &result_item) {
  result_item.score = vec_doc->score;

  Doc doc;
  int docid = vec_doc->docid;
  VectorManager *vector_mgr = static_cast<VectorManager *>(vector_mgr_);
  Table *table = static_cast<Table *>(table_);

  // add vector into result
  size_t fields_size = fields_name.size();
  if (fields_size != 0) {
    std::vector<std::pair<string, int>> vec_fields_ids;
    std::vector<string> table_fields;

    for (size_t i = 0; i < fields_size; ++i) {
      std::string &name =  fields_name[i];
      if (!vector_mgr->Contains(name)) {
        table_fields.push_back(name);
      } else {
        vec_fields_ids.emplace_back(std::make_pair(name, docid));
      }
    }

    std::vector<string> vec;
    int ret = vector_mgr->GetVector(vec_fields_ids, vec, true);

    table->GetDocInfo(docid, doc, table_fields);

    if (ret == 0 && vec.size() == vec_fields_ids.size()) {
      for (size_t i = 0; i < vec_fields_ids.size(); ++i) {
        const string &field_name = vec_fields_ids[i].first;
        result_item.names.emplace_back(std::move(field_name));
        result_item.values.emplace_back(vec[i]);
      }
    } else {
      // get vector error
      // TODO : release extra field
      ;
    }
  } else {
    std::vector<string> table_fields;
    table->GetDocInfo(docid, doc, table_fields);
  }

  std::vector<struct Field> &fields = doc.TableFields();

  for (struct Field &field : fields) {
    result_item.names.emplace_back(std::move(field.name));
    result_item.values.emplace_back(std::move(field.value));
  }

  cJSON *extra_json = cJSON_CreateObject();
  cJSON *vec_result_json = cJSON_CreateArray();
  cJSON_AddItemToObject(extra_json, EXTRA_VECTOR_RESULT.c_str(),
                        vec_result_json);
  for (int i = 0; i < vec_doc->fields_len; ++i) {
    VectorDocField *vec_field = vec_doc->fields + i;
    cJSON *vec_field_json = cJSON_CreateObject();

    cJSON_AddStringToObject(vec_field_json, EXTRA_VECTOR_FIELD_NAME.c_str(),
                            vec_field->name.c_str());
    string source = string(vec_field->source, vec_field->source_len);
    cJSON_AddStringToObject(vec_field_json, EXTRA_VECTOR_FIELD_SOURCE.c_str(),
                            source.c_str());
    cJSON_AddNumberToObject(vec_field_json, EXTRA_VECTOR_FIELD_SCORE.c_str(),
                            vec_field->score);
    cJSON_AddItemToArray(vec_result_json, vec_field_json);
  }

  char *extra_data = cJSON_PrintUnformatted(extra_json);
  result_item.extra = std::string(extra_data, std::strlen(extra_data));
  free(extra_data);
  cJSON_Delete(extra_json);

  return 0;
}

int Response::PackResults(std::vector<std::string> &fields_name) {
  for (int i = 0; i < req_num_; ++i) {
    struct SearchResult result;
    result.total = gamma_results_[i].total;
    result.result_items.resize(gamma_results_[i].results_count);
    for (int j = 0; j < gamma_results_[i].results_count; ++j) {
      VectorDoc *vec_doc = gamma_results_[i].docs[j];
      struct ResultItem result_item;
      PackResultItem(vec_doc, fields_name, result_item);
      result.result_items[j] = std::move(result_item);
    }
    result.msg = "Success";
    result.result_code = SearchResultCode::SUCCESS;
    this->AddResults(std::move(result));
  }
  delete[] gamma_results_;
  gamma_results_ = nullptr;
#ifdef PERFORMANCE_TESTING
  PerfTool *perf_tool = static_cast<PerfTool *>(perf_tool_);
  perf_tool->Perf("pack result total");
  LOG(INFO) << perf_tool->OutputPerf().str();
#endif
  return 0;
}

}  // namespace tig_gamma
