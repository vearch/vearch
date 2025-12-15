/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "response.h"

#include <limits>

#include "doc.h"
#include "idl/pb-gen/data_model.pb.h"
#include "idl/pb-gen/router_grpc.pb.h"
#include "memory/memoryManager.h"
#include "table/table.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/vector_manager.h"

namespace vearch {

Response::Response() {
  response_ = nullptr;
  perf_tool_ = new PerfTool();
}

Response::Response(bool trace) {
  response_ = nullptr;
  if (trace) {
    perf_tool_ = new PerfTool();
  } else {
    perf_tool_ = nullptr;
  }
}

Response::~Response() {
  if (perf_tool_) {
    delete perf_tool_;
    perf_tool_ = nullptr;
  }
  delete[] gamma_results_;
  gamma_results_ = nullptr;
}

int Response::Serialize(const std::string &space_name,
                        std::vector<std::string> &fields_name, Status &status,
                        char **out, int *out_len) {
  std::vector<std::string> vec_fields;
  std::map<std::string, int> attr_idx;
  Table *table = static_cast<Table *>(table_);
  VectorManager *vector_mgr = static_cast<VectorManager *>(vector_mgr_);
  vearchpb::SearchResponse pbResponse;
  pbResponse.set_timeout(false);

  std::string serialized;
  if (!pbResponse.SerializeToString(&serialized)) {
    LOG(ERROR) << "failed to serialize " << serialized.size();
    return -1;
  }
  
  *out_len = serialized.size();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)serialized.data(), *out_len);

  // empty result
  if (table == nullptr || vector_mgr == nullptr) {
    return 0;
  }
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

  const auto &doc_id_map = table->DocidMap();
  for (int i = 0; i < req_num_; ++i) {
    auto *pbRes = pbResponse.add_results();
    vearchpb::SearchStatus *search_status = new vearchpb::SearchStatus();
    search_status->set_total(gamma_results_[i].total);
    search_status->set_msg("");
    search_status->set_successful(gamma_results_[i].total);
    search_status->set_failed(0);
    pbRes->set_allocated_status(search_status);

    double max_score = std::numeric_limits<double>::lowest();
    for (int j = 0; j < gamma_results_[i].results_count; ++j) {
      if (RequestContext::is_killed()) {
        status = Status::MemoryExceeded();
        return -1;
      }
      VectorDoc *vec_doc = gamma_results_[i].docs[j];
      int docid = vec_doc->docid;
      double score = vec_doc->score;

      auto *item = pbRes->add_result_items();
      item->set_score(score);
      max_score = std::max(max_score, score);

      int ret = 0;
      if (table->GetEnableIdCache() && fields_name.size() == 1 && fields_name[0] == table->GetKeyFieldName()) {
        if (auto it = doc_id_map.find(docid); it != doc_id_map.end()) {
          auto *field = item->add_fields();
          field->set_name(table->GetKeyFieldName());
          field->set_value(it->second);
        } else {
          std::vector<uint8_t> val;
          int feild_id = attr_idx[table->GetKeyFieldName()];
          ret = table->GetFieldRawValue(docid, feild_id, val, val);
          if (ret) {
            break;
          }
          auto *field = item->add_fields();
          field->set_name(table->GetKeyFieldName());
          field->set_value(std::string(val.begin(), val.end()));
        }
      } else {
        std::vector<uint8_t> raw_val;

        for (auto &it : attr_idx) {
          std::vector<uint8_t> val;
          ret = table->GetFieldRawValue(docid, it.second, val, raw_val);
          if (ret) {
            break;
          }
          auto *field = item->add_fields();
          field->set_name(it.first);
          field->set_value(std::string(val.begin(), val.end()));
        }
        for (uint32_t k = 0; k < vec_fields.size(); ++k) {
          std::vector<uint8_t> vec;
          ret = vector_mgr->GetDocVector(docid, vec_fields[k], vec);
          if (ret) {
            break;
          }
          auto *field = item->add_fields();
          field->set_name(vec_fields[k]);
          field->set_value(std::string(vec.begin(), vec.end()));
        }
        if (ret) {
          item->set_score(std::numeric_limits<double>::lowest());
        }
      }
    }

    std::string str_msg = status.ToString();
    pbRes->set_msg(str_msg);
    pbRes->set_max_score(max_score);
    pbRes->set_timeout(false);
  }

  if (!pbResponse.SerializeToString(&serialized)) {
    LOG(ERROR) << "failed to serialize " << serialized;
    return -1;
  }
  *out_len = serialized.size();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)serialized.data(), *out_len);
  delete[] gamma_results_;
  gamma_results_ = nullptr;
  if (perf_tool_) {
    PerfTool *perf_tool = static_cast<PerfTool *>(perf_tool_);
    perf_tool->Perf("serialize");
    if (perf_tool->Cost() > perf_tool->slow_search_time) {
      LOG(WARNING) << space_name << " " << request_id_ << " "
                   << perf_tool->OutputPerf().str();
    } else {
      LOG(TRACE) << space_name << " " << request_id_ << " "
                 << perf_tool->OutputPerf().str();
    }
  }
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
      ResultItem item;
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
}

void Response::AddResults(const SearchResult &result) {
  results_.emplace_back(result);
}

void Response::AddResults(SearchResult &&result) {
  results_.emplace_back(std::forward<SearchResult>(result));
}

std::vector<SearchResult> &Response::Results() { return results_; }

void Response::SetEngineInfo(void *table, void *vector_mgr,
                             GammaResult *gamma_results, int req_num) {
  gamma_results_ = gamma_results;
  req_num_ = req_num;
  table_ = table;
  vector_mgr_ = vector_mgr;
}

int Response::PackResultItem(const VectorDoc *vec_doc,
                             std::vector<std::string> &fields_name,
                             ResultItem &result_item) {
  result_item.score = vec_doc->score;

  Doc doc;
  int docid = vec_doc->docid;
  VectorManager *vector_mgr = static_cast<VectorManager *>(vector_mgr_);
  Table *table = static_cast<Table *>(table_);

  // add vector into result
  size_t fields_size = fields_name.size();
  if (fields_size != 0) {
    std::vector<std::pair<std::string, int>> vec_fields_ids;
    std::vector<std::string> table_fields;

    for (size_t i = 0; i < fields_size; ++i) {
      std::string &name = fields_name[i];
      if (!vector_mgr->Contains(name)) {
        table_fields.push_back(name);
      } else {
        vec_fields_ids.emplace_back(std::make_pair(name, docid));
      }
    }

    std::vector<std::string> vec;
    int ret = vector_mgr->GetVector(vec_fields_ids, vec);

    table->GetDocInfo(docid, doc, table_fields);

    if (ret == 0 && vec.size() == vec_fields_ids.size()) {
      for (size_t i = 0; i < vec_fields_ids.size(); ++i) {
        const std::string &field_name = vec_fields_ids[i].first;
        result_item.names.emplace_back(std::move(field_name));
        result_item.values.emplace_back(vec[i]);
      }
    } else {
      // get vector error
      ;
    }
  } else {
    std::vector<std::string> table_fields;
    table->GetDocInfo(docid, doc, table_fields);
  }

  auto &fields = doc.TableFields();

  for (auto &[name, field] : fields) {
    result_item.names.emplace_back(std::move(field.name));
    result_item.values.emplace_back(std::move(field.value));
  }

  return 0;
}

int Response::PackResults(std::vector<std::string> &fields_name) {
  for (int i = 0; i < req_num_; ++i) {
    SearchResult result;
    result.total = gamma_results_[i].total;
    result.result_items.resize(gamma_results_[i].results_count);
    for (int j = 0; j < gamma_results_[i].results_count; ++j) {
      VectorDoc *vec_doc = gamma_results_[i].docs[j];
      ResultItem result_item;
      PackResultItem(vec_doc, fields_name, result_item);
      result.result_items[j] = std::move(result_item);
    }
    result.msg = "Success";
    result.result_code = SearchResultCode::SUCCESS;
    this->AddResults(std::move(result));
  }
  delete[] gamma_results_;
  gamma_results_ = nullptr;
  if (perf_tool_) {
    PerfTool *perf_tool = static_cast<PerfTool *>(perf_tool_);
    perf_tool->Perf("pack results");
    LOG(TRACE) << perf_tool->OutputPerf().str();
  }
  return 0;
}

}  // namespace vearch
