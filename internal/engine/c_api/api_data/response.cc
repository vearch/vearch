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

namespace {

// Result of reading every scalar field of a batch of result docs in one
// MultiGet. A fixed-type field is stored in the doc's packed row
// (ToRowKey(docid)); each string field is its own key (field+":"+ToRowKey).
struct ScalarBatchResult {
  // False when no scalar field was requested (only vector fields, or none),
  // in which case the batched read is skipped and this struct is unused.
  bool valid = false;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses;
  // Every doc queues the same keys in the same order: an optional packed-row
  // key (present iff has_fixed_field) followed by one key per string field.
  // A doc's key window is therefore a pure function of its index, so no
  // per-doc offset table is stored -- FillScalarFromBatch derives the offsets.
  bool has_fixed_field = false;
  int num_string_fields = 0;
};

int SerializeToOut(const vearchpb::SearchResponse &pbResponse, char **out,
                   int *out_len) {
  std::string serialized;
  if (!pbResponse.SerializeToString(&serialized)) {
    LOG(ERROR) << "failed to serialize results";
    return -1;
  }
  *out_len = serialized.size();
  *out = (char *)malloc(*out_len * sizeof(char));
  if (*out == nullptr) {
    LOG(ERROR) << "failed to allocate memory for response";
    return -1;
  }
  memcpy(*out, serialized.data(), *out_len * sizeof(char));
  return 0;
}

// Classify requested scalar fields and read every rocksdb key they need
// (packed row + one key per string field) in a single MultiGet.
ScalarBatchResult ReadScalarBatch(Table *table,
                                  const std::map<std::string, int> &attr_idx,
                                  GammaResult *gamma_results, int req_num) {
  ScalarBatchResult batch;
  std::vector<std::pair<std::string, int>> string_fields;  // name, field_id
  string_fields.reserve(attr_idx.size());
  const auto &attrs = table->Attrs();
  for (auto &it : attr_idx) {
    DataType dt = attrs[it.second];
    if (dt == DataType::STRING || dt == DataType::STRINGARRAY) {
      string_fields.emplace_back(it.first, it.second);
    } else {
      batch.has_fixed_field = true;
    }
  }
  if (!batch.has_fixed_field && string_fields.empty()) {
    return batch;
  }

  batch.valid = true;
  batch.num_string_fields = string_fields.size();
  int total = 0;
  for (int i = 0; i < req_num; ++i) {
    total += gamma_results[i].results_count;
  }
  batch.keys.reserve(total *
                     (string_fields.size() + (batch.has_fixed_field ? 1 : 0)));
  for (int i = 0; i < req_num; ++i) {
    for (int j = 0; j < gamma_results[i].results_count; ++j) {
      int docid = gamma_results[i].docs[j]->docid;
      if (batch.has_fixed_field) {
        batch.keys.emplace_back(utils::ToRowKey(docid));
      }
      for (auto &sf : string_fields) {
        batch.keys.emplace_back(utils::ToStringFieldKey(sf.first, docid));
      }
    }
  }
  batch.statuses = table->GetStorageMgr()->MultiGetByKeys(
      table->CfId(), batch.keys, batch.values);
  return batch;
}

// Fast path for the common "return only _id" query: when the id cache is on
// and _id is the sole requested field, serve it from the in-memory
// docid->key map and skip rocksdb entirely (no MultiGet, no packed-row
// decode). Falls back to a storage read only on a cache miss.
int FillKeyField(vearchpb::ResultItem *item, Table *table,
                 std::map<std::string, int> &attr_idx, int docid) {
  const auto &doc_id_map = table->DocidMap();
  if (auto it = doc_id_map.find(docid); it != doc_id_map.end()) {
    auto *field = item->add_fields();
    field->set_name(table->GetKeyFieldName());
    field->set_value(it->second);
    return 0;
  }
  std::vector<uint8_t> val;
  int field_id = attr_idx[table->GetKeyFieldName()];
  int ret = table->GetFieldRawValue(docid, field_id, val, val);
  if (ret) {
    return ret;
  }
  auto *field = item->add_fields();
  field->set_name(table->GetKeyFieldName());
  field->set_value(std::string(val.begin(), val.end()));
  return 0;
}

// Fill scalar fields from the batched MultiGet results. Returns 1 when any
// requested field cannot be read (fixed-type packed row missing, or a string
// field key missing), so the caller drops the whole doc instead of returning
// it with some fields silently blanked out.
int FillScalarFromBatch(vearchpb::ResultItem *item, Table *table,
                        const std::map<std::string, int> &attr_idx,
                        ScalarBatchResult &batch, size_t doc_idx) {
  // Each doc occupies a fixed-width window in keys/values/statuses: an
  // optional packed-row key, then one key per string field. Derive the
  // window from doc_idx rather than storing per-doc offsets.
  size_t stride = (batch.has_fixed_field ? 1 : 0) + batch.num_string_fields;
  size_t base = doc_idx * stride;
  const std::string *row_value = nullptr;
  if (batch.has_fixed_field && batch.statuses[base].ok()) {
    row_value = &batch.values[base];
  }
  size_t str_off = base + (batch.has_fixed_field ? 1 : 0);
  size_t str_idx = 0;
  for (auto &it : attr_idx) {
    int field_id = it.second;
    DataType dt = table->Attrs()[field_id];
    auto *field = item->add_fields();
    field->set_name(it.first);
    if (dt == DataType::STRING || dt == DataType::STRINGARRAY) {
      size_t idx = str_off + str_idx;
      ++str_idx;
      if (!batch.statuses[idx].ok()) {
        return 1;
      }
      field->set_value(std::move(batch.values[idx]));
    } else if (row_value) {
      size_t offset = table->FieldOffset(field_id);
      int len = Table::FTypeSize(dt);
      field->set_value(row_value->data() + offset, len);
    } else {
      return 1;
    }
  }
  return 0;
}

int FillVectorFields(vearchpb::ResultItem *item, VectorManager *vector_mgr,
                     std::vector<std::string> &vec_fields, int docid) {
  for (uint32_t k = 0; k < vec_fields.size(); ++k) {
    std::vector<uint8_t> vec;
    int ret = vector_mgr->GetDocVector(docid, vec_fields[k], vec);
    if (ret) {
      return ret;
    }
    auto *field = item->add_fields();
    field->set_name(vec_fields[k]);
    field->set_value(std::string(vec.begin(), vec.end()));
  }
  return 0;
}

}  // namespace

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

  // empty result
  if (table == nullptr || vector_mgr == nullptr) {
    return SerializeToOut(pbResponse, out, out_len);
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

  // Fast path: when only _id is requested and the id cache is on, every doc's
  // _id comes from the in-memory docid->key map, so the whole response can
  // skip the rocksdb MultiGet below. This is the hot case for search queries
  // that return just ids, so it is worth special-casing.
  bool id_cache_fast_path = table->GetEnableIdCache() &&
                            fields_name.size() == 1 &&
                            fields_name[0] == table->GetKeyFieldName();

  ScalarBatchResult batch;
  if (!id_cache_fast_path) {
    batch = ReadScalarBatch(table, attr_idx, gamma_results_, req_num_);
  }

  size_t doc_idx = 0;
  for (int i = 0; i < req_num_; ++i) {
    auto *pbRes = pbResponse.add_results();
    vearchpb::SearchStatus *search_status = new vearchpb::SearchStatus();
    // successful/failed are query-level counts (total hits for this request),
    // not per-item counts of result_items. result_items is topK-capped and may
    // shrink further when a doc's field read fails, so its size intentionally
    // differs from successful; the router merges these as partition-level sums.
    search_status->set_total(gamma_results_[i].total);
    search_status->set_msg("");
    search_status->set_successful(gamma_results_[i].total);
    search_status->set_failed(0);
    pbRes->set_allocated_status(search_status);

    double max_score = std::numeric_limits<double>::lowest();
    if (RequestContext::is_killed()) {
      status = Status::MemoryExceeded();
      return -1;
    }
    for (int j = 0; j < gamma_results_[i].results_count; ++j) {
      VectorDoc *vec_doc = gamma_results_[i].docs[j];
      int docid = vec_doc->docid;
      double score = vec_doc->score;

      auto *item = pbRes->add_result_items();
      item->set_score(score);

      // Each doc is judged independently: if any of its scalar or vector
      // fields fails to read, that doc is removed from the response; the rest
      // of the batch and the overall request still succeed.
      bool failed = false;
      if (id_cache_fast_path) {
        failed = FillKeyField(item, table, attr_idx, docid) != 0;
      } else {
        // batch.valid is false only when no scalar field was requested, in
        // which case attr_idx is empty and there is nothing to fill here.
        if (batch.valid) {
          failed = FillScalarFromBatch(item, table, attr_idx, batch, doc_idx);
          ++doc_idx;
        }
        if (!failed && !vec_fields.empty() &&
            FillVectorFields(item, vector_mgr, vec_fields, docid)) {
          failed = true;
        }
      }
      if (failed) {
        pbRes->mutable_result_items()->RemoveLast();
      } else {
        max_score = std::max(max_score, score);
      }
    }

    std::string str_msg = status.ToString();
    pbRes->set_msg(str_msg);
    pbRes->set_max_score(max_score);
    pbRes->set_timeout(false);
  }

  if (SerializeToOut(pbResponse, out, out_len)) {
    return -1;
  }
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
