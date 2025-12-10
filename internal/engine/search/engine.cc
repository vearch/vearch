/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "engine.h"

#include <fcntl.h>
#include <locale.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <thread>
#include <vector>

#include "cjson/cJSON.h"
#include "common/gamma_common_data.h"
#ifdef BUILD_WITH_GPU
#include "index/impl/gpu/gamma_index_ivfflat_gpu.h"
#include "index/impl/gpu/gamma_index_ivfpq_gpu.h"
#endif
#include "omp.h"
#include "table/table_io.h"
#include "third_party/nlohmann/json.hpp"
#include "util/bitmap.h"
#include "util/log.h"
#include "util/status.h"
#include "util/utils.h"

using json = nlohmann::json;

namespace vearch {

class PerfTool;

bool RequestConcurrentController::Acquire(int req_num) {
#ifndef __APPLE__
  int num = __sync_fetch_and_add(&cur_concurrent_num_, req_num);

  if (num < concurrent_threshold_) {
    return true;
  } else {
    LOG(WARNING) << "cur_threads_num [" << num << "] concurrent_threshold ["
                 << concurrent_threshold_ << "]";
    return false;
  }
#else
  return true;
#endif
}

void RequestConcurrentController::Release(int req_num) {
#ifndef __APPLE__
  __sync_fetch_and_sub(&cur_concurrent_num_, req_num);
#else
  return;
#endif
}

RequestConcurrentController::RequestConcurrentController() {
  concurrent_threshold_ = 0;
  max_threads_ = 0;
  cur_concurrent_num_ = 0;
  GetMaxThread();
}

int RequestConcurrentController::GetMaxThread() {
#ifndef __APPLE__
  // Get system config and calculate max threads
  int omp_max_threads = omp_get_max_threads();
  int threads_max = GetSystemInfo("cat /proc/sys/kernel/threads-max");
  int max_map_count = GetSystemInfo("cat /proc/sys/vm/max_map_count");
  int pid_max = GetSystemInfo("cat /proc/sys/kernel/pid_max");
  LOG(INFO) << "System info: threads_max [" << threads_max
            << "] max_map_count [" << max_map_count << "] pid_max [" << pid_max
            << "]";
  max_threads_ = std::min(threads_max, pid_max);
  max_threads_ = std::min(max_threads_, max_map_count / 2);
  // calculate concurrent threshold
  concurrent_threshold_ = (max_threads_ * 0.5) / (omp_max_threads + 1);
  LOG(INFO) << "max_threads [" << max_threads_ << "] concurrent_threshold ["
            << concurrent_threshold_ << "]";
  if (concurrent_threshold_ == 0) {
    LOG(FATAL) << "concurrent_threshold cannot be 0!";
  }
  return max_threads_;
#else
  return 0;
#endif
}

int RequestConcurrentController::GetSystemInfo(const char *cmd) {
  int num = 0;

  char buff[1024];
  memset(buff, 0, sizeof(buff));

  FILE *fstream = popen(cmd, "r");
  if (fstream == nullptr) {
    LOG(ERROR) << "execute command failed: " << strerror(errno);
    num = -1;
  } else {
    fgets(buff, sizeof(buff), fstream);
    num = atoi(buff);
    pclose(fstream);
  }
  return num;
}

Engine::Engine(const std::string &index_root_path,
               const std::string &space_name)
    : index_root_path_(index_root_path),
      space_name_(space_name),
      date_time_format_("%Y-%m-%d-%H:%M:%S"),
      backup_status_(0),
      refresh_interval_(1000) {
  table_ = nullptr;
  vec_manager_ = nullptr;
  index_status_ = IndexStatus::UNINDEXED;
  delete_num_ = 0;
  indexing_state_.store(IndexingState::IDLE);
  is_dirty_ = false;
  field_range_index_ = nullptr;
  created_table_ = false;
  docids_bitmap_ = nullptr;
  storage_mgr_ = nullptr;
#ifdef PERFORMANCE_TESTING
  search_num_ = 0;
#endif
  long_search_time_ = 1000;
}

Engine::~Engine() {
  // Safely stop indexing if it's running
  IndexingState expected = IndexingState::RUNNING;
  if (indexing_state_.compare_exchange_strong(expected,
                                              IndexingState::STOPPING)) {
    LOG(INFO) << space_name_ << " stopping indexing process in destructor...";
    if (!WaitForIndexingComplete(-1)) {  // Wait up to -1 ms (indefinitely)
      LOG(WARNING) << space_name_
                   << " timeout waiting for indexing to stop in destructor";
    }
  }

  if (indexing_thread_.joinable()) {
    indexing_thread_.join();
  }

  if (add_field_index_thread_.joinable()) {
    add_field_index_thread_.join();
  }

  if (remove_field_index_thread_.joinable()) {
    remove_field_index_thread_.join();
  }

  Close();
}

void Engine::Close() {
  delete vec_manager_;
  vec_manager_ = nullptr;

  delete table_;
  table_ = nullptr;

  delete field_range_index_;
  field_range_index_ = nullptr;

  delete docids_bitmap_;
  docids_bitmap_ = nullptr;

  delete storage_mgr_;
  storage_mgr_ = nullptr;
  LOG(INFO) << space_name_ << " engine closed";
}

Engine *Engine::GetInstance(const std::string &index_root_path,
                            const std::string &space_name) {
  Engine *engine = new Engine(index_root_path, space_name);
  Status status = engine->Setup();
  if (!status.ok()) {
    LOG(ERROR) << "Build " << space_name << " [" << index_root_path
               << "] failed!";
    delete engine;
    engine = nullptr;
    return nullptr;
  }
  return engine;
}

Status Engine::Setup() {
  if (!utils::isFolderExist(index_root_path_.c_str())) {
    if (mkdir(index_root_path_.c_str(),
              S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)) {
      std::string msg = "mkdir " + index_root_path_ + " error";
      LOG(ERROR) << msg;
      return Status::IOError(msg);
    }
  }

  dump_path_ = index_root_path_ + "/retrieval_model_index";
  if (!utils::isFolderExist(dump_path_.c_str())) {
    if (mkdir(dump_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)) {
      std::string msg = "mkdir " + dump_path_ + " error";
      LOG(ERROR) << msg;
      return Status::IOError(msg);
    }
  }

  docids_bitmap_ = new bitmap::RocksdbBitmapManager();
  int init_bitmap_size = 5000 * 10000;
  if (docids_bitmap_->Init(init_bitmap_size, index_root_path_ + "/bitmap") !=
      0) {
    std::string msg = "Cannot create bitmap!";
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }
  if (docids_bitmap_->IsLoad()) {
    docids_bitmap_->Load();
  } else {
    docids_bitmap_->Dump();
  }

  max_docid_ = 0;
  LOG(INFO) << space_name_ << " setup successed! bitmap_bytes_size="
            << docids_bitmap_->BytesSize();
  return Status::OK();
}

Status Engine::Search(Request &request, Response &response_results) {
  int req_num = request.ReqNum();
  Status status;

  if (req_num <= 0) {
    std::string msg = space_name_ + " req_num should not less than 0";
    status = Status::InvalidArgument(msg);
    LOG(ERROR) << msg;
    return status;
  }

  bool req_permit = RequestConcurrentController::GetInstance().Acquire(req_num);
  if (not req_permit) {
    std::string msg = "Resource temporarily unavailable";
    LOG(WARNING) << msg;
    RequestConcurrentController::GetInstance().Release(req_num);
    status = Status::ResourceExhausted();
    return status;
  }

  int topn = request.TopN();
  if (topn <= 0) {
    std::string msg = "limit[topN] is zero";
    // for (int i = 0; i < req_num; ++i) {
    //   SearchResult result;
    //   result.msg = msg;
    //   result.result_code = SearchResultCode::SEARCH_ERROR;
    //   response_results.AddResults(std::move(result));
    // }
    RequestConcurrentController::GetInstance().Release(req_num);
    return Status::InvalidArgument(msg);
  }
  int brute_force_search =
      request.BruteForceSearch();  // 0: normal search, 1: brute force search,
                                   // 2: auto
  std::vector<struct VectorQuery> &vec_fields = request.VecFields();
  size_t vec_fields_num = vec_fields.size();

  if (vec_fields_num > 0 && brute_force_search == 2 &&
      index_status_ != IndexStatus::INDEXED) {
    brute_force_search = 1;  // force to use brute force search
  }

  if (vec_fields_num > 0 && (brute_force_search == 0) &&
      (index_status_ != IndexStatus::INDEXED) &&
      (max_docid_ > brute_force_search_threshold)) {
    std::string msg =
        space_name_ + " index not trained, " +
        "brute_force_search is 0, max_docid_ = " + std::to_string(max_docid_) +
        ", threshold = " + std::to_string(brute_force_search_threshold);
    LOG(WARNING) << msg;
    // for (int i = 0; i < req_num; ++i) {
    //   SearchResult result;
    //   result.msg = msg;
    //   result.result_code = SearchResultCode::INDEX_NOT_TRAINED;
    //   response_results.AddResults(std::move(result));
    // }
    RequestConcurrentController::GetInstance().Release(req_num);
    status = Status::IndexNotTrained();
    return status;
  }

  auto *perf_tool = response_results.GetPerfTool();
  if (perf_tool) {
    perf_tool->long_search_time = long_search_time_;
  }
  GammaQuery query;
  query.vec_query = vec_fields;

  query.condition = new SearchCondition(response_results.GetPerfTool());
  query.condition->topn = topn;
  query.condition->multi_vector_rank =
      request.MultiVectorRank() == 1 ? true : false;
  query.condition->brute_force_search = brute_force_search;
  query.condition->l2_sqrt = request.L2Sqrt();
  query.condition->index_params = request.IndexParams();

  query.condition->range_filters = request.RangeFilters();
  query.condition->term_filters = request.TermFilters();
  query.condition->filter_operator = request.FilterOperator();
  query.condition->table = table_;
  query.condition->offset = request.Offset();
  if (request.Ranker()) {
    query.condition->ranker = dynamic_cast<WeightedRanker *>(request.Ranker());
    if (query.condition->ranker == nullptr) {
      std::string msg = "ranker error!";
      LOG(WARNING) << msg;
      // for (int i = 0; i < req_num; ++i) {
      //   SearchResult result;
      //   result.msg = msg;
      //   result.result_code = SearchResultCode::SEARCH_ERROR;
      //   response_results.AddResults(std::move(result));
      // }
      RequestConcurrentController::GetInstance().Release(req_num);
      return Status::InvalidArgument();
    }

    status = query.condition->ranker->Parse();
    if (!status.ok()) {
      std::string msg =
          " ranker parse err, ranker: " + query.condition->ranker->ToString();
      LOG(WARNING) << request.RequestId() << msg;
      RequestConcurrentController::GetInstance().Release(req_num);
      return status;
    }
  }

  MultiRangeQueryResults range_query_result;
  size_t range_filters_num = request.RangeFilters().size();
  size_t term_filters_num = request.TermFilters().size();
  if (range_filters_num > 0 || term_filters_num > 0) {
    int64_t num = MultiRangeQuery(request, query.condition, response_results,
                              &range_query_result);
    if (query.condition->GetPerfTool()) {
      query.condition->GetPerfTool()->Perf("filter result num " +
                                           std::to_string(num));
    }
    if (num == int64_t(ResultStatus::ZERO) || num == int64_t(ResultStatus::INTERNAL_ERR)) {
      RequestConcurrentController::GetInstance().Release(req_num);
      return status;
    } else if (num == int64_t(ResultStatus::KILLED)) {
      RequestConcurrentController::GetInstance().Release(req_num);
      status = Status::MemoryExceeded();
      return status;
    }
  }

  if (vec_fields_num > 0) {
    GammaResult *gamma_results = new GammaResult[req_num];

    int doc_num = GetDocsNum();

    for (int i = 0; i < req_num; ++i) {
      gamma_results[i].total = doc_num;
    }

    status = vec_manager_->Search(query, gamma_results);
    if (!status.ok()) {
      std::string msg =
          space_name_ + " search error [" + status.ToString() + "]";
      // for (int i = 0; i < req_num; ++i) {
      //   SearchResult result;
      //   result.msg = msg;
      //   result.result_code = SearchResultCode::SEARCH_ERROR;
      //   response_results.AddResults(std::move(result));
      // }
      RequestConcurrentController::GetInstance().Release(req_num);
      delete[] gamma_results;
      return status;
    }

    if (query.condition->GetPerfTool()) {
      query.condition->GetPerfTool()->Perf("search total");
    }
    response_results.SetEngineInfo(table_, vec_manager_, gamma_results,
                                   req_num);
  }

  RequestConcurrentController::GetInstance().Release(req_num);
  return status;
}

Status Engine::Query(QueryRequest &request, Response &response_results) {
  Status status;

  if (request.DocumentIds().size() > 0) {
    std::vector<std::string> document_ids = request.DocumentIds();
    GammaResult *gamma_result = new GammaResult[1];
    gamma_result->init(document_ids.size(), nullptr, 0);

    for (size_t i = 0; i < document_ids.size(); i++) {
      if (RequestContext::is_killed()) {
        status = Status::MemoryExceeded();
        break;
      }
      int64_t docid = -1;
      int ret = 0;
      if (request.PartitionId() > 0) {
        char *endptr;
        docid = strtol(document_ids[i].c_str(), &endptr, 10);
        if (*endptr != '\0' || docid < 0 || docid >= max_docid_) {
          LOG(ERROR) << "document_id " << document_ids[i]
                     << " is error, max_docid_ = " << max_docid_;
          continue;
        }
      } else {
        ret = table_->GetDocidByKey(document_ids[i], docid);
        if (ret != 0 || docid < 0) {
          continue;
        }
      }

      if (!docids_bitmap_->Test(docid)) {
        gamma_result->total++;
        gamma_result->docs[(gamma_result->results_count)++]->docid = docid;
      }
    }
    response_results.SetEngineInfo(table_, vec_manager_, gamma_result, 1);
    return status;
  }

  int topn = request.TopN();
  std::vector<uint64_t> docids;
  docids.reserve(topn);

  MultiRangeQueryResults range_query_result;
  size_t range_filters_num = request.RangeFilters().size();
  size_t term_filters_num = request.TermFilters().size();
  if (range_filters_num > 0 || term_filters_num > 0) {
    std::vector<FilterInfo> filters;
    std::vector<struct RangeFilter> &range_filters = request.RangeFilters();
    std::vector<struct TermFilter> &term_filters = request.TermFilters();

    int range_filters_size = range_filters.size();
    int term_filters_size = term_filters.size();

    filters.resize(range_filters_size + term_filters_size);
    int idx = 0;

    for (int i = 0; i < range_filters_size; ++i) {
      struct RangeFilter &filter = range_filters[i];

      filters[idx].field = table_->GetAttrIdx(filter.field);
      filters[idx].lower_value = filter.lower_value;
      filters[idx].upper_value = filter.upper_value;
      filters[idx].include_lower = filter.include_lower;
      filters[idx].include_upper = filter.include_upper;
      filters[idx].is_union = static_cast<FilterOperator>(filter.is_union);

      ++idx;
    }

    for (int i = 0; i < term_filters_size; ++i) {
      struct TermFilter &filter = term_filters[i];

      filters[idx].field = table_->GetAttrIdx(filter.field);
      filters[idx].lower_value = filter.value;
      filters[idx].is_union = static_cast<FilterOperator>(filter.is_union);

      ++idx;
    }

    int num = field_range_index_->Query(
        static_cast<FilterOperator>(request.FilterOperator()), filters, docids,
        (size_t)topn, (size_t)request.Offset());

    if (num == int64_t(ResultStatus::ZERO) || num == int64_t(ResultStatus::INTERNAL_ERR)) {
      std::string msg =
          space_name_ + " no result: numeric filter return 0 result";
      SearchResult result;
      result.msg = msg;
      result.result_code = SearchResultCode::SUCCESS;
      response_results.AddResults(std::move(result));
      return status;
    } else if (num == int64_t(ResultStatus::KILLED)) {
      status = Status::MemoryExceeded();
      return status;
    }
  }

  GammaResult *gamma_result = new GammaResult[1];
  gamma_result->init(topn, nullptr, 0);

  for (int64_t docid : docids) {
    if (RequestContext::is_killed()) {
      status = Status::MemoryExceeded();
      break;
    }

    if (docids_bitmap_->Test(docid)) {
      continue;
    }
    gamma_result->total++;
    if (gamma_result->results_count < topn) {
      gamma_result->docs[(gamma_result->results_count)++]->docid = docid;
    } else {
      break;
    }
  }
  response_results.SetEngineInfo(table_, vec_manager_, gamma_result, 1);
  return status;
}

int64_t Engine::MultiRangeQuery(Request &request, SearchCondition *condition,
                            Response &response_results,
                            MultiRangeQueryResults *range_query_result) {
  std::vector<FilterInfo> filters;
  std::vector<struct RangeFilter> &range_filters = request.RangeFilters();
  std::vector<struct TermFilter> &term_filters = request.TermFilters();

  int range_filters_size = range_filters.size();
  int term_filters_size = term_filters.size();

  filters.resize(range_filters_size + term_filters_size);
  int idx = 0;

  for (int i = 0; i < range_filters_size; ++i) {
    struct RangeFilter &filter = range_filters[i];

    filters[idx].field = table_->GetAttrIdx(filter.field);
    filters[idx].lower_value = filter.lower_value;
    filters[idx].upper_value = filter.upper_value;
    filters[idx].include_lower = filter.include_lower;
    filters[idx].include_upper = filter.include_upper;
    filters[idx].is_union = static_cast<FilterOperator>(filter.is_union);

    ++idx;
  }

  for (int i = 0; i < term_filters_size; ++i) {
    struct TermFilter &filter = term_filters[i];

    filters[idx].field = table_->GetAttrIdx(filter.field);
    filters[idx].lower_value = filter.value;
    filters[idx].is_union = static_cast<FilterOperator>(filter.is_union);

    ++idx;
  }

  int64_t num = field_range_index_->Search(
      static_cast<FilterOperator>(request.FilterOperator()), filters,
      range_query_result);

  if (num == 0) {
    std::string msg =
        space_name_ + " no result: numeric filter return 0 result";
    LOG(TRACE) << request.RequestId() << " " << msg;
    for (int i = 0; i < request.ReqNum(); ++i) {
      SearchResult result;
      result.msg = msg;
      result.result_code = SearchResultCode::SUCCESS;
      response_results.AddResults(std::move(result));
    }
  } else if (num < 0) {
    condition->range_query_result = nullptr;
  } else {
    condition->range_query_result = range_query_result;
  }
  return num;
}

Status Engine::CreateTable(TableInfo &table) {
  Status status;

  storage_mgr_ = new StorageManager(index_root_path_ + "/data");
  size_t cache_size = 512 * 1024 * 1024;  // unit : byte

  std::vector<int> vector_cf_ids;

  vec_manager_ = new VectorManager(VectorStorageType::RocksDB, docids_bitmap_,
                                   index_root_path_, space_name_);
  {
    std::vector<struct VectorInfo> &vectors_infos = table.VectorInfos();

    for (struct VectorInfo &vectors_info : vectors_infos) {
      std::string &name = vectors_info.name;
      vector_cf_ids.push_back(
          storage_mgr_->CreateColumnFamily("vector_" + name));
    }
  }
  status = vec_manager_->CreateVectorTable(table, vector_cf_ids, storage_mgr_);
  if (!status.ok()) {
    std::string msg =
        space_name_ + " cannot create VectorTable: " + status.ToString();
    LOG(ERROR) << msg;
    this->Close();
    return Status::ParamError(msg);
  }

  int table_cf_id = storage_mgr_->CreateColumnFamily("table");

  table_ = new Table(space_name_, storage_mgr_, table_cf_id);
  status = table_->CreateTable(table);
  if (!status.ok()) {
    std::string msg =
        space_name_ + " cannot create table, err: " + status.ToString();
    LOG(ERROR) << msg;
    this->Close();
    return Status::ParamError(msg);
  }

  training_threshold_ = table.TrainingThreshold();
  LOG(INFO) << space_name_
            << " init training_threshold=" << training_threshold_;

  field_range_index_ = new MultiFieldsRangeIndex(table_, storage_mgr_);
  if ((nullptr == field_range_index_) || (AddNumIndexFields() < 0)) {
    std::string msg = "add numeric index fields error!";
    LOG(ERROR) << msg;
    this->Close();
    return Status::ParamError(msg);
  }

  status = storage_mgr_->Init(cache_size);
  if (!status.ok()) {
    LOG(ERROR) << "init error, ret=" << status.ToString();
    this->Close();
    return status;
  }

  std::string table_name = table.Name();
  std::string path = index_root_path_ + "/" + table_name + ".schema";
  TableSchemaIO tio(path);  // rewrite it if the path is already existed
  if (tio.Write(table)) {
    LOG(ERROR) << "write table schema error, path=" << path;
  }

  refresh_interval_ = table.RefreshInterval();
  LOG(INFO) << space_name_ << " init refresh_interval=" << refresh_interval_;

  LOG(INFO) << "create table [" << table_name << "] success!";
  created_table_ = true;
  return Status::OK();
}

int Engine::AddOrUpdate(Doc &doc) {
#ifdef PERFORMANCE_TESTING
  double start = utils::getmillisecs();
#endif
  auto &fields_table = doc.TableFields();
  auto &fields_vec = doc.VectorFields();

  std::string &key = doc.Key();

  // add fields into table
  int64_t docid = -1;
  table_->GetDocidByKey(key, docid);
  if (docid != -1 && docid < max_docid_) {
    if (Update(docid, fields_table, fields_vec)) {
      LOG(ERROR) << space_name_ << " update error, key=" << key
                 << ", docid=" << docid;
      return -1;
    }
    is_dirty_ = true;
    return 0;
  } else if (docid >= max_docid_) {
    LOG(ERROR) << space_name_ << " add error, key=" << key
               << ", max_docid_=" << max_docid_ << ", docid=" << docid;
    return -2;
  } else if (docid == -1) {
    Status status = CheckDoc(fields_table, fields_vec);
    if (!status.ok()) {
      LOG(ERROR) << space_name_ << " add error, key=" << key
                 << " err: " << status.ToString();
      return -3;
    }
  }

  int ret = table_->Add(key, fields_table, max_docid_);
  if (ret != 0) return -4;
  for (auto &[name, field] : fields_table) {
    int idx = table_->GetAttrIdx(field.name);
    field_range_index_->AddDoc(max_docid_, idx);
  }
#ifdef PERFORMANCE_TESTING
  double end_table = utils::getmillisecs();
#endif

  // add vectors by VectorManager
  ret = vec_manager_->AddToStore(max_docid_, fields_vec);
  if (ret != 0) {
    LOG(ERROR) << space_name_ << " add to store error max_docid [" << max_docid_
               << "] err=" << ret;
    return -5;
  }

  ++max_docid_;
  ret = table_->SetStorageManagerSize(max_docid_);
  if (ret != 0) {
    LOG(ERROR) << space_name_ << " table_ set max_docid [" << max_docid_
               << "] err= " << ret;
    return -6;
  };
  ret = docids_bitmap_->SetMaxID(max_docid_);
  if (ret != 0) {
    LOG(ERROR) << space_name_ << " Bitmap set max_docid [" << max_docid_
               << "] err= " << ret;
    return -7;
  };

  if (refresh_interval_ >= 0 and
      indexing_state_.load() == IndexingState::IDLE and
      index_status_ == UNINDEXED) {
    if (max_docid_ - delete_num_ >= training_threshold_) {
      LOG(INFO) << space_name_ << " begin indexing. training_threshold="
                << training_threshold_;
      this->BuildIndex();
    }
  }
#ifdef PERFORMANCE_TESTING
  double end = utils::getmillisecs();
  if (max_docid_ % 10000 == 0) {
    LOG(DEBUG) << space_name_ << " table cost [" << end_table - start
               << "]ms, vec store cost [" << end - end_table
               << "]ms, max_docid_=" << max_docid_;
  }
#endif
  is_dirty_ = true;
  return 0;
}

Status Engine::CheckDoc(
    std::unordered_map<std::string, struct Field> &fields_table,
    std::unordered_map<std::string, struct Field> &fields_vec) {
  for (auto &[name, field] : fields_table) {
    auto it = table_->FieldMap().find(name);
    if (it == table_->FieldMap().end()) {
      std::string msg =
          "Check doc err: cannot find numerical field [" + name + "]";
      return Status::ParamError(msg);
    }
  }

  const auto &raw_vectors = vec_manager_->RawVectors();
  if (fields_vec.size() != raw_vectors.size()) {
    std::string msg = "Check doc err: vector fields length [" +
                      std::to_string(fields_vec.size()) +
                      "] not equal to raw_vectors length = " +
                      std::to_string(raw_vectors.size());
    return Status::ParamError(msg);
  }
  for (auto &[name, field] : fields_vec) {
    auto it = raw_vectors.find(name);
    if (it == raw_vectors.end()) {
      std::string msg = "Check doc err: cannot find raw vector [" + name + "]";
      return Status::ParamError(msg);
    }

    RawVector *raw_vector = it->second;
    size_t element_size =
        raw_vector->MetaInfo()->DataType() == VectorValueType::BINARY
            ? sizeof(char)
            : sizeof(float);
    if ((size_t)raw_vector->MetaInfo()->Dimension() !=
        field.value.size() / element_size) {
      std::string msg =
          "Check doc err: vector field " + name +
          " invalid field value len=" + std::to_string(field.value.size()) +
          ", dimension=" + std::to_string(raw_vector->MetaInfo()->Dimension());
      return Status::ParamError(msg);
    }
  }
  return Status::OK();
}

int Engine::Update(int doc_id,
                   std::unordered_map<std::string, struct Field> &fields_table,
                   std::unordered_map<std::string, struct Field> &fields_vec) {
  int ret = vec_manager_->Update(doc_id, fields_vec);
  if (ret != 0) {
    return ret;
  }

  auto is_equal = table_->CheckFieldIsEqual(fields_table, doc_id);
  for (auto &[name, field] : fields_table) {
    if (is_equal[name]) {
      continue;
    }

    int idx = table_->GetAttrIdx(field.name);
    field_range_index_->Delete(doc_id, idx);
  }

  if (table_->Update(fields_table, doc_id) != 0) {
    LOG(ERROR) << space_name_ << " table update error";
    return -1;
  }

  for (auto &[name, field] : fields_table) {
    if (is_equal[name]) {
      continue;
    }
    int idx = table_->GetAttrIdx(field.name);
    field_range_index_->AddDoc(doc_id, idx);
  }
  is_dirty_ = true;
  return 0;
}

int Engine::Delete(std::string &key) {
  int64_t docid = -1;
  int ret = 0;
  ret = table_->GetDocidByKey(key, docid);
  if (ret != 0 || docid < 0) return -1;

  if (docids_bitmap_->Test(docid)) {
    return ret;
  }
  ret = docids_bitmap_->Set(docid);
  if (ret) {
    LOG(ERROR) << space_name_ << " bitmap set failed: ret=" << ret;
    return ret;
  }
  ++delete_num_;
  ret = docids_bitmap_->Dump(docid, 1);
  if (ret) {
    LOG(ERROR) << space_name_ << " bitmap dump failed: ret=" << ret;
    return ret;
  }
  const auto &name_to_idx = table_->FieldMap();
  for (const auto &ite : name_to_idx) {
    field_range_index_->Delete(docid, ite.second);
  }
  table_->Delete(key);

  vec_manager_->Delete(docid);
  is_dirty_ = true;

  return ret;
}

int Engine::GetDoc(const std::string &key, Doc &doc) {
  int64_t docid = -1;
  int ret = table_->GetDocidByKey(key, docid);
  if (ret != 0 || docid < 0) {
    return -1;
  }

  return GetDoc(docid, doc);
}

int Engine::GetDoc(int docid, Doc &doc, bool next) {
  int ret = 0;

  if ((next ? docid < -1 : docid < 0) || docid >= max_docid_) {
    LOG(ERROR) << space_name_ << " docid [" << docid
               << "] error, max_docid_ = " << max_docid_;
    return -1;
  }

  if (next) {
    while (++docid < max_docid_) {
      if (!docids_bitmap_->Test(docid)) {
        break;
      }
    }
    if (docid >= max_docid_) {
      LOG(ERROR) << space_name_ << " docid [" << docid
                 << "] is greater then max_docid " << max_docid_;
      return -1;
    }
  } else if (docids_bitmap_->Test(docid)) {
    return -1;
  }
  std::vector<std::string> index_names;
  vec_manager_->VectorNames(index_names);

  std::vector<std::string> table_fields;
  ret = table_->GetDocInfo(docid, doc, table_fields);
  if (ret != 0) {
    return ret;
  }

  if (next) {
    struct Field field;
    field.name = "_docid";
    field.datatype = DataType::INT;
    const char *bytes = reinterpret_cast<const char *>(&docid);
    field.value = std::string(bytes, sizeof(docid));
    doc.AddField(std::move(field));
  }

  std::vector<std::pair<std::string, int>> vec_fields_ids;
  for (size_t i = 0; i < index_names.size(); ++i) {
    vec_fields_ids.emplace_back(std::make_pair(index_names[i], docid));
  }

  std::vector<std::string> vec;
  ret = vec_manager_->GetVector(vec_fields_ids, vec);
  if (ret == 0 && vec.size() == vec_fields_ids.size()) {
    for (size_t i = 0; i < index_names.size(); ++i) {
      struct Field field;
      field.name = index_names[i];
      field.datatype = DataType::VECTOR;
      field.value = vec[i];
      doc.AddField(field);
    }
  }
  return 0;
}

int Engine::BuildIndex() {
  // Try to transition from IDLE to STARTING atomically
  IndexingState expected = IndexingState::IDLE;
  if (!indexing_state_.compare_exchange_strong(expected,
                                               IndexingState::STARTING)) {
#ifdef BUILD_WITH_GPU
    auto &indexes = vec_manager_->VectorIndexes();
    for (auto &[name, index] : indexes) {
      if (dynamic_cast<gpu::GammaIVFPQGPUIndex *>(index)) {
        LOG(INFO) << space_name_ << " index [" << name
                  << "] is a GammaIVFPQGPUIndex";
        dynamic_cast<gpu::GammaIVFPQGPUIndex *>(index)->Indexing();
        return 0;
      } else if (dynamic_cast<gpu::GammaIVFFlatGPUIndex *>(index)) {
        LOG(INFO) << space_name_ << " index [" << name
                  << "] is a GammaIVFFlatGPUIndex";
        dynamic_cast<gpu::GammaIVFFlatGPUIndex *>(index)->Indexing();
        return 0;
      }
    }
#endif
    // Already in progress or stopping
    LOG(INFO)
        << space_name_
        << " index building already in progress or stopping, current state: "
        << static_cast<int>(expected);
    return 0;
  }

  LOG(INFO) << space_name_ << " starting index build process...";

  if (indexing_thread_.joinable()) {
    indexing_thread_.join();
  }

  auto func_indexing = std::bind(&Engine::Indexing, this);
  indexing_thread_ = std::thread(func_indexing);
  return 0;
}

// TODO set limit for cpu
int Engine::RebuildIndex(int drop_before_rebuild, int limit_cpu, int describe) {
  int ret = 0;
  if (indexing_state_.load() != IndexingState::RUNNING ||
      index_status_ == IndexStatus::UNINDEXED) {
    LOG(INFO) << space_name_ << " index not running, no need to rebuild!";
    return ret;
  }

  // Stop current indexing process safely using compare_exchange
  IndexingState expected = IndexingState::RUNNING;
  if (indexing_state_.compare_exchange_strong(expected,
                                              IndexingState::STOPPING)) {
    LOG(INFO) << space_name_
              << " stopping current indexing process for rebuild...";
    if (WaitForIndexingComplete()) {
      LOG(INFO) << space_name_
                << " indexing process stopped, proceeding with rebuild";
    } else {
      LOG(WARNING)
          << space_name_
          << " timeout waiting for indexing to stop, proceeding anyway";
    }
  } else if (expected == IndexingState::STARTING) {
    // Wait for starting to complete
    LOG(INFO) << space_name_
              << " waiting for indexing to start before stopping...";
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    expected = IndexingState::RUNNING;
    if (indexing_state_.compare_exchange_strong(expected,
                                                IndexingState::STOPPING)) {
      WaitForIndexingComplete();
    }
  }

  if (indexing_thread_.joinable()) {
    indexing_thread_.join();
  }

  if (describe) {
    vec_manager_->DescribeVectorIndexes();
    return ret;
  }

  if (!drop_before_rebuild) {
    std::map<std::string, IndexModel *> vector_indexes;
    Status status =
        vec_manager_->CreateVectorIndexes(training_threshold_, vector_indexes);
    if (!status.ok()) {
      LOG(ERROR) << space_name_ << " RebuildIndex CreateVectorIndexes failed: "
                 << status.ToString();
      vec_manager_->DestroyVectorIndexes();
      return ret;
    }

    if (indexing_state_.load() == IndexingState::IDLE &&
        max_docid_ - delete_num_ > training_threshold_) {
      ret = vec_manager_->TrainIndex(vector_indexes);
      if (ret) {
        LOG(ERROR) << space_name_
                   << " RebuildIndex TrainIndex failed ,ret=" << ret;
        index_status_ = IndexStatus::UNINDEXED;
        return -1;
      }
    }

    vec_manager_->ResetVectorIndexes(vector_indexes);
  } else {
    Status status = vec_manager_->ReCreateVectorIndexes(training_threshold_);
    if (!status.ok()) {
      LOG(ERROR) << space_name_
                 << " RebuildIndex ReCreateVectorIndexes failed: "
                 << status.ToString();
      vec_manager_->DestroyVectorIndexes();
      return ret;
    }
    index_status_ = IndexStatus::UNINDEXED;
  }

  if (refresh_interval_ >= 0 and
      indexing_state_.load() == IndexingState::IDLE &&
      max_docid_ - delete_num_ >= training_threshold_) {
    ret = BuildIndex();
    if (ret) {
      LOG(ERROR) << space_name_
                 << " ReBuildIndex BuildIndex failed, ret: " << ret;
      return ret;
    }
  }

  Status status = vec_manager_->CompactVector();
  if (!status.ok()) {
    LOG(ERROR) << space_name_ << "compact vector error: " << status.ToString();
    return -1;
  }

  LOG(INFO) << space_name_ << " vector manager RebuildIndex success!";
  return 0;
}

int Engine::Indexing() {
  // Transition from STARTING to RUNNING
  IndexingState expected = IndexingState::STARTING;
  if (!indexing_state_.compare_exchange_strong(expected,
                                               IndexingState::RUNNING)) {
    LOG(ERROR) << space_name_
               << " unexpected indexing state: " << static_cast<int>(expected);
    indexing_state_.store(IndexingState::IDLE);
    indexing_cv_.notify_all();
    return -1;
  }

  if (vec_manager_->TrainIndex(vec_manager_->VectorIndexes()) != 0) {
    LOG(ERROR) << space_name_ << " create index failed!";
    indexing_state_.store(IndexingState::IDLE);
    indexing_cv_.notify_all();
    return -1;
  }

  LOG(INFO) << space_name_ << " vector manager TrainIndex success!";
  int ret = 0;
  bool has_error = false;

  // Main indexing loop
  while (indexing_state_.load() == IndexingState::RUNNING) {
    if (has_error) {
      usleep(5000 * 1000);  // sleep 5000ms
      continue;
    }
    index_status_ = IndexStatus::INDEXED;
    bool index_is_dirty = false;
    int add_ret = vec_manager_->AddRTVecsToIndex(index_is_dirty);
    if (add_ret < 0) {
      has_error = true;
      LOG(ERROR) << "Add real time vectors to index error!";
      continue;
    }
    if (index_is_dirty == true) {
      is_dirty_ = true;
    }
    if (refresh_interval_ > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(refresh_interval_));
    }
  }

  // Clean up and notify waiters
  indexing_state_.store(IndexingState::IDLE);
  { std::lock_guard<std::mutex> lock(indexing_mutex_); }
  indexing_cv_.notify_all();

  LOG(INFO) << space_name_ << " build index exited!";
  return ret;
}

int Engine::GetDocsNum() { return max_docid_ - delete_num_; }

bool Engine::WaitForIndexingComplete(int timeout_ms) {
  std::unique_lock<std::mutex> lk(indexing_mutex_);

  if (timeout_ms < 0) {
    // Wait indefinitely
    indexing_cv_.wait(
        lk, [this] { return indexing_state_.load() == IndexingState::IDLE; });
    return true;
  } else {
    // Wait with timeout
    auto timeout = std::chrono::milliseconds(timeout_ms);
    return indexing_cv_.wait_for(lk, timeout, [this] {
      return indexing_state_.load() == IndexingState::IDLE;
    });
  }
}

std::string Engine::EngineStatus() {
  nlohmann::json j;
  j["index_status"] = index_status_;
  j["backup_status"] = backup_status_.load();
  j["doc_num"] = GetDocsNum();
  j["max_docid"] = max_docid_ - 1;
  if (created_table_ && vec_manager_ != nullptr) {
    j["min_indexed_num"] = vec_manager_->MinIndexedNum();
  } else {
    j["min_indexed_num"] = 0;
  }
  return j.dump();
}

std::string Engine::GetMemoryInfo() {
  nlohmann::json j;
  if (created_table_ && table_ != nullptr) {
  j["table_mem"] = table_->GetMemoryBytes();
  } else {
    j["table_mem"] = 0;
  }
  long vec_mem_bytes = 0, index_mem_bytes = 0;
  if (created_table_ && vec_manager_ != nullptr) {
    vec_manager_->GetTotalMemBytes(index_mem_bytes, vec_mem_bytes);
  }

  long total_mem_b = 0;
  j["index_mem"] = index_mem_bytes;
  j["vector_mem"] = vec_mem_bytes;
  j["field_range_mem"] = total_mem_b;
  if (docids_bitmap_) {
  j["bitmap_mem"] = docids_bitmap_->BytesSize();
  } else {
    j["bitmap_mem"] = 0;
  }
  return j.dump();
}

int Engine::Dump() {
  int ret = 0;
  if (is_dirty_) {
    int max_docid = max_docid_ - 1;
    std::time_t t = std::time(nullptr);
    char tm_str[100];
    std::strftime(tm_str, sizeof(tm_str), date_time_format_.c_str(),
                  std::localtime(&t));

    std::string path = dump_path_ + "/" + tm_str;
    if (!utils::isFolderExist(path.c_str())) {
      mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }

    ret = vec_manager_->Dump(path, 0, max_docid);
    if (ret != 0) {
      LOG(ERROR) << space_name_ << " dump vector error, ret=" << ret;
      utils::remove_dir(path.c_str());
      LOG(ERROR) << space_name_ << " dumped to [" << path
                 << "] failed, now removed";
      return -1;
    }

    const std::string dump_done_file = path + "/dump.done";
    std::ofstream f_done;
    f_done.open(dump_done_file);
    if (!f_done.is_open()) {
      LOG(ERROR) << "Cannot create file " << dump_done_file;
      return -1;
    }
    f_done << "start_docid " << 0 << std::endl;
    f_done << "end_docid " << max_docid << std::endl;
    f_done.close();

    if (last_dump_dir_ != "" && utils::remove_dir(last_dump_dir_.c_str())) {
      LOG(ERROR) << "remove last dump directory error, path=" << last_dump_dir_;
    }
    LOG(INFO) << space_name_ << " dumped to [" << path
              << "], last dump directory(removed)=" << last_dump_dir_;
    last_dump_dir_ = path;
    is_dirty_ = false;
  }
  return 0;
}

int Engine::CreateTableFromLocal(std::string &table_name) {
  std::vector<std::string> file_paths = utils::ls(index_root_path_);
  for (std::string &file_path : file_paths) {
    std::string::size_type pos = file_path.rfind(".schema");
    if (pos == file_path.size() - 7) {
      std::string::size_type begin = file_path.rfind('/');
      assert(begin != std::string::npos);
      begin += 1;
      table_name = file_path.substr(begin, pos - begin);
      LOG(INFO) << space_name_ << " local table name=" << table_name
                << " path=" << file_path;
      TableSchemaIO tio(file_path);
      TableInfo table;
      if (tio.Read(table_name, table)) {
        LOG(ERROR) << "read table schema error, path=" << file_path;
        return -1;
      }

      Status status = CreateTable(table);
      if (!status.ok()) {
        LOG(ERROR) << "create table error when loading";
        return -1;
      }
      return 0;
    }
  }
  return -1;
}

int Engine::Load() {
  if (!created_table_) {
    std::string table_name;
    if (CreateTableFromLocal(table_name)) {
      LOG(ERROR) << space_name_ << " create table from local error";
      return -1;
    }
    LOG(INFO) << space_name_
              << " create table from local success, table name=" << table_name;
  }

  int64_t doc_num = table_->GetStorageManagerSize();

  std::vector<std::pair<std::time_t, std::string>> folders_tm;
  std::vector<std::string> folders = utils::ls_folder(dump_path_);
  std::vector<std::string> folders_not_done;
  for (const std::string &folder_name : folders) {
    if (folder_name == "") continue;
    std::string folder_path = dump_path_ + "/" + folder_name;
    std::string done_file = folder_path + "/dump.done";
    if (!utils::file_exist(done_file)) {
      LOG(INFO) << done_file << " is not existed, skip it!";
      folders_not_done.push_back(folder_path);
      continue;
    }
    struct tm result;
    strptime(folder_name.c_str(), date_time_format_.c_str(), &result);
    std::time_t t = std::mktime(&result);
    folders_tm.push_back(std::make_pair(t, folder_path));
  }
  std::sort(folders_tm.begin(), folders_tm.end(),
            [](const std::pair<std::time_t, std::string> &a,
               const std::pair<std::time_t, std::string> &b) {
              return a.first < b.first;
            });
  std::string last_dir = "";
  std::vector<std::string> dirs;
  if (folders_tm.size() > 0) {
    last_dir = folders_tm[folders_tm.size() - 1].second;
    LOG(INFO) << "Loading from " << last_dir;
    dirs.push_back(last_dir);
  }
  int ret = vec_manager_->Load(dirs, doc_num);
  if (ret != 0) {
    LOG(ERROR) << space_name_ << " load vector error, ret=" << ret
               << ", path=" << last_dir;
    return ret;
  }

  ret = table_->Load(doc_num);
  if (ret != 0) {
    LOG(ERROR) << space_name_ << " load profile error, ret=" << ret;
    return ret;
  }
  ret = table_->SetStorageManagerSize(doc_num);
  if (ret != 0) {
    LOG(ERROR) << space_name_ << " set table size error, ret=" << ret;
    return ret;
  }
  max_docid_ = doc_num;

  delete_num_ = 0;
  for (int i = 0; i < max_docid_; ++i) {
    if (docids_bitmap_->Test(i)) {
      ++delete_num_;
    }
  }

  // load docid to _id value map
  if (table_->GetEnableIdCache()) {
    table_->LoadIdFromTable();
  }

  if (refresh_interval_ >= 0 and
      indexing_state_.load() == IndexingState::IDLE and
      index_status_ == UNINDEXED) {
    if (max_docid_ - delete_num_ >= training_threshold_) {
      LOG(INFO) << space_name_ << " begin indexing. training_threshold="
                << training_threshold_;
      this->BuildIndex();
    }
  }
  // remove directorys which are not done
  for (const std::string &folder : folders_not_done) {
    if (utils::remove_dir(folder.c_str())) {
      LOG(ERROR) << space_name_
                 << " clean error, not done directory=" << folder;
    }
  }
  last_dump_dir_ = last_dir;
  LOG(INFO) << "load engine success! " << space_name_
            << " max docid=" << max_docid_ << ", delete_num=" << delete_num_
            << ", load directory=" << last_dir
            << ", clean directorys(not done)="
            << utils::join(folders_not_done, ',');
  return 0;
}

int Engine::LoadFromFaiss() {
  std::map<std::string, IndexModel *> &vec_indexes =
      vec_manager_->VectorIndexes();
  if (vec_indexes.size() != 1) {
    LOG(ERROR) << space_name_ << " load from faiss index should be only one!";
    return -1;
  }

  IndexModel *index = vec_indexes.begin()->second;
  if (index == nullptr) {
    LOG(ERROR) << space_name_ << " cannot find faiss index";
    return -1;
  }
  index_status_ = INDEXED;

  int64_t load_num;
  Status status = index->Load("files", load_num);
  if (!status.ok()) {
    LOG(ERROR) << space_name_ << " vector [faiss] load gamma index failed!";
    return -1;
  }

  int d = index->vector_->MetaInfo()->Dimension();
  int fd = open("files/feature", O_RDONLY);
  size_t mmap_size = load_num * sizeof(float) * d;
  float *feature =
      (float *)mmap(NULL, mmap_size, PROT_READ, MAP_PRIVATE, fd, 0);
  for (int64_t i = 0; i < load_num; ++i) {
    Doc doc;
    Field field;
    field.name = "_id";
    field.datatype = DataType::STRING;
    field.value = std::to_string(i);
    doc.SetKey(field.value);
    doc.AddField(std::move(field));
    field.name = "faiss";
    field.datatype = DataType::VECTOR;
    field.value = std::string((char *)(feature + i * d), d * sizeof(float));
    doc.AddField(std::move(field));
    AddOrUpdate(doc);
  }
  munmap(feature, mmap_size);
  close(fd);
  return 0;
}

Status vearch::Engine::Backup(int command) {
  if (backup_status_.load()) {
    return Status::IOError("backup is in progress, wait done");
  }

  backup_status_.store(-1);
  auto func_backup =
      std::bind(&Engine::BackupThread, this, std::placeholders::_1);
  backup_thread_ = std::thread(func_backup, command);
  backup_thread_.join();

  return Status::OK();
}

void Engine::BackupThread(int command) {
  std::string backup_path = index_root_path_ + "/backup";
  utils::make_dir(backup_path.c_str());

  // create
  if (command == 0) {
    backup_status_.store(1);
    Status status = storage_mgr_->Backup(backup_path);
    if (!status.ok()) {
      LOG(ERROR) << space_name_ << " storage backup failed!";
      backup_status_.store(0);
      return;
    }
    status = docids_bitmap_->Backup(backup_path);
    if (!status.ok()) {
      LOG(ERROR) << space_name_ << " bitmap backup failed!";
      backup_status_.store(0);
      return;
    }

    LOG(INFO) << space_name_ << " backup success!";
  }

  backup_status_.store(0);
}

Status Engine::AddFieldIndex(const std::string &field_name,
                             const std::string &indexType,
                             const std::string &indexParam) {
  if (table_ == nullptr) {
    return Status::IOError("table not initialized");
  }

  LOG(INFO) << space_name_ << " starting async AddFieldIndex for field "
            << field_name << " with type: " << indexType
            << ", param: " << indexParam;

  // Join previous thread if it exists
  if (add_field_index_thread_.joinable()) {
    add_field_index_thread_.join();
  }

  // Start async execution
  auto func_add_field_index = std::bind(&Engine::AddFieldIndexThread, this,
                                        field_name, indexType, indexParam);
  add_field_index_thread_ = std::thread(func_add_field_index);

  return Status::OK();
}

Status Engine::RemoveFieldIndex(const std::string &field_name) {
  if (table_ == nullptr) {
    return Status::IOError("table not initialized");
  }

  LOG(INFO) << space_name_ << " starting async RemoveFieldIndex for field "
            << field_name;

  // Join previous thread if it exists
  if (remove_field_index_thread_.joinable()) {
    remove_field_index_thread_.join();
  }

  // Start async execution
  auto func_remove_field_index =
      std::bind(&Engine::RemoveFieldIndexThread, this, field_name);
  remove_field_index_thread_ = std::thread(func_remove_field_index);

  return Status::OK();
}

void Engine::AddFieldIndexThread(const std::string &field_name,
                                 const std::string &indexType,
                                 const std::string &indexParam) {
  LOG(INFO) << space_name_ << " AddFieldIndexThread started for field "
            << field_name << " with type: " << indexType
            << ", param: " << indexParam;

  // Check if it's a vector field
  if (vec_manager_ != nullptr &&
      vec_manager_->Contains(const_cast<std::string &>(field_name))) {
    LOG(INFO) << space_name_ << " adding vector index for field " << field_name
              << " with type: " << indexType << ", param: " << indexParam;

    // Stop current indexing process safely using compare_exchange (similar to
    // RebuildIndex)
    IndexingState expected = IndexingState::RUNNING;
    if (indexing_state_.compare_exchange_strong(expected,
                                                IndexingState::STOPPING)) {
      LOG(INFO)
          << space_name_
          << " stopping current indexing process for field index addition...";
      if (WaitForIndexingComplete()) {
        LOG(INFO) << space_name_
                  << " indexing process stopped, proceeding with field index "
                     "addition";
      } else {
        LOG(WARNING)
            << space_name_
            << " timeout waiting for indexing to stop, proceeding anyway";
      }
    } else if (expected == IndexingState::STARTING) {
      // Wait for starting to complete
      LOG(INFO) << space_name_
                << " waiting for indexing to start before stopping...";
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      expected = IndexingState::RUNNING;
      if (indexing_state_.compare_exchange_strong(expected,
                                                  IndexingState::STOPPING)) {
        WaitForIndexingComplete();
      }
    }

    if (indexing_thread_.joinable()) {
      indexing_thread_.join();
    }

    // Add the new index type and parameter
    vec_manager_->AddIndexTypeAndParam(indexType, indexParam);

    // Create vector indexes with the new configuration
    std::map<std::string, IndexModel *> vector_indexes;
    Status status =
        vec_manager_->CreateVectorIndexes(training_threshold_, vector_indexes);
    if (!status.ok()) {
      LOG(ERROR) << space_name_ << " AddFieldIndex CreateVectorIndexes failed: "
                 << status.ToString();
      vec_manager_->DestroyVectorIndexes();
      return;
    }

    // Train index if we have enough documents
    if (indexing_state_.load() == IndexingState::IDLE &&
        max_docid_ - delete_num_ > training_threshold_) {
      int ret = vec_manager_->TrainIndex(vector_indexes);
      if (ret) {
        LOG(ERROR) << space_name_
                   << " AddFieldIndex TrainIndex failed, ret=" << ret;
        index_status_ = IndexStatus::UNINDEXED;
        return;
      }
    }

    // Reset vector indexes with the new configuration
    vec_manager_->ResetVectorIndexes(vector_indexes);

    // Start indexing process if conditions are met
    if (refresh_interval_ >= 0 &&
        indexing_state_.load() == IndexingState::IDLE &&
        max_docid_ - delete_num_ >= training_threshold_) {
      int ret = BuildIndex();
      if (ret) {
        LOG(ERROR) << space_name_
                   << " AddFieldIndex BuildIndex failed, ret: " << ret;
        return;
      }
    }

    // Compact vector storage
    Status compact_status = vec_manager_->CompactVector();
    if (!compact_status.ok()) {
      LOG(ERROR) << space_name_ << " AddFieldIndex compact vector error: "
                 << compact_status.ToString();
      return;
    }

    LOG(INFO) << space_name_
              << " vector index added successfully for field: " << field_name;
    return;
  }

  // Check if field exists in table
  int field_id = table_->GetAttrIdx(field_name);
  if (field_id < 0) {
    LOG(ERROR) << space_name_ << " field not found: " << field_name;
    return;
  }

  // For other fields, add to field range index if it supports indexing
  if (field_range_index_ != nullptr) {
    DataType field_type;
    if (table_->GetFieldType(field_name, field_type) == 0) {
      // Add field to range index
      std::string field_name_copy = field_name;
      int result =
          field_range_index_->AddField(field_id, field_type, field_name_copy);
      if (result != 0) {
        LOG(ERROR) << space_name_
                   << " failed to add field to range index for field: "
                   << field_name;
        return;
      }
      LOG(INFO) << space_name_
                << " added range index for field: " << field_name;

      // Add index for all existing documents
      LOG(INFO) << space_name_
                << " building index for existing documents for field: "
                << field_name;
      for (int64_t docid = 0; docid < max_docid_; ++docid) {
        if (!docids_bitmap_->Test(docid)) {  // Only for non-deleted documents
          field_range_index_->AddDoc(docid, field_id);
        }
      }
      LOG(INFO) << space_name_ << " completed building index for "
                << (max_docid_ - delete_num_)
                << " existing documents for field: " << field_name;
    }
  }

  LOG(INFO) << space_name_
            << " AddFieldIndexThread completed for field: " << field_name;
}

void Engine::RemoveFieldIndexThread(const std::string &field_name) {
  LOG(INFO) << space_name_ << " RemoveFieldIndexThread started for field "
            << field_name;

  // Check if it's a vector field
  if (vec_manager_ != nullptr &&
      vec_manager_->Contains(const_cast<std::string &>(field_name))) {
    LOG(INFO) << space_name_ << " removing vector index for field "
              << field_name;

    IndexingState expected = IndexingState::RUNNING;
    if (indexing_state_.compare_exchange_strong(expected,
                                                IndexingState::STOPPING)) {
      LOG(INFO) << space_name_
                << " stopping current indexing process for field removal...";
      if (WaitForIndexingComplete()) {
        LOG(INFO) << space_name_
                  << " indexing process stopped, proceeding with field removal";
      } else {
        LOG(WARNING)
            << space_name_
            << " timeout waiting for indexing to stop, proceeding anyway";
      }
    } else if (expected == IndexingState::STARTING) {
      // Wait for starting to complete
      LOG(INFO) << space_name_
                << " waiting for indexing to start before stopping...";
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      expected = IndexingState::RUNNING;
      if (indexing_state_.compare_exchange_strong(expected,
                                                  IndexingState::STOPPING)) {
        WaitForIndexingComplete();
      }
    }

    if (indexing_thread_.joinable()) {
      indexing_thread_.join();
    }

    Status remove_status = vec_manager_->RemoveVectorIndex(field_name);
    if (!remove_status.ok()) {
      LOG(ERROR) << space_name_ << " failed to remove vector index for field "
                 << field_name << ": " << remove_status.ToString();
      return;
    }

    index_status_ = IndexStatus::UNINDEXED;

    LOG(INFO) << space_name_
              << " vector index removed for field: " << field_name;
    return;
  }

  // Check if field exists in table
  int field_id = table_->GetAttrIdx(field_name);
  if (field_id < 0) {
    LOG(ERROR) << space_name_ << " field not found: " << field_name;
    return;
  }

  // For other fields, remove from field range index if it exists
  if (field_range_index_ != nullptr) {
    LOG(INFO) << space_name_
              << " removing range index for field: " << field_name;

    // Remove index for all existing documents
    LOG(INFO) << space_name_
              << " removing index from existing documents for field: "
              << field_name;
    for (int64_t docid = 0; docid < max_docid_; ++docid) {
      if (!docids_bitmap_->Test(docid)) {  // Only for non-deleted documents
        field_range_index_->Delete(docid, field_id);
      }
    }
    LOG(INFO) << space_name_ << " completed removing index from "
              << (max_docid_ - delete_num_)
              << " existing documents for field: " << field_name;

    // Remove field from range index
    int result = field_range_index_->RemoveField(field_id);
    if (result != 0) {
      LOG(ERROR) << space_name_
                 << " failed to remove field from range index for field: "
                 << field_name;
      return;
    }
    LOG(INFO) << space_name_
              << " removed range index for field: " << field_name;
  }

  LOG(INFO) << space_name_
            << " RemoveFieldIndexThread completed for field: " << field_name;
}

int Engine::AddNumIndexFields() {
  int retvals = 0;
  std::map<std::string, enum DataType> attr_type;
  retvals = table_->GetAttrType(attr_type);

  field_range_index_->Init();
  std::map<std::string, bool> attr_index;
  retvals = table_->GetAttrIsIndex(attr_index);
  for (const auto &it : attr_type) {
    std::string field_name = it.first;
    const auto &attr_index_it = attr_index.find(field_name);
    if (attr_index_it == attr_index.end()) {
      LOG(ERROR) << space_name_ << " cannot find field [" << field_name << "]";
      continue;
    }
    bool is_index = attr_index_it->second;
    if (not is_index) {
      continue;
    }
    int field_idx = table_->GetAttrIdx(field_name);
    LOG(INFO) << space_name_ << " add range field [" << field_name << "]";
    field_range_index_->AddField(field_idx, it.second, field_name);
  }
  return retvals;
}

int Engine::GetConfig(std::string &conf_str) {
  size_t table_cache_size = 0;
  storage_mgr_->GetCacheSize(table_cache_size);
  nlohmann::json j;
  j["engine_cache_size"] = table_cache_size;
  j["path"] = index_root_path_;
  j["long_search_time"] = long_search_time_;
  j["refresh_interval"] = refresh_interval_;
  j["enable_id_cache"] = table_->GetEnableIdCache();
  conf_str = j.dump();
  return 0;
}

int Engine::SetConfig(std::string conf_str) {
  nlohmann::json j = nlohmann::json::parse(conf_str);

  if (j.contains("engine_cache_size")) {
    size_t table_cache_size = j["engine_cache_size"];
    storage_mgr_->AlterCacheSize(table_cache_size);
  }

  if (j.contains("path")) {
    index_root_path_ = j["path"];
  }

  if (j.contains("long_search_time")) {
    long_search_time_ = j["long_search_time"];
  }

  if (j.contains("refresh_interval")) {
    refresh_interval_ = j["refresh_interval"];
    LOG(INFO) << space_name_
              << " update refresh_interval=" << refresh_interval_;
  }

  if (j.contains("enable_id_cache")) {
    bool enable_id_cache = j["enable_id_cache"];
    if (enable_id_cache != table_->GetEnableIdCache()) {
      table_->SetEnableIdCache(enable_id_cache);
      LOG(INFO) << space_name_
              << " update enable_id_cache=" << enable_id_cache;
    }
  }
  return 0;
}

}  // namespace vearch
