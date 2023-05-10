/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_engine.h"

#include <fcntl.h>
#include <locale.h>
#ifndef __APPLE__
#include <malloc.h>
#endif
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <thread>
#include <vector>

#include "cjson/cJSON.h"
#include "common/gamma_common_data.h"
#include "gamma_table_io.h"
#include "io/raw_vector_io.h"
#include "omp.h"
#include "search/error_code.h"
#include "util/bitmap.h"
#include "util/log.h"
#include "util/utils.h"

using std::string;
using namespace tig_gamma::table;

namespace tig_gamma {

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

#ifdef DEBUG
static string float_array_to_string(float *data, int len) {
  if (data == nullptr) return "";
  std::stringstream ss;
  ss << "[";
  for (int i = 0; i < len; ++i) {
    ss << data[i];
    if (i != len - 1) {
      ss << ",";
    }
  }
  ss << "]";
  return ss.str();
}

static string VectorQueryToString(VectorQuery *vector_query) {
  std::stringstream ss;
  ss << "name:"
     << std::string(vector_query->name->value, vector_query->name->len)
     << " min score:" << vector_query->min_score
     << " max score:" << vector_query->max_score
     << " boost:" << vector_query->boost
     << " has boost:" << vector_query->has_boost << " value:"
     << float_array_to_string((float *)vector_query->value->value,
                              vector_query->value->len / sizeof(float));
  return ss.str();
}

// static string RequestToString(const Request *request) {
//   std::stringstream ss;
//   ss << "{req_num:" << request->req_num << " topn:" << request->topn
//      << " has_rank:" << request->has_rank
//      << " vec_num:" << request->vec_fields_num;
//   for (int i = 0; i < request->vec_fields_num; ++i) {
//     ss << " vec_id:" << i << " [" <<
//     VectorQueryToString(request->vec_fields[i])
//        << "]";
//   }
//   ss << "}";
//   return ss.str();
// }
#endif  // DEBUG

#ifndef __APPLE__
static std::thread *gMemTrimThread = nullptr;
void MemTrimHandler() {
  LOG(INFO) << "memory trim thread start......";
  while (1) {
    malloc_trim(0);
    std::this_thread::sleep_for(std::chrono::seconds(60));  // 1 minute
  }
  LOG(INFO) << "memory trim thread exit!";
}
#endif

GammaEngine::GammaEngine(const string &index_root_path)
    : index_root_path_(index_root_path),
      date_time_format_("%Y-%m-%d-%H:%M:%S") {
  table_ = nullptr;
  vec_manager_ = nullptr;
  index_status_ = IndexStatus::UNINDEXED;
  delete_num_ = 0;
  b_running_ = 0;
  b_field_running_ = false;
  is_dirty_ = false;
  field_range_index_ = nullptr;
  created_table_ = false;
  b_loading_ = false;
  docids_bitmap_ = nullptr;
  migrate_data_ = nullptr;
#ifdef PERFORMANCE_TESTING
  search_num_ = 0;
#endif
}

GammaEngine::~GammaEngine() {
  if (b_running_) {
    b_running_ = 0;
    std::mutex running_mutex;
    std::unique_lock<std::mutex> lk(running_mutex);
    running_cv_.wait(lk);
  }

  if (b_field_running_) {
    b_field_running_ = false;
    std::mutex running_mutex;
    std::unique_lock<std::mutex> lk(running_mutex);
    running_field_cv_.wait(lk);
  }

  if (vec_manager_) {
    delete vec_manager_;
    vec_manager_ = nullptr;
  }

  if (table_) {
    delete table_;
    table_ = nullptr;
  }

  if (field_range_index_) {
    delete field_range_index_;
    field_range_index_ = nullptr;
  }

  if (docids_bitmap_) {
    delete docids_bitmap_;
    docids_bitmap_ = nullptr;
  }
  if (migrate_data_) {
    migrate_data_->TerminateMigrate(index_root_path_);
    delete migrate_data_;
    migrate_data_ = nullptr;
  }
}

GammaEngine *GammaEngine::GetInstance(const string &index_root_path) {
  GammaEngine *engine = new GammaEngine(index_root_path);
  int ret = engine->Setup();
  if (ret < 0) {
    LOG(ERROR) << "BuildSearchEngine [" << index_root_path << "] error!";
    return nullptr;
  }
  return engine;
}

int GammaEngine::Setup() {
  if (!utils::isFolderExist(index_root_path_.c_str())) {
    mkdir(index_root_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  dump_path_ = index_root_path_ + "/retrieval_model_index";
  if (!utils::isFolderExist(dump_path_.c_str())) {
    mkdir(dump_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  docids_bitmap_ = new bitmap::BitmapManager();
  docids_bitmap_->SetDumpFilePath(index_root_path_ + "/bitmap");
  int init_bitmap_size = 5000 * 10000;
  bool is_load = false;
  int file_bytes_size = docids_bitmap_->FileBytesSize();
  if (file_bytes_size != 0) {
    init_bitmap_size = file_bytes_size * 8;
    is_load = true;
  }

  if (docids_bitmap_->Init(init_bitmap_size) != 0) {
    LOG(ERROR) << "Cannot create bitmap!";
    return INTERNAL_ERR;
  }
  if (is_load) {
    docids_bitmap_->Load();
  } else {
    docids_bitmap_->Dump();
  }

  if (!table_) {
    table_ = new Table(index_root_path_);
  }

  if (!vec_manager_) {
    vec_manager_ = new VectorManager(VectorStorageType::Mmap, docids_bitmap_,
                                     index_root_path_);
  }

#ifndef __APPLE__
  if (gMemTrimThread == nullptr) {
    gMemTrimThread = new std::thread(MemTrimHandler);
    if (gMemTrimThread) {
      gMemTrimThread->detach();
    } else {
      LOG(ERROR) << "create memory trim thread error";
    }
  }
#endif

  max_docid_ = 0;
  LOG(INFO) << "GammaEngine setup successed! bitmap_bytes_size="
            << docids_bitmap_->BytesSize();
  return 0;
}

int GammaEngine::Search(Request &request, Response &response_results) {
#ifdef DEBUG
// LOG(INFO) << "search request:" << RequestToString(request);
#endif

  int ret = 0;
  int req_num = request.ReqNum();

  if (req_num <= 0) {
    string msg = "req_num should not less than 0";
    LOG(ERROR) << msg;
    return -1;
  }

  bool req_permit = RequestConcurrentController::GetInstance().Acquire(req_num);
  if (not req_permit) {
    LOG(WARNING) << "Resource temporarily unavailable";
    RequestConcurrentController::GetInstance().Release(req_num);
    return -1;
  }

  // TODO: it may be opened later
  // utils::OnlineLogger logger;
  // if (0 != logger.Init(online_log_level)) {
  //   LOG(WARNING) << "init online logger error!";
  // }

  int topn = request.TopN();
  bool brute_force_search = request.BruteForceSearch();

  if ((not brute_force_search) && (index_status_ != IndexStatus::INDEXED)) {
    string msg = "index not trained!";
    LOG(WARNING) << msg;
    for (int i = 0; i < req_num; ++i) {
      SearchResult result;
      result.msg = msg;
      result.result_code = SearchResultCode::INDEX_NOT_TRAINED;
      response_results.AddResults(std::move(result));
    }
    RequestConcurrentController::GetInstance().Release(req_num);
    return -2;
  }

  std::vector<struct VectorQuery> &vec_fields = request.VecFields();
  GammaQuery gamma_query;
  gamma_query.vec_query = vec_fields;

  gamma_query.condition = new GammaSearchCondition(static_cast<PerfTool *>(response_results.GetPerTool()));
  gamma_query.condition->topn = topn;
  gamma_query.condition->multi_vector_rank =
      request.MultiVectorRank() == 1 ? true : false;
  gamma_query.condition->brute_force_search = brute_force_search;
  gamma_query.condition->l2_sqrt = request.L2Sqrt();
  gamma_query.condition->retrieval_parameters = request.RetrievalParams();
  gamma_query.condition->has_rank = request.HasRank();

  gamma_query.condition->range_filters = request.RangeFilters();
  gamma_query.condition->term_filters = request.TermFilters();
  gamma_query.condition->table = table_;

  MultiRangeQueryResults range_query_result;
  std::vector<struct RangeFilter> &range_filters = request.RangeFilters();
  size_t range_filters_num = range_filters.size();

  std::vector<struct TermFilter> &term_filters = request.TermFilters();
  size_t term_filters_num = term_filters.size();
  if (range_filters_num > 0 || term_filters_num > 0) {
    int num = MultiRangeQuery(request, gamma_query.condition, response_results,
                              &range_query_result);
    if (num == 0) {
      RequestConcurrentController::GetInstance().Release(req_num);
      return 0;
    }
  }
#ifdef PERFORMANCE_TESTING
  gamma_query.condition->GetPerfTool().Perf("filter");
#endif

  size_t vec_fields_num = vec_fields.size();
  if (vec_fields_num > 0) {
    GammaResult *gamma_results = new GammaResult[req_num];

    int doc_num = GetDocsNum();

    for (int i = 0; i < req_num; ++i) {
      gamma_results[i].total = doc_num;
    }

    ret = vec_manager_->Search(gamma_query, gamma_results);
    if (ret != 0) {
      string msg = "search error [" + std::to_string(ret) + "]";
      for (int i = 0; i < req_num; ++i) {
        SearchResult result;
        result.msg = msg;
        result.result_code = SearchResultCode::SEARCH_ERROR;
        response_results.AddResults(std::move(result));
      }
      RequestConcurrentController::GetInstance().Release(req_num);
      delete[] gamma_results;
      return -3;
    }

#ifdef PERFORMANCE_TESTING
    gamma_query.condition->GetPerfTool().Perf("search total");
#endif
    response_results.SetEngineInfo(table_, vec_manager_, gamma_results, req_num);
  } else {
    GammaResult *gamma_result = new GammaResult[1];

    std::vector<std::pair<string, int>> fields_ids;
    std::vector<string> vec_names;

    const auto range_result = range_query_result.GetAllResult();
    if (range_result == nullptr && term_filters_num > 0) {
      for (size_t i = 0; i < term_filters_num; ++i) {
        struct TermFilter &term_filter = term_filters[i];

        string value = term_filter.field;

        int doc_id = -1;
        if (table_->GetDocIDByKey(term_filter.value, doc_id) != 0) {
          continue;
        }

        fields_ids.emplace_back(std::make_pair(value, doc_id));
        vec_names.emplace_back(std::move(value));
      }
      if (fields_ids.size() > 0) {
        gamma_result->init(topn, vec_names.data(), fields_ids.size());
        std::vector<string> vec;
        int ret = vec_manager_->GetVector(fields_ids, vec);
        if (ret == 0) {
          int idx = 0;
          VectorDoc *doc = gamma_result->docs[gamma_result->results_count];
          for (const auto &field_id : fields_ids) {
            int id = field_id.second;
            doc->docid = id;
            doc->fields[idx].name = vec[idx];
            doc->fields[idx].source = nullptr;
            doc->fields[idx].source_len = 0;
            ++idx;
          }
          ++(gamma_result->results_count);
          gamma_result->total = 1;
        }
      }
    } else {
      gamma_result->init(topn, nullptr, 0);
      for (int docid = 0; docid < max_docid_; ++docid) {
        if (range_query_result.Has(docid) && !docids_bitmap_->Test(docid)) {
          ++(gamma_result->total);
          if (gamma_result->results_count < topn) {
            gamma_result->docs[(gamma_result->results_count)++]->docid = docid;
          }
        }
      }
    }
    response_results.SetEngineInfo(table_, vec_manager_, gamma_result, 1);
  }

#ifdef PERFORMANCE_TESTING
  std::string online_log_level = request.OnlineLogLevel();
  if (strncasecmp("debug", online_log_level.c_str(), 5) == 0) {
    response_results.SetOnlineLogMessage(
        gamma_query.condition->GetPerfTool().OutputPerf().str());
  }
#endif // PERFORMANCE_TESTING

  RequestConcurrentController::GetInstance().Release(req_num);
  return ret;
}

int GammaEngine::MultiRangeQuery(Request &request,
                                 GammaSearchCondition *condition,
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

    ++idx;
  }

  for (int i = 0; i < term_filters_size; ++i) {
    struct TermFilter &filter = term_filters[i];

    filters[idx].field = table_->GetAttrIdx(filter.field);
    filters[idx].lower_value = filter.value;
    filters[idx].is_union = static_cast<FilterOperator>(filter.is_union);

    ++idx;
  }

  int retval = field_range_index_->Search(filters, range_query_result);

  if (retval == 0) {
    string msg = "No result: numeric filter return 0 result";
    LOG(INFO) << msg;
    for (int i = 0; i < request.ReqNum(); ++i) {
      SearchResult result;
      result.msg = msg;
      result.result_code = SearchResultCode::SUCCESS;
      response_results.AddResults(std::move(result));
    }
  } else if (retval < 0) {
    condition->range_query_result = nullptr;
  } else {
    condition->range_query_result = range_query_result;
  }
  return retval;
}

int GammaEngine::CreateTable(TableInfo &table) {
  if (!vec_manager_ || !table_) {
    LOG(ERROR) << "vector and table should not be null!";
    return -1;
  }

  string dump_meta_path = index_root_path_ + "/dump.meta";
  utils::JsonParser *meta_jp = nullptr;
  utils::ScopeDeleter1<utils::JsonParser> del1;
  if (utils::file_exist(dump_meta_path)) {
    long len = utils::get_file_size(dump_meta_path);
    if (len > 0) {
      utils::FileIO fio(dump_meta_path);
      if (fio.Open("r")) {
        LOG(ERROR) << "open file error, path=" << dump_meta_path;
        return IO_ERR;
      }
      char *buf = new char[len + 1];
      buf[len] = '\0';
      if ((size_t)len != fio.Read(buf, 1, (size_t)len)) {
        LOG(ERROR) << "read file error, path=" << dump_meta_path;
        return IO_ERR;
      }
      meta_jp = new utils::JsonParser();
      del1.set(meta_jp);
      if (meta_jp->Parse(buf)) {
        return FORMAT_ERR;
      }
    }
  }

  if (vec_manager_->CreateVectorTable(table, meta_jp) != 0) {
    LOG(ERROR) << "Cannot create VectorTable!";
    return -2;
  }
  TableParams disk_table_params;
  if (meta_jp) {
    utils::JsonParser table_jp;
    meta_jp->GetObject("table", table_jp);
    disk_table_params.Parse(table_jp);
  }
  int ret_table = table_->CreateTable(table, disk_table_params, docids_bitmap_);
  indexing_size_ = table.IndexingSize();
  if (ret_table != 0) {
    LOG(ERROR) << "Cannot create table!";
    return -2;
  }

  if (!meta_jp) {
    utils::JsonParser dump_meta_;
    dump_meta_.PutInt("version", 327);  // version=3.2.0

    utils::JsonParser table_jp;
    table_->GetDumpConfig()->ToJson(table_jp);
    dump_meta_.PutObject("table", std::move(table_jp));

    utils::JsonParser vectors_jp;
    for (auto &it : vec_manager_->RawVectors()) {
      DumpConfig *dc = it.second->GetDumpConfig();
      if (dc) {
        utils::JsonParser jp;
        dc->ToJson(jp);
        vectors_jp.PutObject(dc->name, std::move(jp));
      }
    }
    dump_meta_.PutObject("vectors", std::move(vectors_jp));

    utils::FileIO fio(dump_meta_path);
    fio.Open("w");
    string meta_str = dump_meta_.ToStr(true);
    fio.Write(meta_str.c_str(), 1, meta_str.size());
  }

  field_range_index_ = new MultiFieldsRangeIndex(index_root_path_, table_);
  if ((nullptr == field_range_index_) || (AddNumIndexFields() < 0)) {
    LOG(ERROR) << "add numeric index fields error!";
    return -3;
  }

  auto func_build_field_index = std::bind(&GammaEngine::BuildFieldIndex, this);
  std::thread t(func_build_field_index);
  t.detach();
  
  std::string table_name = table.Name();
  std::string path = index_root_path_ + "/" + table_name + ".schema";
  TableSchemaIO tio(path);  // rewrite it if the path is already existed
  if (tio.Write(table)) {
    LOG(ERROR) << "write table schema error, path=" << path;
  }

  LOG(INFO) << "create table [" << table_name << "] success!";
  created_table_ = true;
  return 0;
}

int GammaEngine::AddOrUpdate(Doc &doc) {
#ifdef PERFORMANCE_TESTING
  double start = utils::getmillisecs();
#endif
  std::vector<struct Field> &fields_table = doc.TableFields();
  std::vector<struct Field> &fields_vec = doc.VectorFields();
  std::string &key = doc.Key();

  // add fields into table
  int docid = -1;
  table_->GetDocIDByKey(key, docid);
  if (docid == -1) {
    int ret = table_->Add(key, fields_table, max_docid_);
    if (ret != 0) return -2;
    for (size_t i = 0; i < fields_table.size(); ++i) {
      struct Field &field = fields_table[i];
      int idx = table_->GetAttrIdx(field.name);
      field_range_index_->Add(max_docid_, idx);
    }
  } else {
    if (Update(docid, fields_table, fields_vec)) {
      LOG(ERROR) << "update error, key=" << key << ", docid=" << docid;
      return -3;
    }
    is_dirty_ = true;
    return 0;
  }
#ifdef PERFORMANCE_TESTING
  double end_table = utils::getmillisecs();
#endif

  // add vectors by VectorManager
  if (vec_manager_->AddToStore(max_docid_, fields_vec) != 0) {
    LOG(ERROR) << "Add to store error max_docid [" << max_docid_ << "]";
    return -4;
  }
  if (migrate_data_) { migrate_data_->AddDocid(max_docid_); }
  ++max_docid_;
  docids_bitmap_->SetMaxID(max_docid_);

  if (not b_running_ and index_status_ == UNINDEXED) {
    if (max_docid_ >= indexing_size_) {
      LOG(INFO) << "Begin indexing.";
      this->BuildIndex();
    }
  }
#ifdef PERFORMANCE_TESTING
  double end = utils::getmillisecs();
  if (max_docid_ % 10000 == 0) {
    LOG(INFO) << "table cost [" << end_table - start << "]ms, vec store cost ["
              << end - end_table << "]ms";
  }
#endif
  is_dirty_ = true;
  return 0;
}

int GammaEngine::AddOrUpdateDocs(Docs &docs, BatchResult &result) {
#ifdef PERFORMANCE_TESTING
  double start = utils::getmillisecs();
#endif
  std::vector<Doc> &doc_vec = docs.GetDocs();
  std::set<std::string> remove_dupliacte;
  int batch_size = 0, start_id = 0;

  auto batchAdd = [&](int start_id, int batch_size) {
    if (batch_size <= 0) return;

    int ret =
        table_->BatchAdd(start_id, batch_size, max_docid_, doc_vec, result);
    if (ret != 0) {
      LOG(ERROR) << "BatchAdd to table error";
      return;
    }

    for (int i = start_id; i < start_id + batch_size; ++i) {
      Doc &doc = doc_vec[i];
      std::vector<struct Field> &fields_table = doc.TableFields();
      for (size_t j = 0; j < fields_table.size(); ++j) {
        struct Field &field = fields_table[j];
        int idx = table_->GetAttrIdx(field.name);
        field_range_index_->Add(max_docid_ + i - start_id, idx);
      }
      // add vectors by VectorManager
      std::vector<struct Field> &fields_vec = doc.VectorFields();
      ret = vec_manager_->AddToStore(max_docid_ + i - start_id, fields_vec);
      if (ret != 0) {
        std::string msg = "Add to vector manager error";
        result.SetResult(i, -1, msg);
        LOG(ERROR) << msg;
        continue;
      }
      if (migrate_data_) { migrate_data_->AddDocid(max_docid_ + i - start_id); }
    }

    max_docid_ += batch_size;
    docids_bitmap_->SetMaxID(max_docid_);
  };

  for (size_t i = 0; i < doc_vec.size(); ++i) {
    Doc &doc = doc_vec[i];
    std::string &key = doc.Key();
    auto ite = remove_dupliacte.find(key);
    if (ite == remove_dupliacte.end()) remove_dupliacte.insert(key);
    // add fields into table
    int docid = -1;
    table_->GetDocIDByKey(key, docid);
    if (docid == -1 && ite == remove_dupliacte.end()) {
      ++batch_size;
      continue;
    } else {
      batchAdd(start_id, batch_size);
      batch_size = 0;
      start_id = i + 1;
      std::vector<struct Field> &fields_table = doc.TableFields();
      std::vector<struct Field> &fields_vec = doc.VectorFields();
      if (ite != remove_dupliacte.end()) table_->GetDocIDByKey(key, docid);
      if (Update(docid, fields_table, fields_vec)) {
        LOG(ERROR) << "update error, key=" << key << ", docid=" << docid;
        continue;
      }
    }
  }

  batchAdd(start_id, batch_size);
  if (not b_running_ and index_status_ == UNINDEXED) {
    if (max_docid_ >= indexing_size_) {
      LOG(INFO) << "Begin indexing.";
      this->BuildIndex();
    }
  }
#ifdef PERFORMANCE_TESTING
  double end = utils::getmillisecs();
  if (max_docid_ % 10000 == 0) {
    LOG(INFO) << "Doc_num[" << max_docid_ << "], BatchAdd[" << batch_size
              << "] total cost [" << end - start << "]ms";
  }
#endif
  is_dirty_ = true;
  return 0;
}

int GammaEngine::Update(int doc_id, std::vector<struct Field> &fields_table,
                        std::vector<struct Field> &fields_vec) {
  int ret = vec_manager_->Update(doc_id, fields_vec);
  if (ret != 0) {
    return ret;
  }

  std::vector<bool> is_equal = table_->CheckFieldIsEqual(fields_table, doc_id);
  for (size_t i = 0; i < fields_table.size(); ++i) {
    struct Field &field = fields_table[i];
    if (is_equal[i] == true) {
      continue;
    }
    LOG(INFO) << "value of field.name[" << field.name
              << "] is changed, if has index it is updated";
    int idx = table_->GetAttrIdx(field.name);
    field_range_index_->Delete(doc_id, idx);
  }

  if (table_->Update(fields_table, doc_id) != 0) {
    LOG(ERROR) << "table update error";
    return -1;
  }

  for (size_t i = 0; i < fields_table.size(); ++i) {
    if (is_equal[i] == true) {
      continue;
    }
    struct Field &field = fields_table[i];
    int idx = table_->GetAttrIdx(field.name);
    field_range_index_->Add(doc_id, idx);
  }
  if (migrate_data_) { migrate_data_->AddDocid(doc_id); }

#ifdef DEBUG
  LOG(INFO) << "update success! key=" << key;
#endif
  is_dirty_ = true;
  return 0;
}

int GammaEngine::Delete(std::string &key) {
  int docid = -1, ret = 0;
  ret = table_->GetDocIDByKey(key, docid);
  if (ret != 0 || docid < 0) return -1;

  if (docids_bitmap_->Test(docid)) {
    return ret;
  }
  ++delete_num_;
  docids_bitmap_->Set(docid);
  docids_bitmap_->Dump(docid, 1);
  const auto &name_to_idx = table_->FieldMap();
  for (const auto &ite : name_to_idx) {
    field_range_index_->Delete(docid, ite.second);
  }
  table_->Delete(key);

  vec_manager_->Delete(docid);
  if (migrate_data_) { migrate_data_->DeleteDocid(docid); }
  is_dirty_ = true;

  return ret;
}

int GammaEngine::DelDocByQuery(Request &request) {
#ifdef DEBUG
// LOG(INFO) << "delete by query request:" << RequestToString(request);
#endif

  std::vector<struct RangeFilter> &range_filters = request.RangeFilters();

  if (range_filters.size() <= 0) {
    LOG(ERROR) << "no range filter";
    return 1;
  }
  MultiRangeQueryResults range_query_result;  // Note its scope

  std::vector<FilterInfo> filters;
  filters.resize(range_filters.size());
  int idx = 0;

  for (size_t i = 0; i < range_filters.size(); ++i) {
    struct RangeFilter &range_filter = range_filters[i];

    filters[idx].field = table_->GetAttrIdx(range_filter.field);
    filters[idx].lower_value = range_filter.lower_value;
    filters[idx].upper_value = range_filter.upper_value;

    ++idx;
  }

  int retval = field_range_index_->Search(filters, &range_query_result);
  if (retval == 0) {
    LOG(ERROR) << "numeric index search error, ret=" << retval;
    return 1;
  }

  std::vector<int> doc_ids = range_query_result.ToDocs();
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    int docid = doc_ids[i];
    if (docids_bitmap_->Test(docid)) {
      continue;
    }
    ++delete_num_;
    docids_bitmap_->Set(docid);
    docids_bitmap_->Dump(docid, 1);
  }
  is_dirty_ = true;
  return 0;
}

int GammaEngine::DelDocByFilter(Request &request, char **del_ids,
                                int *str_len) {
  *str_len = 0;
  MultiRangeQueryResults range_query_result;
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

    ++idx;
  }

  for (int i = 0; i < term_filters_size; ++i) {
    struct TermFilter &filter = term_filters[i];

    filters[idx].field = table_->GetAttrIdx(filter.field);
    filters[idx].lower_value = filter.value;
    filters[idx].is_union = static_cast<FilterOperator>(filter.is_union);

    ++idx;
  }

  int retval = field_range_index_->Search(filters, &range_query_result);

  int del_num = 0;
  cJSON *root = cJSON_CreateArray();
  if (retval > 0) {
    for (int del_docid = 0; del_docid < max_docid_; ++del_docid) {
      if (range_query_result.Has(del_docid) == true) {
        std::string key;
        if (table_->GetKeyByDocid(del_docid, key) !=
            0) {  // docid can be deleted.
          continue;
        }
        if (docids_bitmap_->Test(del_docid)) continue;

        docids_bitmap_->Set(del_docid);
        docids_bitmap_->Dump(del_docid, 1);
        const auto &name_to_idx = table_->FieldMap();
        for (const auto &ite : name_to_idx) {
          field_range_index_->Delete(del_docid, ite.second);
        }
        table_->Delete(key);
        vec_manager_->Delete(del_docid);
        if (migrate_data_) { migrate_data_->DeleteDocid(del_docid); }
        if (table_->IdType() == 0) {
          cJSON_AddItemToArray(root, cJSON_CreateString(key.c_str()));
        } else {
          long key_long;
          memcpy(&key_long, key.c_str(), sizeof(key_long));
          cJSON_AddItemToArray(root, cJSON_CreateNumber(key_long));
        }
        ++delete_num_;
        ++del_num;
      }
    }
  }
  LOG(INFO) << "DelDocByFilter(), Delete doc num: " << del_num;

  *del_ids = cJSON_PrintUnformatted(root);
  *str_len = strlen(*del_ids);
  if (root) cJSON_Delete(root);
  is_dirty_ = true;
  return 0;
}

int GammaEngine::GetDoc(const std::string &key, Doc &doc) {
  int docid = -1, ret = 0;
  ret = table_->GetDocIDByKey(key, docid);
  if (ret != 0 || docid < 0) {
    LOG(INFO) << "GetDocIDbyKey [" << key << "] error!";
    return -1;
  }

  return GetDoc(docid, doc);
}

int GammaEngine::GetDoc(int docid, Doc &doc) {
  int ret = 0;
  if (docids_bitmap_->Test(docid)) {
    LOG(INFO) << "docid [" << docid << "] is deleted!";
    return -1;
  }
  std::vector<std::string> index_names;
  vec_manager_->VectorNames(index_names);

  std::vector<string> table_fields;
  ret = table_->GetDocInfo(docid, doc, table_fields);
  if (ret != 0) {
    return ret;
  }

  std::vector<std::pair<std::string, int>> vec_fields_ids;
  for (size_t i = 0; i < index_names.size(); ++i) {
    vec_fields_ids.emplace_back(std::make_pair(index_names[i], docid));
  }

  std::vector<std::string> vec;
  ret = vec_manager_->GetVector(vec_fields_ids, vec, true);
  if (ret == 0 && vec.size() == vec_fields_ids.size()) {
    for (size_t i = 0; i < index_names.size(); ++i) {
      struct Field field;
      field.name = index_names[i];
      field.datatype = DataType::VECTOR;
      field.value = vec[i];
      doc.AddField(field);
    }
  }
  return ret;
}

int GammaEngine::BuildIndex() {
  int running = __sync_fetch_and_add(&b_running_, 1);
  if (running) {
    if (vec_manager_->Indexing() != 0) {
      LOG(ERROR) << "Create index failed!";
      return -1;
    }
    return 0;
  }

  auto func_indexing = std::bind(&GammaEngine::Indexing, this);
  std::thread t(func_indexing);
  t.detach();
  return 0;
}

int GammaEngine::Indexing() {
  if (vec_manager_->Indexing() != 0) {
    LOG(ERROR) << "Create index failed!";
    b_running_ = 0;
    return -1;
  }

  LOG(INFO) << "vector manager indexing success!";
  int ret = 0;
  bool has_error = false;
  while (b_running_) {
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
    usleep(1000 * 1000);  // sleep 5000ms
  }
  running_cv_.notify_one();
  LOG(INFO) << "Build index exited!";
  return ret;
}

int GammaEngine::BuildFieldIndex() {
  b_field_running_ = true;

  std::map<std::string, enum DataType> attr_type_map;
  table_->GetAttrType(attr_type_map);
  int field_num = attr_type_map.size();

  while (b_field_running_) {
    if (b_loading_) {
      usleep(5000 * 1000);  // sleep 5000ms
      continue;
    }

#pragma omp parallel for
    for (int i = 0; i < field_num; ++i) {
    }

    usleep(5000 * 1000);  // sleep 5000ms
  }
  running_field_cv_.notify_one();
  LOG(INFO) << "Build field index exited!";
  return 0;
}

int GammaEngine::GetDocsNum() { return max_docid_ - delete_num_; }

void GammaEngine::GetIndexStatus(EngineStatus &engine_status) {
  engine_status.SetIndexStatus(index_status_);

  long table_mem_bytes = table_->GetMemoryBytes();
  long vec_mem_bytes = 0, index_mem_bytes = 0;
  vec_manager_->GetTotalMemBytes(index_mem_bytes, vec_mem_bytes);

  long total_mem_b = 0;
  long dense_b = 0, sparse_b = 0;
  if (field_range_index_) {
    total_mem_b += field_range_index_->MemorySize(dense_b, sparse_b);
  }

  // long total_mem_kb = total_mem_b / 1024;
  // long total_mem_mb = total_mem_kb / 1024;
  // LOG(INFO) << "Field range memory [" << total_mem_kb << "]kb, ["
  //           << total_mem_mb << "]MB, dense [" << dense_b / 1024 / 1024
  //           << "]MB sparse [" << sparse_b / 1024 / 1024
  //           << "]MB";

  engine_status.SetTableMem(table_mem_bytes);
  engine_status.SetIndexMem(index_mem_bytes);
  engine_status.SetVectorMem(vec_mem_bytes);
  engine_status.SetFieldRangeMem(total_mem_b);
  engine_status.SetBitmapMem(docids_bitmap_->BytesSize());
  engine_status.SetDocNum(GetDocsNum());
  engine_status.SetMaxDocID(max_docid_ - 1);
  engine_status.SetMinIndexedNum(vec_manager_->MinIndexedNum());
}

int GammaEngine::Dump() {
  int ret = table_->Sync();
  if (ret != 0) {
    LOG(ERROR) << "dump table error, ret=" << ret;
    return -1;
  }

  if (is_dirty_) {
    int max_docid = max_docid_ - 1;
    std::time_t t = std::time(nullptr);
    char tm_str[100];
    std::strftime(tm_str, sizeof(tm_str), date_time_format_.c_str(),
                  std::localtime(&t));

    string path = dump_path_ + "/" + tm_str;
    if (!utils::isFolderExist(path.c_str())) {
      mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }

    ret = vec_manager_->Dump(path, 0, max_docid);
    if (ret != 0) {
      LOG(ERROR) << "dump vector error, ret=" << ret;
      return -1;
    }

    const string dump_done_file = path + "/dump.done";
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
    LOG(INFO) << "Dumped to [" << path
              << "], last dump directory(removed)=" << last_dump_dir_;
    last_dump_dir_ = path;
    is_dirty_ = false;
  }
  return 0;
}

int GammaEngine::CreateTableFromLocal(std::string &table_name) {
  std::vector<string> file_paths = utils::ls(index_root_path_);
  for (string &file_path : file_paths) {
    std::string::size_type pos = file_path.rfind(".schema");
    if (pos == file_path.size() - 7) {
      std::string::size_type begin = file_path.rfind('/');
      assert(begin != std::string::npos);
      begin += 1;
      table_name = file_path.substr(begin, pos - begin);
      LOG(INFO) << "local table name=" << table_name;
      TableSchemaIO tio(file_path);
      TableInfo table;
      if (tio.Read(table_name, table)) {
        LOG(ERROR) << "read table schema error, path=" << file_path;
        return -1;
      }

      if (CreateTable(table)) {
        LOG(ERROR) << "create table error when loading";
        return -1;
      }
      return 0;
    }
  }
  return -1;
}

int GammaEngine::Load() {
  b_loading_ = true;
  if (!created_table_) {
    string table_name;
    if (CreateTableFromLocal(table_name)) {
      LOG(ERROR) << "create table from local error";
      return -1;
    }
    LOG(INFO) << "create table from local success, table name=" << table_name;
  }

  std::vector<std::pair<std::time_t, string>> folders_tm;
  std::vector<string> folders = utils::ls_folder(dump_path_);
  std::vector<string> folders_not_done;
  for (const string &folder_name : folders) {
    if (folder_name == "") continue;
    string folder_path = dump_path_ + "/" + folder_name;
    string done_file = folder_path + "/dump.done";
    if (!utils::file_exist(done_file)) {
      LOG(INFO) << "done file is not existed, skip it! path=" << done_file;
      folders_not_done.push_back(folder_path);
      continue;
    }
    struct tm result;
    strptime(folder_name.c_str(), date_time_format_.c_str(), &result);
    std::time_t t = std::mktime(&result);
    folders_tm.push_back(std::make_pair(t, folder_path));
  }
  std::sort(folders_tm.begin(), folders_tm.end(),
            [](const std::pair<std::time_t, string> &a,
               const std::pair<std::time_t, string> &b) {
              return a.first < b.first;
            });
  if (folders_tm.size() > 0) {
    string dump_done_file =
        folders_tm[folders_tm.size() - 1].second + "/dump.done";
    utils::FileIO fio(dump_done_file);
    if (fio.Open("r")) {
      LOG(ERROR) << "Cannot read from file " << dump_done_file;
      return -1;
    }
    long fsize = utils::get_file_size(dump_done_file);
    char *buf = new char[fsize];
    fio.Read(buf, 1, fsize);
    string buf_str(buf, fsize);
    std::vector<string> lines = utils::split(buf_str, "\n");
    assert(lines.size() == 2);
    std::vector<string> items = utils::split(lines[1], " ");
    assert(items.size() == 2);
    int index_dump_num = (int)std::strtol(items[1].c_str(), nullptr, 10) + 1;
    LOG(INFO) << "read index_dump_num=" << index_dump_num << " from "
              << dump_done_file;
    delete[] buf;
  }

  max_docid_ = table_->GetStorageManagerSize();

  string last_dir = "";
  std::vector<string> dirs;
  if (folders_tm.size() > 0) {
    last_dir = folders_tm[folders_tm.size() - 1].second;
    LOG(INFO) << "Loading from " << last_dir;
    dirs.push_back(last_dir);
  }
  int ret = vec_manager_->Load(dirs, max_docid_);
  if (ret != 0) {
    LOG(ERROR) << "load vector error, ret=" << ret << ", path=" << last_dir;
    return ret;
  }

  ret = table_->Load(max_docid_);
  if (ret != 0) {
    LOG(ERROR) << "load profile error, ret=" << ret;
    return ret;
  }

  int field_num = table_->FieldsNum();
  for (int i = 0; i < max_docid_; ++i) {
    for (int j = 0; j < field_num; ++j) {
      field_range_index_->Add(i, j);
    }
  }

  delete_num_ = 0;
  for (int i = 0; i < max_docid_; ++i) {
    if (docids_bitmap_->Test(i)) {
      ++delete_num_;
    }
  }

  if (not b_running_ and index_status_ == UNINDEXED) {
    if (max_docid_ >= indexing_size_) {
      LOG(INFO) << "Begin indexing. indexing_size=" << indexing_size_;
      this->BuildIndex();
    }
  }
  // remove directorys which are not done
  for (const string &folder : folders_not_done) {
    if (utils::remove_dir(folder.c_str())) {
      LOG(ERROR) << "clean error, not done directory=" << folder;
    }
  }

  last_dump_dir_ = last_dir;
  LOG(INFO) << "load engine success! max docid=" << max_docid_
            << ", load directory=" << last_dir
            << ", clean directorys(not done)="
            << utils::join(folders_not_done, ',');
  b_loading_ = false;
  return 0;
}

int GammaEngine::LoadFromFaiss() {
  std::map<std::string, RetrievalModel *> &vec_indexes =
      vec_manager_->VectorIndexes();
  if (vec_indexes.size() != 1) {
    LOG(ERROR) << "Load from faiss index should be only one!";
    return -1;
  }

  RetrievalModel *index = vec_indexes.begin()->second;
  if (index == nullptr) {
    LOG(ERROR) << "Cannot find faiss index";
    return -1;
  }
  index_status_ = INDEXED;

  int load_num = index->Load("files");
  if (load_num < 0) {
    LOG(ERROR) << "vector [faiss] load gamma index failed!";
    return -1;
  }

  int d = index->vector_->MetaInfo()->Dimension();
  int fd = open("files/feature", O_RDONLY);
  size_t mmap_size = load_num * sizeof(float) * d;
  float *feature = (float *)mmap(NULL, mmap_size, PROT_READ, MAP_PRIVATE, fd, 0);
  for (int i = 0; i < load_num; ++i) {
    Doc doc;
    Field field;
    field.name = "_id";
    field.datatype = DataType::STRING;
    field.value = std::to_string(i);
    doc.SetKey(field.value);
    doc.AddField(std::move(field));
    field.name = "faiss";
    field.datatype = DataType::VECTOR;
    field.value = std::string((char *)(feature + (uint64_t)i * d), d * sizeof(float));
    doc.AddField(std::move(field));
    AddOrUpdate(doc);
  }
  munmap(feature, mmap_size);
  close(fd);
  return 0;
}

int GammaEngine::AddNumIndexFields() {
  int retvals = 0;
  std::map<std::string, enum DataType> attr_type;
  retvals = table_->GetAttrType(attr_type);

  std::map<std::string, bool> attr_index;
  retvals = table_->GetAttrIsIndex(attr_index);
  for (const auto &it : attr_type) {
    string field_name = it.first;
    const auto &attr_index_it = attr_index.find(field_name);
    if (attr_index_it == attr_index.end()) {
      LOG(ERROR) << "Cannot find field [" << field_name << "]";
      continue;
    }
    bool is_index = attr_index_it->second;
    if (not is_index) {
      continue;
    }
    int field_idx = table_->GetAttrIdx(field_name);
    LOG(INFO) << "Add range field [" << field_name << "]";
    field_range_index_->AddField(field_idx, it.second);
  }
  return retvals;
}

int GammaEngine::GetConfig(Config &conf) {
  conf.ClearCacheInfos();
  vec_manager_->GetAllCacheSize(conf);
  int table_cache_size = 0;
  int str_cache_size = 0;
  table_->GetCacheSize(table_cache_size, str_cache_size);
  conf.AddCacheInfo("table", table_cache_size);
  conf.AddCacheInfo("string", str_cache_size);
  return 0;
}

int GammaEngine::SetConfig(Config &conf) {
  int table_cache_size = 0;
  int str_cache_size = 0;
  for (auto &c : conf.CacheInfos()) {
    if (c.field_name == "table") {
      table_cache_size = c.cache_size;
    } else if (c.field_name == "string") {
      str_cache_size = c.cache_size;
    } else {
      vec_manager_->AlterCacheSize(c);
    }
  }
  table_->AlterCacheSize(table_cache_size, str_cache_size);
  GetConfig(conf);
  return 0;
}

int GammaEngine::BeginMigrate() {
  if (migrate_data_) {
    TerminateMigrate();
    delete migrate_data_;
    migrate_data_ = nullptr;
  }
  migrate_data_ = new MigrateData();
  migrate_data_->Init(docids_bitmap_, index_root_path_);
  migrate_data_->BeginMigrate(max_docid_);
  return 0;
}

int GammaEngine::GetMigrageDoc(Doc &doc, int *is_delete) {
  if (migrate_data_ == nullptr) return -1;
  int docid = -1, ret = -1;
  bool is_del;
  if (migrate_data_->GetMigrateDocid(docid, is_del)) {
    if (docid < 0 || docid >= max_docid_) {
      LOG(ERROR) << "MigrateDocid[" << docid << "] is error.";
      return -1;
    }
    if (is_del) {
      std::vector<string> table_fields;
      table_fields.push_back(table_->GetKeyFieldName());
      ret = table_->GetDocInfo(docid, doc, table_fields);
      *is_delete = 1;
    } else {
      ret = GetDoc(docid, doc);
      *is_delete = 0;
    }
  }
  return ret;
}

int GammaEngine::TerminateMigrate() {
  if (migrate_data_) {
    migrate_data_->TerminateMigrate(index_root_path_);
    delete migrate_data_;
    migrate_data_ = nullptr;
  }
  return 0;
}

}  // namespace tig_gamma

