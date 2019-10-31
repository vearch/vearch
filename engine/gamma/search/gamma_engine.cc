/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_engine.h"

#include "log.h"
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <locale.h>
#include <mutex>
#include <sys/mman.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

#include "bitmap.h"
#include "cJSON.h"
#include "gamma_common_data.h"
#include "utils.h"

using std::string;

namespace tig_gamma {

#ifdef DEBUG
static string float_array_to_string(float *data, int len) {
  if (data == nullptr)
    return "";
  std::stringstream ss;
  ss << "[";
  for (int i = 0; i < len; i++) {
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

static string RequestToString(const Request *request) {
  std::stringstream ss;
  ss << "{req_num:" << request->req_num << " topn:" << request->topn
     << " has_rank:" << request->has_rank
     << " vec_num:" << request->vec_fields_num;
  for (int i = 0; i < request->vec_fields_num; i++) {
    ss << " vec_id:" << i << " [" << VectorQueryToString(request->vec_fields[i])
       << "]";
  }
  ss << "}";
  return ss.str();
}
#endif // DEBUG

GammaEngine::GammaEngine(const string &index_root_path)
    : index_root_path_(index_root_path),
      date_time_format_("%Y-%m-%d-%H:%M:%S") {
  docids_bitmap_ = nullptr;
  profile_ = nullptr;
  vec_manager_ = nullptr;
  index_status_ = IndexStatus::UNINDEXED;
  delete_num_ = 0;
  b_running_ = false;
  dump_docid_ = 0;
  bitmap_bytes_size_ = 0;
  field_range_index_ = nullptr;
#ifdef PERFORMANCE_TESTING
  search_num_ = 0;
#endif
}

GammaEngine::~GammaEngine() {
  if (b_running_) {
    b_running_ = false;
    std::mutex running_mutex;
    std::unique_lock<std::mutex> lk(running_mutex);
    running_cv_.wait(lk);
  }

  if (vec_manager_) {
    delete vec_manager_;
    vec_manager_ = nullptr;
  }
  if (profile_) {
    delete profile_;
    profile_ = nullptr;
  }
  if (docids_bitmap_) {
    delete docids_bitmap_;
    docids_bitmap_ = nullptr;
  }
  if (field_range_index_) {
    delete field_range_index_;
    field_range_index_ = nullptr;
  }
}

GammaEngine *GammaEngine::GetInstance(const string &index_root_path,
                                      int max_doc_size) {
  GammaEngine *engine = new GammaEngine(index_root_path);
  int ret = engine->Setup(max_doc_size);
  if (ret < 0) {
    LOG(ERROR) << "BuildSearchEngine [" << index_root_path << "] error!";
    return nullptr;
  }
  return engine;
}

int GammaEngine::Setup(int max_doc_size) {
  if (max_doc_size < 1) {
    return -1;
  }
  max_doc_size_ = max_doc_size;

  if (!utils::isFolderExist(index_root_path_.c_str())) {
    mkdir(index_root_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  dump_path_ = index_root_path_ + "/dump";

  if (!docids_bitmap_) {
    if (bitmap::create(docids_bitmap_, bitmap_bytes_size_, max_doc_size) != 0) {
      LOG(ERROR) << "Cannot create bitmap!";
      return -1;
    }
  }

  if (!profile_) {
    profile_ = new Profile(max_doc_size);
    if (!profile_) {
      LOG(ERROR) << "Cannot create profile!";
      return -2;
    }
  }

  if (!vec_manager_) {
    vec_manager_ = new VectorManager(IVFPQ, Mmap, docids_bitmap_, max_doc_size,
                                     index_root_path_);
    if (!vec_manager_) {
      LOG(ERROR) << "Cannot create vec_manager!";
      return -3;
    }
  }

  max_docid_ = 0;
  LOG(INFO) << "GammaEngine setup successed!";
  return 0;
}

Response *GammaEngine::Search(const Request *request) {
#ifdef PERFORMANCE_TESTING
  double start = utils::getmillisecs();
  std::stringstream ss;
#endif

#ifdef DEBUG
  LOG(INFO) << "search request:" << RequestToString(request);
#endif

  int ret = 0;
  Response *response_results =
      static_cast<Response *>(malloc(sizeof(Response)));
  memset(response_results, 0, sizeof(Response));
  response_results->req_num = request->req_num;

  response_results->results = static_cast<SearchResult **>(
      malloc(response_results->req_num * sizeof(SearchResult *)));
  for (int i = 0; i < response_results->req_num; i++) {
    SearchResult *result =
        static_cast<SearchResult *>(malloc(sizeof(SearchResult)));
    result->total = 0;
    result->result_num = 0;
    result->result_items = nullptr;
    result->msg = nullptr;
    response_results->results[i] = result;
  }

  response_results->online_log_message = nullptr;

  if (request->req_num <= 0) {
    string msg = "req_num should not less than 0";
    for (int i = 0; i < response_results->req_num; i++) {
      response_results->results[i]->msg =
          MakeByteArray(msg.c_str(), msg.length());
      response_results->results[i]->result_code =
          SearchResultCode::SEARCH_ERROR;
    }
    return response_results;
  }

  std::string online_log_level;
  if (request->online_log_level) {
    online_log_level.assign(request->online_log_level->value,
                            request->online_log_level->len);
  }

  utils::OnlineLogger logger;
  if (0 != logger.Init(online_log_level)) {
    LOG(WARNING) << "init online logger error!";
  }

  OLOG(&logger, INFO, "online log level: " << online_log_level);

  bool use_direct_search = ((request->direct_search_type == 1) ||
                            ((request->direct_search_type == 0) &&
                             (index_status_ != IndexStatus::INDEXED)));

  if ((not use_direct_search) && (index_status_ != IndexStatus::INDEXED)) {
    string msg = "index not trained!";
    for (int i = 0; i < response_results->req_num; i++) {
      response_results->results[i]->msg =
          MakeByteArray(msg.c_str(), msg.length());
      response_results->results[i]->result_code =
          SearchResultCode::INDEX_NOT_TRAINED;
    }
    return response_results;
  }

  GammaQuery gamma_query;
  gamma_query.logger = &logger;
  gamma_query.vec_query = request->vec_fields;
  gamma_query.vec_num = request->vec_fields_num;
  GammaSearchCondition condition;
  condition.topn = request->topn;
  condition.parallel_mode = 1; // default to parallelize over inverted list
  condition.recall_num = request->topn; // TODO: recall number should be
                                        // transmitted from search request
  condition.has_rank = request->has_rank == 1 ? true : false;
  condition.use_direct_search = use_direct_search;

  MultiRangeQueryResults range_query_result;
  if (request->range_filters_num > 0 || request->term_filters_num > 0) {
    std::vector<FilterInfo> filters;
    filters.resize(request->range_filters_num + request->term_filters_num);
    int idx = 0;

    for (int i = 0; i < request->range_filters_num; i++) {
      auto c = request->range_filters[i];

      filters[idx].field = string(c->field->value, c->field->len);
      filters[idx].lower_value =
          string(c->lower_value->value, c->lower_value->len);
      filters[idx].upper_value =
          string(c->upper_value->value, c->upper_value->len);

      idx++;
    }

    for (int i = 0; i < request->term_filters_num; i++) {
      auto c = request->term_filters[i];

      filters[idx].field = string(c->field->value, c->field->len);
      filters[idx].lower_value = string(c->value->value, c->value->len);
      filters[idx].is_union = c->is_union;

      idx++;
    }

    double start_time = utils::getmillisecs();
    int retval = field_range_index_->Search(filters, range_query_result);
    double end_time = utils::getmillisecs();
#ifdef PERFORMANCE_TESTING
    LOG(INFO) << "num time " << end_time - start_time << " retval " << retval;
#endif
    OLOG(&logger, DEBUG, "search numeric index, ret: " << retval);

    if (retval == 0) {
      string msg = "No result: numeric filter return 0 result";
      for (int i = 0; i < response_results->req_num; i++) {
        response_results->results[i]->msg =
            MakeByteArray(msg.c_str(), msg.length());
        response_results->results[i]->result_code = SearchResultCode::SUCCESS;
      }

      const char *log_message = logger.Data();
      if (log_message) {
        response_results->online_log_message =
            MakeByteArray(log_message, logger.Length());
      }
      return response_results;
    }

    if (retval < 0) {
      condition.range_query_result = nullptr;
    } else {
      condition.range_query_result = &range_query_result;
    }
  }
#ifdef PERFORMANCE_TESTING
  double numeric_filter_time = utils::getmillisecs();
  ss << "numeric filter cost [" << numeric_filter_time - start << "]ms, ";
#endif

  // condition.min_dist = request->vec_fields[0]->min_score;
  // condition.max_dist = request->vec_fields[0]->max_score;

  gamma_query.condition = &condition;
  if (request->vec_fields_num > 0) {
    GammaResult gamma_results[request->req_num];
    for (int i = 0; i < request->req_num; ++i) {
      gamma_results[i].total = this->GetDocsNum();
    }
    ret = vec_manager_->Search(gamma_query, gamma_results);
    if (ret != 0) {
      string msg = "search error [" + std::to_string(ret) + "]";
      for (int i = 0; i < response_results->req_num; i++) {
        response_results->results[i]->msg =
            MakeByteArray(msg.c_str(), msg.length());
        response_results->results[i]->result_code =
            SearchResultCode::SEARCH_ERROR;
      }

      const char *log_message = logger.Data();
      if (log_message) {
        response_results->online_log_message =
            MakeByteArray(log_message, logger.Length());
      }

      return response_results;
    }

    PackResults(gamma_results, response_results);
  } else {
    GammaResult gamma_result;
    gamma_result.topn = request->topn;
    gamma_result.init(request->topn, nullptr, 0);
    for (int docid = 0; docid < max_docid_; docid++) {
      if (range_query_result.Has(docid) &&
          !bitmap::test(docids_bitmap_, docid)) {
        ++gamma_result.total;
        if (gamma_result.results_count < request->topn) {
          gamma_result.docs[gamma_result.results_count++].docid = docid;
        }
      }
    }
    response_results->req_num = 1; // only one result
    PackResults(&gamma_result, response_results);
  }

#ifdef PERFORMANCE_TESTING
  double search_time = utils::getmillisecs();
  if (++search_num_ % 1000 == 0) {
    ss << "search cost [" << search_time - numeric_filter_time
       << "]ms, total cost [" << search_time - start << "]ms";
    LOG(INFO) << ss.str();
  }
#endif

  const char *log_message = logger.Data();
  if (log_message) {
    response_results->online_log_message =
        MakeByteArray(log_message, logger.Length());
  }

  return response_results;
}

int GammaEngine::CreateTable(const Table *table) {
  if (!vec_manager_ || !profile_) {
    LOG(ERROR) << "vector and profile should not be null!";
    return -1;
  }
  int ret_vec = vec_manager_->CreateVectorTable(
      table->vectors_info, table->vectors_num, table->ivfpq_param);
  int ret_profile = profile_->CreateTable(table);

  if (ret_vec != 0 || ret_profile != 0) {
    LOG(ERROR) << "Cannot create table!";
    return -2;
  }

  field_range_index_ = new MultiFieldsRangeIndex();
  if ((nullptr == field_range_index_) || (AddNumIndexFields() < 0)) {
    LOG(ERROR) << "add numeric index fields error!";
    return -3;
  }

  LOG(INFO) << "create table="
            << std::string(table->name->value, table->name->len) << "success!";

  return 0;
}

int GammaEngine::Add(const Doc *doc) {
  if (max_docid_ >= max_doc_size_) {
    LOG(ERROR) << "Doc size reached upper size [" << max_docid_ << "]";
    return -1;
  }
  std::vector<Field *> fields_profile;
  std::vector<Field *> fields_vec;
  for (int i = 0; i < doc->fields_num; i++) {
    if (doc->fields[i]->data_type != VECTOR) {
      fields_profile.push_back(doc->fields[i]);
    } else {
      fields_vec.push_back(doc->fields[i]);
    }
  }
  // add fields into profile
  if (profile_->Add(fields_profile, max_docid_, false) != 0) {
    return -1;
  }

  for (int i = 0; i < doc->fields_num; ++i) {
    auto *f = doc->fields[i];
    field_range_index_->Add(string(f->name->value, f->name->len),
                            reinterpret_cast<unsigned char *>(f->value->value),
                            f->value->len, max_docid_);
  }

  // add vectors by VectorManager
  if (vec_manager_->AddToStore(max_docid_, fields_vec) != 0) {
    return -2;
  }
  max_docid_++;

  return 0;
}

int GammaEngine::AddOrUpdate(const Doc *doc) {
  if (max_docid_ >= max_doc_size_) {
    LOG(ERROR) << "Doc size reached upper size [" << max_docid_ << "]";
    return -1;
  }
  std::vector<Field *> fields_profile;
  std::vector<Field *> fields_vec;
  string key;
  for (int i = 0; i < doc->fields_num; i++) {
    if (doc->fields[i]->data_type != VECTOR) {
      fields_profile.push_back(doc->fields[i]);
      const string &name =
          string(doc->fields[i]->name->value, doc->fields[i]->name->len);
      if (name == "_id") {
        key = string(doc->fields[i]->value->value, doc->fields[i]->value->len);
      }
    } else {
      fields_vec.push_back(doc->fields[i]);
    }
  }
  // add fields into profile
  int docid = -1;
  profile_->GetDocIDByKey(key, docid);
  if (docid == -1) {
    int ret = profile_->Add(fields_profile, max_docid_, false);
    if (ret != 0)
      return -1;
  } else {
    Del(key);
    int ret = profile_->Add(fields_profile, max_docid_, true);
    if (ret != 0)
      return -1;
  }

  for (int i = 0; i < doc->fields_num; ++i) {
    auto *f = doc->fields[i];
    field_range_index_->Add(string(f->name->value, f->name->len),
                            reinterpret_cast<unsigned char *>(f->value->value),
                            f->value->len, max_docid_);
  }

  // add vectors by VectorManager
  if (vec_manager_->AddToStore(max_docid_, fields_vec) != 0) {
    return -2;
  }
  max_docid_++;

  return 0;
}

int GammaEngine::Update(const Doc *doc) {
  string key;
  std::vector<Field *> fields_profile;
  std::vector<Field *> fields_vec;
  for (int i = 0; i < doc->fields_num; i++) {
    if (doc->fields[i]->data_type != VECTOR) {
      if (strncmp(doc->fields[i]->name->value, "_id", 3) == 0) {
        key = string(doc->fields[i]->value->value, doc->fields[i]->value->len);
      }
      fields_profile.push_back(doc->fields[i]);
    } else {
      fields_vec.push_back(doc->fields[i]);
    }
  }

  int doc_id = -1;
  if (profile_->GetDocIDByKey(key, doc_id) < 0) {
    return -1;
  }

  if (Del(key) != 0) {
    return -1;
  }

  if (profile_->Add(fields_profile, max_docid_, true) != 0) {
    return -1;
  }

  for (int i = 0; i < doc->fields_num; ++i) {
    auto *f = doc->fields[i];
    field_range_index_->Add(string(f->name->value, f->name->len),
                            reinterpret_cast<unsigned char *>(f->value->value),
                            f->value->len, max_docid_);
  }

  // add vectors by VectorManager
  if (vec_manager_->AddToStore(max_docid_, fields_vec) != 0) {
    return -2;
  }
  max_docid_++;
  return 0;
}

int GammaEngine::Del(const std::string &key) {
  int docid = -1, ret = 0;
  ret = profile_->GetDocIDByKey(key, docid);
  if (ret != 0 || docid < 0)
    return -1;

  if (bitmap::test(docids_bitmap_, docid)) {
    return ret;
  }
  ++delete_num_;
  bitmap::set(docids_bitmap_, docid);

  return ret;
}

int GammaEngine::DelDocByQuery(Request *request) {
#ifdef DEBUG
  LOG(INFO) << "delete by query request:" << RequestToString(request);
#endif

  if (request->range_filters_num <= 0) {
    LOG(ERROR) << "no range filter";
    return 1;
  }
  MultiRangeQueryResults range_query_result; // Note its scope

  std::vector<FilterInfo> filters;
  filters.resize(request->range_filters_num);
  int idx = 0;

  for (int i = 0; i < request->range_filters_num; i++) {
    auto c = request->range_filters[i];

    filters[idx].field = string(c->field->value, c->field->len);
    filters[idx].lower_value =
        string(c->lower_value->value, c->lower_value->len);
    filters[idx].upper_value =
        string(c->upper_value->value, c->upper_value->len);

    idx++;
  }

  int retval = field_range_index_->Search(filters, range_query_result);
  if (retval == 0) {
    LOG(ERROR) << "numeric index search error, ret=" << retval;
    return 1;
  }

  std::vector<int> doc_ids = range_query_result.ToDocs();
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    int docid = doc_ids[i];
    if (bitmap::test(docids_bitmap_, docid)) {
      continue;
    }
    ++delete_num_;
    bitmap::set(docids_bitmap_, docid);
  }
  return 0;
}

Doc *GammaEngine::GetDoc(const std::string &id) {
  int docid = -1, ret = 0;
  ret = profile_->GetDocIDByKey(id, docid);
  if (ret != 0 || docid < 0) {
    LOG(INFO) << "GetDocIDbyKey [" << id << "] error!";
    return nullptr;
  }

  if (bitmap::test(docids_bitmap_, docid)) {
    LOG(INFO) << "bitmap set error!";
    return nullptr;
  }
  return profile_->Get(id);
}

int GammaEngine::BuildIndex() {
  if (vec_manager_->Indexing() != 0) {
    LOG(ERROR) << "Create index failed!";
    return -1;
  }
  LOG(INFO) << "vector manager indexing success!";

  b_running_ = true;
  int ret = 0;
  while (b_running_) {
    if (vec_manager_->AddRTVecsToIndex() != 0) {
      LOG(ERROR) << "Add real time vectors to index error!";
      ret = -3;
      break;
    }
    index_status_ = IndexStatus::INDEXED;
    usleep(5000 * 1000); // sleep 5000ms
  }
  running_cv_.notify_one();
  return ret;
}

int GammaEngine::GetDocsNum() { return max_docid_ - delete_num_; }

long GammaEngine::GetMemoryBytes() {
  long profile_mem_bytes = profile_->GetMemoryBytes();
  long vec_mem_bytes = vec_manager_->GetTotalMemBytes();

  long total_mem_bytes = profile_mem_bytes + vec_mem_bytes + bitmap_bytes_size_;
  LOG(INFO) << "total_mem_bytes: " << total_mem_bytes
            << ", profile_mem_bytes: " << profile_mem_bytes
            << ", vec_mem_bytes: " << vec_mem_bytes
            << ", bitmap_bytes_size: " << bitmap_bytes_size_;
  return total_mem_bytes;
}

int GammaEngine::GetIndexStatus() { return index_status_; }

int GammaEngine::Dump() {
  int max_docid = max_docid_ - 1;
  if (max_docid <= dump_docid_) {
    LOG(INFO) << "No fresh doc, cannot dump.";
    return 0;
  }

  if (!utils::isFolderExist(dump_path_.c_str())) {
    mkdir(dump_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  std::time_t t = std::time(nullptr);
  char tm_str[100];
  std::strftime(tm_str, sizeof(tm_str), date_time_format_.c_str(),
                std::localtime(&t));

  string path = dump_path_ + "/" + tm_str;
  if (!utils::isFolderExist(path.c_str())) {
    mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  const string dumping_file_name = path + "/dumping";
  std::ofstream f_dumping;
  f_dumping.open(dumping_file_name);
  if (!f_dumping.is_open()) {
    LOG(ERROR) << "Cannot create file " << dumping_file_name;
    return -1;
  }
  f_dumping << "start_docid " << dump_docid_ << std::endl;
  f_dumping << "end_docid " << max_docid << std::endl;
  f_dumping.close();

  int ret = profile_->Dump(path, max_docid, dump_docid_);
  if (ret != 0) {
    LOG(ERROR) << "dump profile error, ret=" << ret;
    return -1;
  }
  ret = vec_manager_->Dump(path, dump_docid_, max_docid);
  if (ret != 0) {
    LOG(ERROR) << "dump vector error, ret=" << ret;
    return -1;
  }

  const string bp_name = path + "/" + "bitmap";
  FILE *fp_output = fopen(bp_name.c_str(), "wb");
  if (fp_output == nullptr) {
    LOG(ERROR) << "Cannot write file " << bp_name;
    return -1;
  }

  fwrite((void *)(docids_bitmap_), sizeof(char), bitmap_bytes_size_, fp_output);
  fclose(fp_output);
  dump_docid_ = max_docid + 1;

  const string dump_done_file_name = path + "/dump.done";
  std::stringstream ss;
  ss << "mv " << dumping_file_name << " " << dump_done_file_name;
  system(ss.str().c_str());

  LOG(INFO) << "Dumped to [" << path << "], next dump docid [" << dump_docid_
            << "]";
  return ret;
}

int GammaEngine::Load() {
  std::map<std::time_t, string> folders_map;
  std::vector<std::time_t> folders_tm;
  std::vector<string> folders = utils::ls_folder(dump_path_);
  for (const string &folder_name : folders) {
    struct tm result;
    strptime(folder_name.c_str(), date_time_format_.c_str(), &result);

    std::time_t t = std::mktime(&result);
    folders_tm.push_back(t);
    folders_map.insert(std::make_pair(t, folder_name));
  }

  std::sort(folders_tm.begin(), folders_tm.end());
  folders.clear();
  for (const std::time_t t : folders_tm) {
    const string folder_path = dump_path_ + "/" + folders_map[t];
    const string done_file = folder_path + "/dump.done";
    if (utils::get_file_size(done_file.c_str()) < 0) {
      LOG(ERROR) << "dump.done cannot be found in [" << folder_path << "]";
      break;
    }
    folders.push_back(dump_path_ + "/" + folders_map[t]);
  }

  int ret = profile_->Load(folders, max_docid_);
  if (ret != 0) {
    LOG(ERROR) << "load profile error, ret=" << ret;
    return -1;
  }
  ret = vec_manager_->Load(folders);
  if (ret != 0) {
    LOG(ERROR) << "load vector error, ret=" << ret;
    return -1;
  }

  field_range_index_ = new MultiFieldsRangeIndex();
  if ((nullptr == field_range_index_) || (AddNumIndexFields()) < 0) {
    LOG(ERROR) << "add numeric index fields error!";
    return -1;
  }

  for (int i = 0; i < max_docid_; ++i) {
    Doc *doc = profile_->Get(i);
    for (int j = 0; j < doc->fields_num; ++j) {
      auto *f = doc->fields[j];
      field_range_index_->Add(
          string(f->name->value, f->name->len),
          reinterpret_cast<unsigned char *>(f->value->value), f->value->len, i);
    }
    DestroyDoc(doc);
  }

  if (docids_bitmap_ == nullptr) {
    LOG(ERROR) << "docid bitmap is not initilized";
    return -1;
  }
  string bitmap_file_name = folders[folders.size() - 1] + "/bitmap";
  FILE *fp_bm = fopen(bitmap_file_name.c_str(), "rb");
  if (fp_bm == nullptr) {
    LOG(ERROR) << "Cannot open file " << bitmap_file_name;
    return -1;
  }
  long bm_file_size = utils::get_file_size(bitmap_file_name.c_str());
  if (bm_file_size > bitmap_bytes_size_) {
    LOG(ERROR) << "bitmap file size=" << bm_file_size
               << " > allocated bitmap bytes size=" << bitmap_bytes_size_
               << ", max doc size=" << max_doc_size_;
    fclose(fp_bm);
    return -1;
  }
  fread((void *)(docids_bitmap_), sizeof(char), bm_file_size, fp_bm);
  fclose(fp_bm);

  LOG(INFO) << "load all success! bitmap file=" << bitmap_file_name
            << ", folders=" << utils::join(folders, ',');

  dump_docid_ = max_docid_;

  return ret;
}

int GammaEngine::AddNumIndexFields() {
  int retvals = 0;
  std::map<std::string, enum DataType> attr_type;
  retvals = profile_->GetAttrType(attr_type);

  std::map<std::string, int> attr_index;
  retvals = profile_->GetAttrIsIndex(attr_index);
  for (const auto &it : attr_type) {
    string field_name = it.first;
    const auto &attr_index_it = attr_index.find(field_name);
    if (attr_index_it == attr_index.end()) {
      LOG(ERROR) << "Cannot find field [" << field_name << "]";
      continue;
    }
    int is_index = attr_index_it->second;
    if (is_index == 0) {
      continue;
    }
    field_range_index_->AddField(field_name, it.second);
  }
  return retvals;
}

// TODO: handle malloc error and NULL pointer errors
void GammaEngine::PackResults(const GammaResult *gamma_results,
                              Response *response_results) {
  for (int i = 0; i < response_results->req_num; i++) {
    SearchResult *result = response_results->results[i];
    result->total = gamma_results[i].total;
    result->result_num = gamma_results[i].results_count;
    result->result_items = new ResultItem *[gamma_results[i].results_count];
    for (int j = 0; j < gamma_results[i].results_count; j++) {
      ResultItem *result_item = new ResultItem;
      result->result_items[j] = result_item;
      VectorDoc *vec_doc = gamma_results[i].docs + j;
      result->result_items[j]->score = vec_doc->score;

      int docid = vec_doc->docid;
      result->result_items[j]->doc = profile_->Get(docid);

      /*
      Doc *doc_ptr = result->result_items[j]->doc;
      doc_ptr->fields[doc_ptr->fields_num - 1]->value =
          static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
      doc_ptr->fields[doc_ptr->fields_num - 1]->value->value =
          static_cast<char *>(
              malloc(gamma_results[i].docs[j].source_len * sizeof(char)));

      memcpy(doc_ptr->fields[doc_ptr->fields_num - 1]->value->value,
             gamma_results[i].docs[j].source,
             gamma_results[i].docs[j].source_len);

      doc_ptr->fields[doc_ptr->fields_num - 1]->value->len =
          gamma_results[i].docs[j].source_len;
      */

      cJSON *extra_json = cJSON_CreateObject();
      cJSON *vec_result_json = cJSON_CreateArray();
      cJSON_AddItemToObject(extra_json, EXTRA_VECTOR_RESULT.c_str(),
                            vec_result_json);
      for (int i = 0; i < vec_doc->fields_len; i++) {
        VectorDocField *vec_field = vec_doc->fields + i;
        cJSON *vec_field_json = cJSON_CreateObject();

        cJSON_AddStringToObject(vec_field_json, EXTRA_VECTOR_FIELD_NAME.c_str(),
                                vec_field->name.c_str());
        string source = string(vec_field->source, vec_field->source_len);
        cJSON_AddStringToObject(
            vec_field_json, EXTRA_VECTOR_FIELD_SOURCE.c_str(), source.c_str());
        cJSON_AddNumberToObject(
            vec_field_json, EXTRA_VECTOR_FIELD_SCORE.c_str(), vec_field->score);
        cJSON_AddItemToArray(vec_result_json, vec_field_json);
      }
      char *extra_data = cJSON_PrintUnformatted(extra_json);
      result_item->extra = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
      result_item->extra->len = std::strlen(extra_data);
      result_item->extra->value =
          static_cast<char *>(malloc(result_item->extra->len));
      memcpy(result_item->extra->value, extra_data, result_item->extra->len);
      free(extra_data);
      cJSON_Delete(extra_json);
    }
    string msg = "Success";
    result->msg = MakeByteArray(msg.c_str(), msg.length());
    result->result_code = SearchResultCode::SUCCESS;
  }

  return;
}
} // namespace tig_gamma
