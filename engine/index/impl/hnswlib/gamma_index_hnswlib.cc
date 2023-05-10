/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_index_hnswlib.h"

#include <immintrin.h>
#include <unistd.h>

#include <cstdlib>
#include <mutex>
#include <string>
#include <omp.h>

#include "search/error_code.h"
#include "vector/memory_raw_vector.h"
#include "util/utils.h"

namespace tig_gamma {

using idx_t = faiss::Index::idx_t;

struct HNSWLIBModelParams {
  int nlinks;                   // link number for hnsw graph
  int efConstruction;           // construction parameter for building hnsw graph
  int efSearch;                 // search parameter for hnsw graph
  int do_efSearch_check;        // check efsearch or not when searching
  DistanceComputeType metric_type;

  HNSWLIBModelParams() {
    nlinks = 32;
    efConstruction = 100;
    efSearch = 64;
    do_efSearch_check = 1;
    metric_type = DistanceComputeType::L2;
  }

  bool Validate() {
    if (nlinks < 0 || efConstruction < 0) return false;
    return true;
  }

  int Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      LOG(ERROR) << "parse HNSW retrieval parameters error: " << str;
      return -1;
    }

    int nlinks;
    int efConstruction;
    int efSearch;
    int do_efSearch_check;

    // for -1, set as default
    if (!jp.GetInt("nlinks", nlinks)) {
      if (nlinks < -1) {
        LOG(ERROR) << "invalid nlinks = " << nlinks;
        return -1;
      }
      if (nlinks > 0) this->nlinks = nlinks;
    } else {
      LOG(ERROR) << "cannot get nlinks for hnsw, set it when create space";
      return -1;
    }

    if (!jp.GetInt("efConstruction", efConstruction)) {
      if (efConstruction < -1) {
        LOG(ERROR) << "invalid efConstruction = " << efConstruction;
        return -1;
      }
      if (efConstruction > 0) this->efConstruction = efConstruction;
    } else {
      LOG(ERROR)
          << "cannot get efConstruction for hnsw, set it when create space";
      return -1;
    }

    if (!jp.GetInt("efSearch", efSearch)) {
      if (efSearch < -1) {
        LOG(ERROR) << "invalid efSearch = " << efSearch;
        return -1;
      }
      if (efSearch > 0) this->efSearch = efSearch;
    }

    if (!jp.GetInt("do_efSearch_check", do_efSearch_check)) {
      if (do_efSearch_check < -1) {
        LOG(ERROR) << "invalid do_efSearch_check = " << do_efSearch_check;
        return -1;
      }
      if (do_efSearch_check > 0) this->do_efSearch_check = 1;
      if (do_efSearch_check == 0) this->do_efSearch_check = 0;
    }

    std::string metric_type;

    if (!jp.GetString("metric_type", metric_type)) {
      if (strcasecmp("L2", metric_type.c_str()) &&
          strcasecmp("InnerProduct", metric_type.c_str())) {
        LOG(ERROR) << "invalid metric_type = " << metric_type;
        return -1;
      }
      if (!strcasecmp("L2", metric_type.c_str()))
        this->metric_type = DistanceComputeType::L2;
      else
        this->metric_type = DistanceComputeType::INNER_PRODUCT;
    } else {
      this->metric_type = DistanceComputeType::L2;
    }
    return 0;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "nlinks =" << nlinks << ", ";
    ss << "efConstruction =" << efConstruction << ", ";
    ss << "efSearch =" << efSearch << ", ";
    ss << "do_efSearch_check =" << do_efSearch_check << ", ";
    ss << "metric_type =" << (int)metric_type;
    return ss.str();
  }
};

REGISTER_MODEL(HNSW, GammaIndexHNSWLIB);

GammaIndexHNSWLIB::GammaIndexHNSWLIB()
    : GammaFLATIndex(), HierarchicalNSW(nullptr) {
  indexed_vec_count_ = 0;
  updated_num_ = 0;
  deleted_num_ = 0;
}

GammaIndexHNSWLIB::~GammaIndexHNSWLIB() {
  if (space_interface_ != nullptr) {
    delete space_interface_;
    space_interface_ = nullptr;
  }

  if (space_interface_ip_ != nullptr) {
    delete space_interface_ip_;
    space_interface_ip_ = nullptr;
  }
  int ret = pthread_rwlock_destroy(&shared_mutex_);
  if (0 != ret) {
    LOG(ERROR) << "destory read write lock error, ret=" << ret;
  }
}

int GammaIndexHNSWLIB::Init(const std::string &model_parameters, int indexing_size) {
  indexing_size_ = indexing_size;
  raw_vec_ = dynamic_cast<MemoryRawVector *>(vector_);
  if (raw_vec_ == nullptr) {
    LOG(ERROR) << "HNSW can only work in memory only mode";
    return -1;
  }
  if (raw_vec_->HaveZFPCompressor()) {
    LOG(ERROR) << "HNSW can't work with zfp compressor, shouldn't set compress when create table";
    return -1;
  }

  HNSWLIBModelParams hnsw_param;
  if (model_parameters != "" && hnsw_param.Parse(model_parameters.c_str())) {
    return -2;
  }
  LOG(INFO) << hnsw_param.ToString();

  d = vector_->MetaInfo()->Dimension();
  size_t ef_construction = hnsw_param.efConstruction;

  space_interface_ = new L2Space(d);
  space_interface_ip_ = new InnerProductSpace(d);

  max_elements_ = 1000000;
  std::vector<std::mutex>(max_elements_).swap(link_list_locks_);
  std::vector<std::mutex>(max_update_element_locks)
      .swap(link_list_update_locks_);
  element_levels_ = std::vector<int>(max_elements_);
  num_deleted_ = 0;
  if (hnsw_param.metric_type == DistanceComputeType::INNER_PRODUCT) {
    fstdistfunc_ = space_interface_ip_->get_dist_func();
    dist_func_param_ = space_interface_ip_->get_dist_func_param();
  } else {
    fstdistfunc_ = space_interface_->get_dist_func();
    dist_func_param_ = space_interface_->get_dist_func_param();
  }
  metric_type_ = hnsw_param.metric_type;
  vec_data_size_ = space_interface_->get_data_size();
  data_size_ = 0;
  int M = hnsw_param.nlinks;
  M_ = M;
  maxM_ = M_;
  maxM0_ = M_ * 2;
  ef_construction_ = std::max(ef_construction, M_);
  ef_ = hnsw_param.efSearch;
  do_efSearch_check_ = hnsw_param.do_efSearch_check;
  int random_seed = 100;
  level_generator_.seed(random_seed);
  update_probability_generator_.seed(random_seed + 1);

  size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
  size_data_per_element_ = size_links_level0_ + data_size_ + sizeof(labeltype);
  offsetData_ = size_links_level0_;
  label_offset_ = size_links_level0_ + data_size_;
  offsetLevel0_ = 0;

  data_level0_memory_ = (char *)malloc(max_elements_ * size_data_per_element_);
  if (data_level0_memory_ == nullptr)
    throw std::runtime_error("Not enough memory");

  cur_element_count = 0;

  visited_list_pool_ = new VisitedListPool(1, max_elements_);

  // initializations for special treatment of the first node
  enterpoint_node_ = -1;
  maxlevel_ = -1;

  linkLists_ = (char **)malloc(sizeof(void *) * max_elements_);
  if (linkLists_ == nullptr)
    throw std::runtime_error(
        "Not enough memory: HierarchicalNSW failed to allocate linklists");
  size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
  mult_ = 1 / log(1.0 * M_);
  revSize_ = 1.0 / mult_;

  int ret = pthread_rwlock_init(&shared_mutex_, NULL);
  if (ret != 0) {
    LOG(ERROR) << "init read-write lock error, ret=" << ret;
  }

  return 0;
}

RetrievalParameters *GammaIndexHNSWLIB::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new HNSWLIBRetrievalParameters(metric_type_, ef_, do_efSearch_check_);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  enum DistanceComputeType type = metric_type_;
  std::string metric_type;
  if (!jp.GetString("metric_type", metric_type)) {
    if (strcasecmp("L2", metric_type.c_str()) &&
        strcasecmp("InnerProduct", metric_type.c_str())) {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
    }
    if (!strcasecmp("L2", metric_type.c_str()))
      type = DistanceComputeType::L2;
    else
      type = DistanceComputeType::INNER_PRODUCT;
  }

  int efSearch = 0;
  jp.GetInt("efSearch", efSearch);

  int do_efSearch_check = 1;
  jp.GetInt("do_efSearch_check", do_efSearch_check);

  RetrievalParameters *retrieval_params =
      new HNSWLIBRetrievalParameters(type, efSearch > 0 ? efSearch : ef_, 
                                     do_efSearch_check > -1 ? do_efSearch_check : do_efSearch_check_);
  return retrieval_params;
}

int GammaIndexHNSWLIB::Indexing() { return 0; }

bool GammaIndexHNSWLIB::Add(int n, const uint8_t *vec) {
  int n0 = indexed_vec_count_;

  const float *x = reinterpret_cast<const float *>(vec);

  std::unique_lock<std::mutex> templock(dump_mutex_);
  AddVertices(n0, n, x);  
  indexed_vec_count_ += n;

  return true;
}

int GammaIndexHNSWLIB::AddVertices(size_t n0, size_t n, const float *x) {
#ifdef PERFORMANCE_TESTING
  double t0 = utils::getmillisecs();
#endif  // PERFORMANCE_TESTING
  if (n == 0) {
    return 0;
  }

  while(n0 + n >= max_elements_) {
    resizeIndex(max_elements_ * 2);
  }

#pragma omp parallel for
  for (size_t i = 0; i < n; ++i) {
    addPoint((const void *)(x + i * d), n0 + i);
  }
#ifdef PERFORMANCE_TESTING
  add_count_ += n;
  if (add_count_ >= 10000) {
    LOG(INFO) << "adding elements on top of " << n0 << ", average add time "
              << (utils::getmillisecs() - t0) / n << " ms";
    add_count_ = 0;
  }
#endif  // PERFORMANCE_TESTING
  return 0;
}

int GammaIndexHNSWLIB::Search(RetrievalContext *retrieval_context, int n,
                              const uint8_t *x, int k, float *distances,
                              int64_t *labels) {
  const float *xq = reinterpret_cast<const float *>(x);
  if (xq == nullptr) {
    LOG(ERROR) << "search feature is null";
    return -1;
  }

  idx_t *idxs = reinterpret_cast<idx_t *>(labels);
  if (idxs == nullptr) {
    LOG(ERROR) << "search result'ids is null";
    return -2;
  }

  HNSWLIBRetrievalParameters *retrieval_params =
      dynamic_cast<HNSWLIBRetrievalParameters *>(
          retrieval_context->RetrievalParams());
  if (retrieval_params == nullptr) {
    retrieval_params = new HNSWLIBRetrievalParameters(metric_type_, ef_, do_efSearch_check_);
    retrieval_context->retrieval_params_ = retrieval_params;
  }

  DISTFUNC<float> fstdistfunc;
  if (retrieval_params->GetDistanceComputeType() ==
      DistanceComputeType::INNER_PRODUCT) {
    fstdistfunc = space_interface_ip_->get_dist_func();
  } else {
    fstdistfunc = space_interface_->get_dist_func();
  }
  int threads_num = n < omp_get_max_threads() ? n : omp_get_max_threads();

#pragma omp parallel for schedule(dynamic) num_threads(threads_num)
  for (int i = 0; i < n; ++i) {
    int j = 0;

    auto result = searchKnn((const void *)(xq + i * d), k, fstdistfunc,
                            retrieval_params->EfSearch(), 
                            retrieval_params->DoEfSearchCheck(), retrieval_context);

    if (retrieval_params->GetDistanceComputeType() ==
        DistanceComputeType::INNER_PRODUCT) {
      while (!result.empty()) {
        auto &top = result.top();
        idxs[i * k + k - j - 1] = top.second;
        distances[i * k + k - j - 1] = 1 - top.first;
        ++j;
        result.pop();
      }
    } else {
      while (!result.empty()) {
        auto &top = result.top();
        idxs[i * k + k - j - 1] = top.second;
        distances[i * k + k - j - 1] = top.first;
        ++j;
        result.pop();
      }
    }
  }

#ifdef PERFORMANCE_TESTING
  std::string compute_msg = "hnsw compute ";
  compute_msg += std::to_string(n);
  retrieval_context->GetPerfTool().Perf(compute_msg);
#endif  // PERFORMANCE_TESTING

  return 0;
}

long GammaIndexHNSWLIB::GetTotalMemBytes() {
  size_t total_mem_bytes = max_elements_ * size_data_per_element_;
  return total_mem_bytes;
}

int GammaIndexHNSWLIB::Update(const std::vector<int64_t> &ids,
                              const std::vector<const uint8_t *> &vecs) {
  std::unique_lock<std::mutex> templock(dump_mutex_);
  for (size_t i = 0; i < ids.size(); i++) {
    updatePoint((const void *)vecs[i], ids[i], 1.0);
  }
  updated_num_ += ids.size();
  LOG(INFO) << "update index success! size=" << ids.size()
            << ", total=" << updated_num_;
  return 0;
}

int GammaIndexHNSWLIB::Delete(const std::vector<int64_t> &ids) {
  std::unique_lock<std::mutex> templock(dump_mutex_);
  for (size_t i = 0; i < ids.size(); i++) {
    markDelete(ids[i]);
  }
  deleted_num_ += ids.size();
  LOG(INFO) << "delete index success! size=" << ids.size()
            << ", total=" << deleted_num_;
  return 0;
}

int GammaIndexHNSWLIB::Dump(const std::string &dir) {
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string index_dir = dir + "/" + index_name;
  if (utils::make_dir(index_dir.c_str())) {
    LOG(ERROR) << "mkdir error, index dir=" << index_dir;
    return IO_ERR;
  }

  std::string index_file = index_dir + "/hnswlib.index";
  std::unique_lock<std::mutex> templock(dump_mutex_);
  saveIndex(index_file);
  return 0;
}

int GammaIndexHNSWLIB::Load(const std::string &index_dir) {
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string index_file = index_dir + "/" + index_name + "/hnswlib.index";
  if (!utils::file_exist(index_file)) {
    LOG(INFO) << index_file << " isn't existed, skip loading";
    return 0;  // it should train again after load
  }
  if (metric_type_ == DistanceComputeType::INNER_PRODUCT) {
    loadIndex(index_file, space_interface_ip_);
  } else {
    loadIndex(index_file, space_interface_);
  }
  indexed_vec_count_ = cur_element_count;
  return indexed_vec_count_;
}

}  // namespace tig_gamma
