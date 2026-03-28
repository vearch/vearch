/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This faiss source code is licensed under the MIT license.
 * https://github.com/facebookresearch/faiss/blob/master/LICENSE
 *
 *
 * The works below are modified based on faiss:
 * 1. Replace the static batch indexing with real time indexing
 * 2. Add the numeric field and bitmap filters in the process of searching
 *
 * Modified works copyright 2019 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#include "gamma_index_ivfrabitq.h"

#include <faiss/IndexFlat.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/utils.h>
#include <omp.h>

#include <cstdio>
#include <cstdlib>
#include <memory>

#include "common/gamma_common_data.h"
#include "index/index_io.h"
#include "vector/rocksdb_raw_vector.h"
namespace vearch {

using namespace faiss;

REGISTER_INDEX(IVFRABITQ, GammaIVFRABITQIndex);

GammaIVFRABITQIndex::GammaIVFRABITQIndex() {
  indexed_vec_count_ = 0;
  updated_num_ = 0;
  rt_invert_index_ptr_ = nullptr;
#ifdef PERFORMANCE_TESTING
  add_count_ = 0;
#endif
}

GammaIVFRABITQIndex::~GammaIVFRABITQIndex() {
  CHECK_DELETE(rt_invert_index_ptr_);
  CHECK_DELETE(invlists);
  CHECK_DELETE(quantizer);
}

Status GammaIVFRABITQIndex::Init(const std::string &model_parameters,
  int training_threshold) {
  model_param_ = new IVFRABITQModelParams();
  IVFRABITQModelParams &ivfrabitq_param = *model_param_;
  if (model_parameters != "") {
    Status status = ivfrabitq_param.Parse(model_parameters.c_str());
    if (!status.ok()) return status;
  }

  d = vector_->MetaInfo()->Dimension();

  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);

  this->nlist = ivfrabitq_param.ncentroids;
  if (training_threshold) {
    training_threshold_ = training_threshold;
  } else {
    // shouldn't less than max_points_per_centroid because of pq.train() when
    // nbit = 8 and ksub = 2^8
    training_threshold_ =
    std::max((int)nlist * default_points_per_centroid, max_points_per_centroid);
  }
  ivfrabitq_param.training_threshold = training_threshold_;

  metric_type_ = ivfrabitq_param.metric_type;
  if (metric_type_ == DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }

  if (ivfrabitq_param.has_hnsw == false) {
    quantizer = new faiss::IndexFlat(d, metric_type);
    quantizer_type_ = 0;
  } else {
    faiss::IndexHNSWFlat *hnsw_flat =
    new faiss::IndexHNSWFlat(d, ivfrabitq_param.nlinks, metric_type);
    hnsw_flat->hnsw.efSearch = ivfrabitq_param.efSearch;
    hnsw_flat->hnsw.efConstruction = ivfrabitq_param.efConstruction;
    hnsw_flat->hnsw.search_bounded_queue = false;
    quantizer = hnsw_flat;
    quantizer_type_ = 1;
  }

  // rabitq
  rabitq.nb_bits = ivfrabitq_param.nb_bits;
  rabitq.d = d;
  rabitq.metric_type = metric_type;
  rabitq.code_size = rabitq.compute_code_size(d, rabitq.nb_bits);

  this->qb = ivfrabitq_param.qb;

  // for debug
  ivfrabitq_param.d = d;
  ivfrabitq_param.code_size = rabitq.code_size;
  LOG(INFO) << ivfrabitq_param.ToString();

  code_size = rabitq.code_size;
  own_fields = false;

  clustering_index = nullptr;
  cp.niter = 10;
  if (metric_type == faiss::METRIC_INNER_PRODUCT) {
    cp.spherical = true;
  }

  is_trained = false;
  by_residual = true;

  // if nlist is very large,
  // the size of RTInvertIndex bucket should be smaller
  rt_invert_index_ptr_ = new realtime::RTInvertIndex(
    this->nlist, this->code_size, raw_vec->Bitmap(),
    ivfrabitq_param.bucket_init_size, ivfrabitq_param.bucket_max_size);

  if (this->invlists) {
    delete this->invlists;
    this->invlists = nullptr;
  }
  d_ = d;
  bool ret = rt_invert_index_ptr_->Init();

  if (ret) {
    this->invlists =
    new realtime::RTInvertedLists(rt_invert_index_ptr_, nlist, code_size);
  }

  if ((size_t)ivfrabitq_param.nprobe <= this->nlist) {
  this->nprobe = ivfrabitq_param.nprobe;
  } else {
    std::string msg =
    "nprobe = " + std::to_string(ivfrabitq_param.nprobe) +
    " should less than ncentroids = " + std::to_string(this->nlist);
    LOG(ERROR) << msg;
    return Status::ParamError(msg);
  }
  return Status::OK();
}

RetrievalParameters *GammaIVFRABITQIndex::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new IVFRABITQRetrievalParameters(this->nprobe, metric_type_);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  IVFRABITQRetrievalParameters *retrieval_params =
      new IVFRABITQRetrievalParameters(this->nprobe, metric_type_);
  if (!jp.GetString("metric_type", metric_type)) {
    if (!strcasecmp("L2", metric_type.c_str())) {
      retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
    } else if (!strcasecmp("InnerProduct", metric_type.c_str())) {
      retrieval_params->SetDistanceComputeType(
          DistanceComputeType::INNER_PRODUCT);
    } else {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                << ", so use default value.";
      retrieval_params->SetDistanceComputeType(metric_type_);
    }
  } else {
    retrieval_params->SetDistanceComputeType(metric_type_);
  }

  int recall_num;
  int nprobe;
  int parallel_on_queries;
  int collect_metrics;

  if (!jp.GetInt("recall_num", recall_num)) {
    if (recall_num > 0) {
      retrieval_params->SetRecallNum(recall_num);
    }
  }

  if (!jp.GetInt("nprobe", nprobe)) {
    if (nprobe > 0) {
      retrieval_params->SetNprobe(nprobe);
    }
  }

  if (!jp.GetInt("parallel_on_queries", parallel_on_queries)) {
    if (parallel_on_queries != 0) {
      retrieval_params->SetParallelOnQueries(true);
    } else {
      retrieval_params->SetParallelOnQueries(false);
    }
  }

  if (!jp.GetInt("collect_metrics", collect_metrics)) {
    if (collect_metrics > 0) {
      retrieval_params->SetCollectMetrics(collect_metrics);
    }
  }

  int qb;
  if (!jp.GetInt("qb", qb)) {
    if (qb >= 0 && qb <= 8) {
      retrieval_params->SetQb(qb);
    } else {
      LOG(ERROR) << "invalid qb = " << qb << " should be integer in [0, 8]";
      retrieval_params->SetQb(this->qb);
    }
  } else {
    retrieval_params->SetQb(this->qb);
  }

  bool centered;
  if (!jp.GetBool("centered", centered)) {
    retrieval_params->SetCentered(centered);
  }

  return retrieval_params;
}

int GammaIVFRABITQIndex::Indexing() {
  if (this->is_trained) {
    LOG(INFO) << "gamma GammaIVFRABITQIndex is already trained, skip indexing";
    return 0;
  }
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  size_t vectors_count = raw_vec->MetaInfo()->Size();

  size_t num;
  if ((size_t)training_threshold_ < nlist) {
    num = nlist * 39;
    LOG(WARNING) << "Because training_threshold[" << training_threshold_
                << "] < ncentroids[" << nlist
                << "], training_threshold becomes ncentroids * 39[" << num
                << "].";
  } else if ((size_t)training_threshold_ <= nlist * 256) {
    if ((size_t)training_threshold_ < nlist * 39) {
      LOG(WARNING)
          << "training_threshold[" << training_threshold_ << "] is too small. "
          << "The appropriate range is [ncentroids * 39, ncentroids * 256]";
    }
    num = training_threshold_;
  } else {
    num = nlist * 256;
    LOG(WARNING)
        << "training_threshold[" << training_threshold_ << "] is too big. "
        << "The appropriate range is [ncentroids * 39, ncentroids * 256]."
        << "training_threshold becomes ncentroids * 256[" << num << "].";
  }
  if (num > vectors_count) {
    LOG(ERROR) << "vector total count [" << vectors_count
              << "] less then training_threshold[" << num << "], failed!";
    return -1;
  }

  ScopeVectors headers;
  std::vector<int> lens;
  raw_vec->GetVectorHeader(0, num, headers, lens);

  // merge vectors
  int raw_d = raw_vec->MetaInfo()->Dimension();
  const uint8_t *train_raw_vec = nullptr;
  utils::ScopeDeleter1<uint8_t> del_train_raw_vec;
  size_t n_get = 0;
  if (lens.size() == 1) {
    train_raw_vec = headers.Get(0);
    n_get = lens[0];
    if (num > n_get) {
      LOG(ERROR) << "training vector get count [" << n_get
                << "] less then training_threshold[" << num << "], failed!";
      return -2;
    }
  } else {
    train_raw_vec = new uint8_t[raw_d * num * sizeof(float)];
    del_train_raw_vec.set(train_raw_vec);
    size_t offset = 0;
    for (size_t i = 0; i < headers.Size(); ++i) {
      n_get += lens[i];
      memcpy((void *)(train_raw_vec + offset), (void *)headers.Get(i),
            sizeof(float) * raw_d * lens[i]);
      offset += sizeof(float) * raw_d * lens[i];
    }
  }
  LOG(INFO) << "train vector wanted num=" << num << ", real num=" << n_get;

  IndexIVFRaBitQ::train(n_get, (const float *)train_raw_vec);

  LOG(INFO) << "train successed!";
  return 0;
}

bool GammaIVFRABITQIndex::Add(int n, const uint8_t *vec) {
#ifdef PERFORMANCE_TESTING
  double t0 = faiss::getmillisecs();
#endif
  if (not is_trained) {
    return 0;
  }
  std::map<int, std::vector<long>> new_keys;
  std::map<int, std::vector<uint8_t>> new_codes;

  idx_t *idx;
  utils::ScopeDeleter<idx_t> del_idx;

  idx_t *idx0 = new idx_t[n];
  quantizer->assign(n, (const float *)vec, idx0);
  idx = idx0;
  del_idx.set(idx);

  uint8_t *xcodes = new uint8_t[n * code_size];
  utils::ScopeDeleter<uint8_t> del_xcodes(xcodes);

  long vid = indexed_vec_count_;
  int n_add = 0;
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  for (int i = 0; i < n; i++) {
    long list_no = idx[i];
    if (raw_vec->Bitmap()->Test(vid + i)) {
      continue;
    }
    if (list_no >= (long)nlist) {
      LOG(WARNING) << "ivfrabitq add invalid list_no=" << list_no << ", vid=" << vid + i;
      continue;
    }
    if (list_no < 0) {
      LOG(WARNING) << "ivfrabitq add invalid list_no=" << list_no << ", vid=" << vid + i;
      list_no = vid % nlist;
    }
    std::vector<float> centroid(d);
    quantizer->reconstruct(list_no, centroid.data());
    rabitq.compute_codes_core(
            (const float *)vec + i * d, xcodes + i * code_size, 1, centroid.data());    
    new_keys[list_no].push_back(vid + i);
    size_t ofs = new_codes[list_no].size();
    new_codes[list_no].resize(ofs + code_size);
    memcpy((void *)(new_codes[list_no].data() + ofs), (void *)(xcodes + i * code_size), code_size);
    n_add +=1;
  }

  /* stage 2 : add invert info to invert index */
  if (!rt_invert_index_ptr_->AddKeys(new_keys, new_codes)) {
    return false;
  }
  indexed_vec_count_ += n;
#ifdef PERFORMANCE_TESTING
  add_count_ += n;
  if (add_count_ >= ADD_COUNT_THRESHOLD) {
    double t1 = faiss::getmillisecs();
    LOG(DEBUG) << "Add time [" << (t1 - t0) / n << "]ms, count "
              << indexed_vec_count_ << " wanted n=" << n
              << " real add=" << n_add;
    add_count_ = 0;
  }
#endif
  return true;
}

void GammaIVFRABITQIndex::Describe() {
  if (rt_invert_index_ptr_) rt_invert_index_ptr_->PrintBucketSize();
}

int GammaIVFRABITQIndex::Update(const std::vector<int64_t> &ids,
                              const std::vector<const uint8_t *> &vecs) {
  if (not is_trained) {
    return 0;
  }
  int n_update = 0;
  for (size_t i = 0; i < ids.size(); i++) {
    if (ids[i] < 0) {
      LOG(WARNING) << "ivfflat update invalid id=" << ids[i];
      continue;
    }
    if (vecs[i] == nullptr) {
      continue;
    }
    const float *vec = reinterpret_cast<const float *>(vecs[i]);
    if (vec == nullptr) {
      continue;
    }
    idx_t idx = -1;
    quantizer->assign(1, vec, &idx);
    std::vector<uint8_t> code(code_size);
    std::vector<float> centroid(d);
    quantizer->reconstruct(idx, centroid.data());
    rabitq.compute_codes_core(
            (const float *)vec, code.data(), 1, centroid.data());  
    rt_invert_index_ptr_->Update(idx, ids[i], code);
    n_update++;
  }
  updated_num_ += n_update;
  LOG(DEBUG) << "update index success! size=" << ids.size()
            << ", n_update=" << n_update << ", updated_num="
            << updated_num_;
  // now check id need to do compaction
  rt_invert_index_ptr_->CompactIfNeed();
  return 0;
}

int GammaIVFRABITQIndex::Delete(const std::vector<int64_t> &ids) {
  if (not is_trained) {
    return 0;
  }
  std::vector<int64_t> vids(ids.begin(), ids.end());
  rt_invert_index_ptr_->Delete(vids.data(), vids.size());
  return 0;
}

int GammaIVFRABITQIndex::Search(RetrievalContext *retrieval_context, int n,
                              const uint8_t *rx, int k, float *distances,
                              idx_t *labels) {
#ifndef FAISSLIKE_INDEX
  IVFRABITQRetrievalParameters *retrieval_params =
      dynamic_cast<IVFRABITQRetrievalParameters *>(
          retrieval_context->RetrievalParams());

  utils::ScopeDeleter1<IVFRABITQRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params =
        new IVFRABITQRetrievalParameters(this->nprobe, metric_type_);
    del_params.set(retrieval_params);
  }

  SearchCondition *condition =
      dynamic_cast<SearchCondition *>(retrieval_context);
  if (condition->brute_force_search == true || is_trained == false) {
    // reset retrieval_params
    delete retrieval_context->RetrievalParams();
    retrieval_context->retrieval_params_ =
        new FlatRetrievalParameters(retrieval_params->ParallelOnQueries(),
                                    retrieval_params->GetDistanceComputeType());
    int ret =
        GammaFLATIndex::Search(retrieval_context, n, rx, k, distances, labels);
    return ret;
  }
  int nprobe = this->nprobe;
  if (retrieval_params->Nprobe() > 0 &&
      (size_t)retrieval_params->Nprobe() <= this->nlist) {
    nprobe = retrieval_params->Nprobe();
  } else {
    LOG(WARNING) << "nlist = " << this->nlist
                << ", nprobe = " << retrieval_params->Nprobe()
                << ", invalid, now use:" << this->nprobe;
  }
#else
  int nprobe = this->nprobe;
#endif
  const float *x = reinterpret_cast<const float *>(rx);

  std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
  std::unique_ptr<float[]> coarse_dis(new float[n * nprobe]);

  quantizer->search(n, x, nprobe, coarse_dis.get(), idx.get());

  search_preassigned(retrieval_context, n, x, k, idx.get(), coarse_dis.get(),
                    distances, labels, nprobe, false);
  if (RequestContext::is_killed()) {
    return -2;
  }

  return 0;
}

/*****************************************************
* Depending on parallel_mode, there are two possible ways
* to organize the search. Here we define local functions
* that are in common between the two
******************************************************/

namespace {

using HeapForIP = faiss::CMin<float, idx_t>;
using HeapForL2 = faiss::CMax<float, idx_t>;

// intialize + reorder a result heap

int init_result(faiss::MetricType metric_type, int k, float *simi,
                idx_t *idxi) {
  if (metric_type == faiss::METRIC_INNER_PRODUCT) {
    faiss::heap_heapify<HeapForIP>(k, simi, idxi);
  } else {
    faiss::heap_heapify<HeapForL2>(k, simi, idxi);
  }
  return 0;
};

int reorder_result(faiss::MetricType metric_type, int k, float *simi,
                    idx_t *idxi) {
  if (metric_type == faiss::METRIC_INNER_PRODUCT) {
    faiss::heap_reorder<HeapForIP>(k, simi, idxi);
  } else {
    faiss::heap_reorder<HeapForL2>(k, simi, idxi);
  }
  return 0;
};

// single list scan using the current scanner (with query
// set porperly) and storing results in simi and idxi
size_t scan_one_list(faiss::InvertedListScanner *scanner, idx_t key,
                      float coarse_dis_i, float *simi, idx_t *idxi, int k,
                      idx_t nlist, faiss::InvertedLists *invlists,
                      bool store_pairs) {
  if (key < 0) {
    // not enough centroids for multiprobe
    return 0;
  }
  if (key >= (idx_t)nlist) {
    LOG(WARNING) << "Invalid key=" << key << ", nlist=" << nlist;
    return 0;
  }

  size_t list_size = invlists->list_size(key);

  // don't waste time on empty lists
  if (list_size == 0) {
    return 0;
  }

  std::unique_ptr<faiss::InvertedLists::ScopedIds> sids;
  const idx_t *ids = nullptr;

  if (!store_pairs) {
    sids.reset(new faiss::InvertedLists::ScopedIds(invlists, key));
    ids = sids->get();
  }

  scanner->set_list(key, coarse_dis_i);

  // scan_codes need uint8_t *
  const uint8_t *codes = nullptr;
  faiss::InvertedLists::ScopedCodes scodes(invlists, key);
  codes = scodes.get();

  scanner->scan_codes(list_size, codes, ids, simi, idxi, k);

  return list_size;
};

void compute_dis(int k, const float *xi, float *simi, idx_t *idxi,
                  float *recall_simi, idx_t *recall_idxi, int recall_num,
                  bool rerank, faiss::MetricType metric_type, VectorReader *vec,
                  RetrievalContext *retrieval_context) {
  if (rerank == true) {
    ScopeVectors scope_vecs;
    std::vector<idx_t> vids(recall_idxi, recall_idxi + recall_num);
    int ret = vec->Gets(vids, scope_vecs);
    if (ret != 0) {
      LOG(ERROR) << "get raw vector failed, ret=" << ret;
      return;
    }
    int raw_d = vec->MetaInfo()->Dimension();
    for (int j = 0; j < recall_num; j++) {
      if (RequestContext::is_killed()) return;
      if (recall_idxi[j] < 0) continue;
      float dis = 0;
      if (scope_vecs.Get(j) == nullptr) {
        continue;
      }
      const float *vec = reinterpret_cast<const float *>(scope_vecs.Get(j));
      if (vec == nullptr) {
        continue;
      }
      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        dis = faiss::fvec_inner_product(xi, vec, raw_d);
      } else {
        dis = faiss::fvec_L2sqr(xi, vec, raw_d);
      }

      if (retrieval_context->IsSimilarScoreValid(dis) == true) {
        if (metric_type == faiss::METRIC_INNER_PRODUCT) {
          if (HeapForIP::cmp(simi[0], dis)) {
            faiss::heap_pop<HeapForIP>(k, simi, idxi);
            long id = recall_idxi[j];
            faiss::heap_push<HeapForIP>(k, simi, idxi, dis, id);
          }
        } else {
          if (HeapForL2::cmp(simi[0], dis)) {
            faiss::heap_pop<HeapForL2>(k, simi, idxi);
            long id = recall_idxi[j];
            faiss::heap_push<HeapForL2>(k, simi, idxi, dis, id);
          }
        }
      }
    }
    reorder_result(metric_type, k, simi, idxi);
  } else {
    // compute without rank
    reorder_result(metric_type, recall_num, recall_simi, recall_idxi);
  }
}

}  // namespace

void GammaIVFRABITQIndex::search_preassigned(RetrievalContext *retrieval_context,
                                          idx_t n, const float *x, int k,
                                          const idx_t *keys,
                                          const float *coarse_dis,
                                          float *distances, idx_t *labels,
                                          int nprobe, bool store_pairs) {
  IVFRABITQRetrievalParameters *retrieval_params =
      dynamic_cast<IVFRABITQRetrievalParameters *>(
          retrieval_context->RetrievalParams());
  utils::ScopeDeleter1<IVFRABITQRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params =
        new IVFRABITQRetrievalParameters(this->nprobe, metric_type_);
    del_params.set(retrieval_params);
  }

  faiss::MetricType metric_type;
  if (retrieval_params->GetDistanceComputeType() ==
      DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }

  long max_codes = 1000000000;
  size_t ndis = 0;

  using HeapForIP = CMin<float, idx_t>;
  using HeapForL2 = CMax<float, idx_t>;

  int recall_num = k;
  bool rerank = retrieval_params->RecallNum() > 0 ? true : false;
  if (retrieval_params->RecallNum() > k) {
    recall_num = retrieval_params->RecallNum();
  }

  float *recall_distances = nullptr;
  idx_t *recall_labels = nullptr;
  if (rerank) {
    recall_distances = new float[n * recall_num];
    recall_labels = new idx_t[n * recall_num];
  }

  utils::ScopeDeleter<float> del1(recall_distances);
  utils::ScopeDeleter<idx_t> del2(recall_labels);

#ifdef PERFORMANCE_TESTING
  if (retrieval_context->GetPerfTool()) {
    retrieval_context->GetPerfTool()->Perf("search prepare");
  }
#endif

  bool parallel_mode = retrieval_params->ParallelOnQueries() ? 0 : 1;

  bool do_parallel =
      omp_get_max_threads() >= 2 && (parallel_mode == 0   ? n > 1
                                     : parallel_mode == 1 ? nprobe > 1
                                                          : nprobe * n > 1);

  if (RequestContext::is_killed()) {
    return;
  }
  RawData *request = RequestContext::get_current_request();
  int partition_id = RequestContext::get_partition_id();

#pragma omp parallel if (do_parallel) reduction(+ : ndis)
  {
    faiss::InvertedListScanner *scanner =
      GetInvertedListScanner(store_pairs, nullptr, retrieval_params->Qb(), retrieval_params->Centered(), retrieval_context);
    utils::ScopeDeleter1<faiss::InvertedListScanner> del(scanner);

    if (RequestContext::get_current_request() == nullptr) {
      RequestContext::ScopedContext(request, partition_id);
    }

    /****************************************************
    * Actual loops, depending on parallel_mode
    ****************************************************/

    if (parallel_mode == 0) {  // parallelize over queries
#pragma omp for
      for (idx_t i = 0; i < n; i++) {
        if (!RequestContext::is_killed()) {
          // loop over queries
          const float *xi = x + i * d;
          scanner->set_query(xi);
          float *simi = distances + i * k;
          idx_t *idxi = labels + i * k;
          init_result(metric_type, k, simi, idxi);

          float *recall_simi = simi;
          idx_t *recall_idxi = idxi;

          if (rerank) {
            recall_simi = recall_distances + i * recall_num;
            recall_idxi = recall_labels + i * recall_num;
            init_result(metric_type, recall_num, recall_simi, recall_idxi);
          }

          long nscan = 0;

          // loop over probes
          for (int ik = 0; ik < nprobe; ik++) {
            if (RequestContext::is_killed()) {
              break;
            }
            nscan += scan_one_list(scanner, keys[i * nprobe + ik],
                                  coarse_dis[i * nprobe + ik], recall_simi,
                                  recall_idxi, recall_num, this->nlist,
                                  this->invlists, store_pairs);

            if (max_codes && nscan >= max_codes) break;
          }

          ndis += nscan;
          compute_dis(k, x + i * d, simi, idxi, recall_simi, recall_idxi,
                    recall_num, rerank, metric_type, vector_,
                    retrieval_context);
        }
      }       // parallel for
    } else {  // parallelize over inverted lists
      std::vector<idx_t> local_idx(recall_num);
      std::vector<float> local_dis(recall_num);

      for (int i = 0; i < n; i++) {
          const float *xi = x + i * d;
          scanner->set_query(xi);

          init_result(metric_type, recall_num, local_dis.data(),
                      local_idx.data());

#pragma omp for schedule(dynamic)
          for (int ik = 0; ik < nprobe; ik++) {
            if (!RequestContext::is_killed()) {
              size_t nscan = scan_one_list(
                scanner, keys[i * nprobe + ik], coarse_dis[i * nprobe + ik],
                local_dis.data(), local_idx.data(), recall_num, this->nlist,
                this->invlists, store_pairs);
              ndis += nscan;
              // can't do the test on max_codes
              if (retrieval_params->CollectMetrics()) {
                LOG(TRACE) << "nscan: " << nscan << ", ik: " << ik << ", i: " << i;
              }
            }
          }

          // merge thread-local results

          float *simi = distances + i * k;
          idx_t *idxi = labels + i * k;

          float *recall_simi = simi;
          idx_t *recall_idxi = idxi;

          if (rerank) {
            recall_simi = recall_distances + i * recall_num;
            recall_idxi = recall_labels + i * recall_num;
          }

#pragma omp single
          {
            init_result(metric_type, k, simi, idxi);
            if (rerank) {
              init_result(metric_type, recall_num, recall_simi, recall_idxi);
            }
          }

#pragma omp barrier
#pragma omp critical
          {
            if (!RequestContext::is_killed()) {
              if (metric_type == faiss::METRIC_INNER_PRODUCT) {
                faiss::heap_addn<HeapForIP>(recall_num, recall_simi, recall_idxi,
                                          local_dis.data(), local_idx.data(),
                                          recall_num);
              } else {
                faiss::heap_addn<HeapForL2>(recall_num, recall_simi, recall_idxi,
                                          local_dis.data(), local_idx.data(),
                                          recall_num);
              }
            }
          }
#pragma omp barrier
#pragma omp single
          {
#ifdef PERFORMANCE_TESTING
            if (retrieval_context->GetPerfTool()) {
              retrieval_context->GetPerfTool()->Perf("coarse");
            }
#endif
            if (!RequestContext::is_killed()) {
              compute_dis(k, x + i * d, simi, idxi, recall_simi, recall_idxi,
                        recall_num, rerank, metric_type, vector_,
                        retrieval_context);
            }

#ifdef PERFORMANCE_TESTING
            if (retrieval_context->GetPerfTool()) {
              retrieval_context->GetPerfTool()->Perf("reorder");
            }
#endif
          }
      }
    }
  }  // parallel
  if (retrieval_params->CollectMetrics()) {
    LOG(TRACE) << "parallel_mode: " << parallel_mode << ", nprobe: " << nprobe
                << ", ndis: " << ndis;
  }
#ifdef PERFORMANCE_TESTING
  if (retrieval_context->GetPerfTool()) {
    std::string compute_msg = "compute ";
    compute_msg += std::to_string(n);
    retrieval_context->GetPerfTool()->Perf(compute_msg);
  }
#endif
}

std::string IVFRABITQToString(const faiss::IndexIVFRaBitQ *ivfrabitq) {
  std::stringstream ss;
  ss << "d=" << ivfrabitq->d << ", ntotal=" << ivfrabitq->ntotal
    << ", is_trained=" << ivfrabitq->is_trained
    << ", metric_type=" << ivfrabitq->metric_type << ", nlist=" << ivfrabitq->nlist
    << ", nprobe=" << ivfrabitq->nprobe << ", nb_bits=" << ivfrabitq->rabitq.nb_bits
    << ", code_size=" << ivfrabitq->code_size << ", by_residual=" << ivfrabitq->by_residual
    << ", qb=" << static_cast<int>(ivfrabitq->qb);

  faiss::IndexHNSWFlat *hnsw_flat =
  dynamic_cast<faiss::IndexHNSWFlat *>(ivfrabitq->quantizer);
  if (hnsw_flat) {
    ss << ", hnsw: efSearch=" << hnsw_flat->hnsw.efSearch
      << ", efConstruction=" << hnsw_flat->hnsw.efConstruction
      << ", search_bounded_queue=" << hnsw_flat->hnsw.search_bounded_queue;
  }

  return ss.str();
}

Status GammaIVFRABITQIndex::Dump(const std::string &dir) {
  if (!this->is_trained) {
    LOG(INFO) << "gamma index is not trained, skip dumping";
    return Status::OK();
  }
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string index_dir = dir + "/" + index_name;
  if (utils::make_dir(index_dir.c_str())) {
    std::string msg = std::string("mkdir error, index dir=") + index_dir;
    LOG(ERROR) << msg;
    return Status::PathNotFound(msg);
  }

  std::string index_file = index_dir + "/ivfrabitq.index";
  faiss::IOWriter *f = new FileIOWriter(index_file.c_str());
  utils::ScopeDeleter1<FileIOWriter> del((FileIOWriter *)f);
  const IndexIVFRaBitQ *ivfrabitq = static_cast<const IndexIVFRaBitQ *>(this);
  // keep format same as faiss, 1-bit (backward compatible) or multi-bit (new format)
  if (ivfrabitq->rabitq.nb_bits == 1) {
    uint32_t h = faiss::fourcc("Iwrq"); // 1-bit (backward compatible)
    WRITE1(h);
    vearch::write_ivf_header(ivfrabitq, f);
    vearch::write_RaBitQuantizer(&ivfrabitq->rabitq, f, false);
  } else {
    uint32_t h = faiss::fourcc("Iwrr"); // multi-bit (new format)
    WRITE1(h);
    vearch::write_ivf_header(ivfrabitq, f);
    vearch::write_RaBitQuantizer(&ivfrabitq->rabitq, f, true);
  }

  WRITE1(ivfrabitq->code_size);
  WRITE1(ivfrabitq->by_residual);
  WRITE1(ivfrabitq->qb);

  int64_t indexed_count = indexed_vec_count_;
  if (WriteInvertedLists(f, rt_invert_index_ptr_)) {
    std::string msg =
        std::string("write invert list error, index name=") + index_name;
    LOG(ERROR) << msg;
    return Status::IOError(msg);
  }
  WRITE1(indexed_count);

  LOG(INFO) << "dump:" << IVFRABITQToString(ivfrabitq)
            << ", indexed count=" << indexed_count;
  return Status::OK();
};

Status GammaIVFRABITQIndex::Load(const std::string &dir, int64_t &load_num) {
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  std::string index_file = dir + "/" + index_name + "/ivfrabitq.index";
  if (!utils::file_exist(index_file)) {
    LOG(INFO) << index_file << " isn't existed, skip loading";
    load_num = 0;
    return Status::OK();  // it should train again after load
  }

  faiss::IOReader *f = new FileIOReader(index_file.c_str());
  utils::ScopeDeleter1<FileIOReader> del((FileIOReader *)f);
  uint32_t h;
  READ1(h);
  // keep format same as faiss, 1-bit (backward compatible) or multi-bit (new format)
  assert(h == faiss::fourcc("Iwrq") || h == faiss::fourcc("Iwrr"));
  IndexIVFRaBitQ *ivfrabitq = static_cast<IndexIVFRaBitQ *>(this);
  vearch::read_ivf_header(ivfrabitq, f, nullptr);  // not legacy

  if (h == faiss::fourcc("Iwrq")) {
    vearch::read_RaBitQuantizer(&ivfrabitq->rabitq, f, false);
  } else if (h == faiss::fourcc("Iwrr")) {
    // Iwrr = multi-bit format (new)
    vearch::read_RaBitQuantizer(&ivfrabitq->rabitq, f, true); // Reads nb_bits from file
  }

  READ1(ivfrabitq->code_size);
  READ1(ivfrabitq->by_residual);
  READ1(ivfrabitq->qb);

  // Update rabitq to match nb_bits
  ivfrabitq->rabitq.code_size =
          ivfrabitq->rabitq.compute_code_size(ivfrabitq->d, ivfrabitq->rabitq.nb_bits);
  ivfrabitq->code_size = ivfrabitq->rabitq.code_size;

  faiss::IndexHNSWFlat *hnsw_flat =
      dynamic_cast<faiss::IndexHNSWFlat *>(ivfrabitq->quantizer);
  if (hnsw_flat) {
    hnsw_flat->hnsw.search_bounded_queue = false;
    quantizer_type_ = 1;
  }

  int64_t indexed_vec_count = 0;
  Status status = ReadInvertedLists(f, rt_invert_index_ptr_, indexed_vec_count);
  if (status.code() == status::kIndexError) {
    indexed_vec_count_ = 0;
    load_num = 0;
    LOG(INFO) << "unsupported inverted list format, it need rebuilding!";
  } else if (status.ok()) {
    READ1(indexed_vec_count_);
    if (indexed_vec_count_ < 0) {
      std::string msg = std::string("invalid indexed count [") +
                        std::to_string(indexed_vec_count_) + "] vector size [" +
                        std::to_string(vector_->MetaInfo()->size_) + "]";
      LOG(ERROR) << msg;
      return Status::IndexError(msg);
    }
    LOG(INFO) << "load: " << IVFRABITQToString(ivfrabitq)
              << ", indexed vector count=" << indexed_vec_count_;
  } else {
    std::string msg =
        std::string("read invert list error, index name=") + index_name;
    LOG(ERROR) << msg;
    return Status::IndexError(msg);
  }
  assert(this->is_trained);
  load_num = indexed_vec_count_;
  return Status::OK();
};

}  // namespace vearch