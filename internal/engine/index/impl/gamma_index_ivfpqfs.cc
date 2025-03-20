/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This faiss source code is licensed under the MIT license.
 * https://github.com/facebookresearch/faiss/blob/master/LICENSE
 *
 *
 * The works below are modified based on faiss:
 * 1. Replace the static batch indexing with real time indexing
 * 2. Add the fine-grained sort after PQ coarse sort
 * 3. Add the numeric field and bitmap filters in the process of searching
 *
 * Modified works copyright 2019 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#include "gamma_index_ivfpqfs.h"

#include <algorithm>
#include <stdexcept>
#include <vector>

#include "faiss/invlists/BlockInvertedLists.h"
#include "index/index_io.h"
#include "omp.h"
#include "util/bitmap.h"
#include "util/utils.h"

namespace vearch {

inline size_t roundup(size_t a, size_t b) { return (a + b - 1) / b * b; }

REGISTER_INDEX(IVFPQFastScan, GammaIVFPQFastScanIndex)

GammaIVFPQFastScanIndex::GammaIVFPQFastScanIndex() : indexed_vec_count_(0) {
  compaction_ = false;
  compact_bucket_no_ = 0;
  compacted_num_ = 0;
  updated_num_ = 0;
  is_trained = false;
  opq_ = nullptr;
#ifdef PERFORMANCE_TESTING
  add_count_ = 0;
#endif
}

GammaIVFPQFastScanIndex::~GammaIVFPQFastScanIndex() {
  delete rt_invert_index_ptr_;
  rt_invert_index_ptr_ = nullptr;
  delete invlists;
  invlists = nullptr;
  delete quantizer;  // it will not be delete in parent class
  quantizer = nullptr;
  delete opq_;
  opq_ = nullptr;

  CHECK_DELETE(model_param_);
  int ret = pthread_rwlock_destroy(&shared_mutex_);
  if (0 != ret) {
    LOG(ERROR) << "destory read write lock error, ret=" << ret;
  }
}

Status GammaIVFPQFastScanIndex::Init(const std::string &model_parameters,
                                     int training_threshold) {
  model_param_ = new IVFPQFastScanModelParams();
  IVFPQFastScanModelParams &ivfpqfs_param = *model_param_;
  if (model_parameters != "") {
    Status status = ivfpqfs_param.Parse(model_parameters.c_str());
    if (!status.ok()) return status;
  }
  LOG(INFO) << ivfpqfs_param.ToString();

  d = vector_->MetaInfo()->Dimension();

  if (d % ivfpqfs_param.nsubvector != 0) {
    std::string msg = std::string("Dimension [") +
                      std::to_string(vector_->MetaInfo()->Dimension()) +
                      "] cannot divide by nsubvector [" +
                      std::to_string(ivfpqfs_param.nsubvector);
    LOG(ERROR) << msg;
    return Status::IndexError(msg);
  }

  nlist = ivfpqfs_param.ncentroids;
  if (training_threshold) {
    training_threshold_ = training_threshold;
  } else {
    training_threshold_ = nlist * min_points_per_centroid;
  }
  if (ivfpqfs_param.has_hnsw == false) {
    quantizer = new faiss::IndexFlatL2(d);
    quantizer_type_ = 0;
  } else {
    faiss::IndexHNSWFlat *hnsw_flat =
        new faiss::IndexHNSWFlat(d, ivfpqfs_param.nlinks);
    hnsw_flat->hnsw.efSearch = ivfpqfs_param.efSearch;
    hnsw_flat->hnsw.efConstruction = ivfpqfs_param.efConstruction;
    hnsw_flat->hnsw.search_bounded_queue = false;
    quantizer = hnsw_flat;
    quantizer_type_ = 1;
  }

  if (ivfpqfs_param.has_opq) {
    if (d % ivfpqfs_param.opq_nsubvector != 0) {
      std::string msg =
          std::to_string(d) + " % " +
          std::to_string(ivfpqfs_param.opq_nsubvector) +
          " != 0, opq nsubvector should be divisible by dimension.";
      LOG(ERROR) << msg;
      return Status::IndexError(msg);
    }
    opq_ = new faiss::OPQMatrix(d, ivfpqfs_param.opq_nsubvector, d);
  }

  pq.d = d;
  pq.M = ivfpqfs_param.nsubvector;
  pq.nbits = ivfpqfs_param.nbits_per_idx;
  pq.set_derived_values();

  metric_type_ = ivfpqfs_param.metric_type;
  if (metric_type_ == DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }

  if (metric_type == faiss::METRIC_INNER_PRODUCT) {
    cp.spherical = true;
  }
  quantizer_trains_alone = 0;
  own_fields = false;
  clustering_index = nullptr;
  parallel_mode = 0;

  code_size = pq.code_size;
  own_invlists = true;
  invlists = new faiss::ArrayInvertedLists(nlist, code_size);

  is_trained = false;
  by_residual = false;

  bbs = ivfpqfs_param.bbs;
  M2 = roundup(pq.M, 2);

  replace_invlists(new faiss::BlockInvertedLists(nlist, bbs, bbs * M2 / 2),
                   true);

  d_ = d;

  this->nprobe = ivfpqfs_param.nprobe;
  int ret = pthread_rwlock_init(&shared_mutex_, NULL);
  if (ret != 0) {
    LOG(ERROR) << "init read-write lock error, ret=" << ret;
  }

  return Status::OK();
}

RetrievalParameters *GammaIVFPQFastScanIndex::Parse(
    const std::string &parameters) {
  if (parameters == "") {
    return new IVFPQRetrievalParameters(this->nprobe, metric_type_);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  IVFPQRetrievalParameters *retrieval_params =
      new IVFPQRetrievalParameters(this->nprobe, metric_type_);
  if (!jp.GetString("metric_type", metric_type)) {
    if (strcasecmp("L2", metric_type.c_str()) &&
        strcasecmp("InnerProduct", metric_type.c_str())) {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
    }
    if (!strcasecmp("L2", metric_type.c_str())) {
      retrieval_params->SetDistanceComputeType(DistanceComputeType::L2);
    } else {
      retrieval_params->SetDistanceComputeType(
          DistanceComputeType::INNER_PRODUCT);
    }
  } else {
    retrieval_params->SetDistanceComputeType(metric_type_);
  }

  int recall_num;
  int nprobe;

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
  return retrieval_params;
}

int GammaIVFPQFastScanIndex::Indexing() {
  if (this->is_trained) {
    LOG(INFO) << "gamma ivfpq index is already trained, skip indexing";
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
    num = (size_t)training_threshold_;
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
  if (lens.size() == 1) {
    train_raw_vec = headers.Get(0);
  } else {
    train_raw_vec = new uint8_t[raw_d * num * sizeof(float)];
    del_train_raw_vec.set(train_raw_vec);
    size_t offset = 0;
    for (size_t i = 0; i < headers.Size(); ++i) {
      memcpy((void *)(train_raw_vec + offset), (void *)headers.Get(i),
             sizeof(float) * raw_d * lens[i]);
      offset += sizeof(float) * raw_d * lens[i];
    }
  }

  const float *xt = nullptr;
  utils::ScopeDeleter1<float> del_xt;
  if (opq_ != nullptr) {
    opq_->train(num, (const float *)train_raw_vec);
    xt = opq_->apply(num, (const float *)train_raw_vec);
    del_xt.set(xt == (const float *)train_raw_vec ? nullptr : xt);
  } else {
    xt = (const float *)train_raw_vec;
  }

  faiss::IndexIVFPQFastScan::train(num, xt);

  LOG(INFO) << "train successed!";
  return 0;
}

int GammaIVFPQFastScanIndex::Delete(const std::vector<int64_t> &ids) {
  return 0;
}

int GammaIVFPQFastScanIndex::Update(const std::vector<int64_t> &ids,
                                    const std::vector<const uint8_t *> &vecs) {
  return 0;
}

bool GammaIVFPQFastScanIndex::Add(int n, const uint8_t *vec) {
#ifdef PERFORMANCE_TESTING
  double t0 = faiss::getmillisecs();
#endif
  pthread_rwlock_wrlock(&shared_mutex_);
  add(n, (const float *)vec);
  pthread_rwlock_unlock(&shared_mutex_);
  indexed_vec_count_ += n;
#ifdef PERFORMANCE_TESTING
  add_count_ += n;
  if (add_count_ >= 10000) {
    double t1 = faiss::getmillisecs();
    LOG(DEBUG) << "Add time [" << (t1 - t0) / n << "]ms, count "
               << indexed_vec_count_;
    // rt_invert_index_ptr_->PrintBucketSize();
    add_count_ = 0;
  }
#endif
  return true;
}

namespace {

using HeapForIP = faiss::CMin<float, idx_t>;
using HeapForL2 = faiss::CMax<float, idx_t>;

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

void compute_dis(int k, const float *xi, float *simi, idx_t *idxi,
                 float *recall_simi, idx_t *recall_idxi, int recall_num,
                 faiss::MetricType metric_type, VectorReader *vec) {
  ScopeVectors scope_vecs;
  std::vector<idx_t> vids(recall_idxi, recall_idxi + recall_num);
  if (vec->Gets(vids, scope_vecs)) {
    LOG(ERROR) << "get raw vector failed";
    return;
  }
  int raw_d = vec->MetaInfo()->Dimension();
  for (int j = 0; j < recall_num; j++) {
    if (recall_idxi[j] == -1) continue;
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
  reorder_result(metric_type, k, simi, idxi);
}

}  // namespace

int GammaIVFPQFastScanIndex::Search(RetrievalContext *retrieval_context, int n,
                                    const uint8_t *x, int k, float *distances,
                                    idx_t *labels) {
  /*
  IVFPQRetrievalParameters *retrieval_params =
      dynamic_cast<IVFPQRetrievalParameters *>(
          retrieval_context->RetrievalParams());

  utils::ScopeDeleter1<IVFPQRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params = new IVFPQRetrievalParameters();
    del_params.set(retrieval_params);
  }

  SearchCondition *condition =
      dynamic_cast<SearchCondition *>(retrieval_context);
  if (condition->brute_force_search == true || is_trained == false) {
    // reset retrieval_params
    delete retrieval_context->RetrievalParams();
    retrieval_context->retrieval_params_ = new FlatRetrievalParameters(
        retrieval_params->ParallelOnQueries(),
  retrieval_params->GetDistanceComputeType()); int ret =
        GammaFLATIndex::Search(retrieval_context, n, x, k, distances, labels);
    return ret;
  }
  */
  if (!is_trained) {
    LOG(WARNING) << "index not trained, wait for trainning finished.";
    return 0;
  }
  const float *xq = reinterpret_cast<const float *>(x);
  float *recall_distances = nullptr;
  idx_t *recall_labels = nullptr;
  utils::ScopeDeleter<float> del1;
  utils::ScopeDeleter<idx_t> del2;

  int k_rerank = 0;
  if (rerank_ > 0) {
    k_rerank = k * rerank_;
    recall_distances = new float[n * k_rerank];
    recall_labels = new idx_t[n * k_rerank];
    del1.set(recall_distances);
    del2.set(recall_labels);

#ifdef PERFORMANCE_TESTING
    if (retrieval_context->GetPerfTool()) {
      retrieval_context->GetPerfTool()->Perf("search prepare");
    }
#endif
    pthread_rwlock_rdlock(&shared_mutex_);
    search(n, xq, k_rerank, recall_distances, recall_labels);
    pthread_rwlock_unlock(&shared_mutex_);
  } else {
#ifdef PERFORMANCE_TESTING
    if (retrieval_context->GetPerfTool()) {
      retrieval_context->GetPerfTool()->Perf("search prepare");
    }
#endif
    pthread_rwlock_rdlock(&shared_mutex_);
    search(n, xq, k, distances, labels);
    pthread_rwlock_unlock(&shared_mutex_);
  }
#ifdef PERFORMANCE_TESTING
  if (retrieval_context->GetPerfTool()) {
    retrieval_context->GetPerfTool()->Perf("search");
  }
#endif
  if (rerank_ > 0) {
#pragma omp for
    for (int i = 0; i < n; i++) {
      float *simi = distances + i * k;
      idx_t *idxi = labels + i * k;

      float *recall_simi = recall_distances + i * k_rerank;
      idx_t *recall_idxi = recall_labels + i * k_rerank;

      init_result(metric_type, k, simi, idxi);
      compute_dis(k, xq + i * d, simi, idxi, recall_simi, recall_idxi, k_rerank,
                  metric_type, vector_);
    }
#ifdef PERFORMANCE_TESTING
    if (retrieval_context->GetPerfTool()) {
      retrieval_context->GetPerfTool()->Perf("recompute");
    }
#endif
  }
  return 0;
}

Status GammaIVFPQFastScanIndex::Dump(const std::string &dir) {
  return Status::OK();
}

Status GammaIVFPQFastScanIndex::Load(const std::string &index_dir,
                                     int64_t &load_num) {
  load_num = 0;
  return Status::OK();
}

}  // namespace vearch
