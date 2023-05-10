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

#include "gamma_index_ivfpq.h"

#include <algorithm>
#include <stdexcept>
#include <vector>

#include "util/bitmap.h"
#include "search/error_code.h"
#include "faiss/IndexFlat.h"
#include "index/gamma_index_io.h"
#include "vector/mmap_raw_vector.h"
#include "omp.h"
#include "util/utils.h"

namespace tig_gamma {

static inline void ConvertVectorDim(size_t num, int raw_d, int d,
                                    const float *raw_vec, float *vec) {
  memset(vec, 0, num * d * sizeof(float));

#pragma omp parallel for
  for (size_t i = 0; i < num; ++i) {
    for (int j = 0; j < raw_d; ++j) {
      vec[i * d + j] = raw_vec[i * raw_d + j];
    }
  }
}

IndexIVFPQStats indexIVFPQ_stats;

REGISTER_MODEL(IVFPQ, GammaIVFPQIndex)

GammaIVFPQIndex::GammaIVFPQIndex() : indexed_vec_count_(0) {
  compaction_ = false;
  compact_bucket_no_ = 0;
  compacted_num_ = 0;
  updated_num_ = 0;
  is_trained = false;
  rt_invert_index_ptr_ = nullptr;
  invlists = nullptr;
  quantizer = nullptr;
  model_param_ = nullptr;
  opq_ = nullptr;
#ifdef PERFORMANCE_TESTING
  search_count_ = 0;
  add_count_ = 0;
#endif
}

GammaIVFPQIndex::~GammaIVFPQIndex() {
  if (rt_invert_index_ptr_) {
    delete rt_invert_index_ptr_;
    rt_invert_index_ptr_ = nullptr;
  }
  if (invlists) {
    delete invlists;
    invlists = nullptr;
  }
  if (quantizer) {
    delete quantizer;  // it will not be delete in parent class
    quantizer = nullptr;
  }
  if (opq_) {
    delete opq_;
    opq_ = nullptr;
  }

  CHECK_DELETE(model_param_);
}

GammaInvertedListScanner *GammaIVFPQIndex::GetInvertedListScanner(
    bool store_pairs, faiss::MetricType metric_type) {
  if (pq.nbits == 8) {
    return GetGammaInvertedListScanner<faiss::PQDecoder8>(store_pairs, metric_type);
  } else if (pq.nbits == 16) {
    return GetGammaInvertedListScanner<faiss::PQDecoder16>(store_pairs, metric_type);
  } else {
    return GetGammaInvertedListScanner<faiss::PQDecoderGeneric>(store_pairs, metric_type);
  }
  return nullptr;
}

template <class PQDecoder>
GammaInvertedListScanner *GammaIVFPQIndex::GetGammaInvertedListScanner(
    bool store_pairs, faiss::MetricType metric_type) {
  if (metric_type == faiss::METRIC_INNER_PRODUCT) {
    auto scanner =
        new GammaIVFPQScanner<faiss::METRIC_INNER_PRODUCT,
                              faiss::CMin<float, idx_t>, PQDecoder>(*this, store_pairs, 2);
    return scanner;
  } else if (metric_type == faiss::METRIC_L2) {
    auto scanner =
        new GammaIVFPQScanner<faiss::METRIC_L2, faiss::CMax<float, idx_t>, PQDecoder>(
            *this, store_pairs, 2);
    return scanner;
  }
  return nullptr;
}

int GammaIVFPQIndex::Init(const std::string &model_parameters, int indexing_size) {
  indexing_size_ = indexing_size;
  model_param_ = new IVFPQModelParams();
  IVFPQModelParams &ivfpq_param = *model_param_;
  if (model_parameters != "" && ivfpq_param.Parse(model_parameters.c_str())) {
    return -1;
  }
  LOG(INFO) << ivfpq_param.ToString();

  d = vector_->MetaInfo()->Dimension();

  if (d % ivfpq_param.nsubvector != 0) {
    if (!ivfpq_param.support_indivisible_nsubvector) {
      LOG(ERROR) << "Dimension [" << vector_->MetaInfo()->Dimension()
                << "] cannot divide by nsubvector [" << ivfpq_param.nsubvector
                << "]. If you really want to use this nsubvector, please set support_indivisible_nsubvector to a non-zero value";
      return -2;
    }
    d = (d / ivfpq_param.nsubvector + 1) * ivfpq_param.nsubvector;
    LOG(INFO) << "Dimension [" << vector_->MetaInfo()->Dimension()
              << "] cannot divide by nsubvector [" << ivfpq_param.nsubvector
              << "], adjusted to [" << d << "]";
  }

  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);

  nlist = ivfpq_param.ncentroids;
  if (ivfpq_param.has_hnsw == false) {
    quantizer = new faiss::IndexFlatL2(d);
    quantizer_type_ = 0;
  } else {
    faiss::IndexHNSWFlat *hnsw_flat = new faiss::IndexHNSWFlat(d, ivfpq_param.nlinks);
    hnsw_flat->hnsw.efSearch = ivfpq_param.efSearch;
    hnsw_flat->hnsw.efConstruction = ivfpq_param.efConstruction;
    hnsw_flat->hnsw.search_bounded_queue = false;
    quantizer = hnsw_flat;
    quantizer_type_ = 1;
  }

  if (ivfpq_param.has_opq) {
    if (d % ivfpq_param.opq_nsubvector != 0) {
      LOG(ERROR) << d << " % " << ivfpq_param.opq_nsubvector 
                 << " != 0, opq nsubvector should be divisible by dimension.";
      return -2; 
    }
    opq_ = new faiss::OPQMatrix(d, ivfpq_param.opq_nsubvector, d);
  }

  pq.d = d;
  pq.M = ivfpq_param.nsubvector;
  pq.nbits = ivfpq_param.nbits_per_idx;
  pq.set_derived_values();

  own_fields = false;
  quantizer_trains_alone = 0;
  clustering_index = nullptr;
  cp.niter = 10;

  code_size = pq.code_size;
  is_trained = false;
  by_residual = true;
  use_precomputed_table = 0;
  scan_table_threshold = 0;

  polysemous_training = nullptr;
  do_polysemous_training = false;
  polysemous_ht = 0;

  // if nlist is very large, 
  // the size of RTInvertIndex bucket should be smaller
  rt_invert_index_ptr_ = new realtime::RTInvertIndex(
    this->nlist, this->code_size, raw_vec->VidMgr(), raw_vec->Bitmap(), 
    ivfpq_param.bucket_init_size, ivfpq_param.bucket_max_size);

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

  metric_type_ = ivfpq_param.metric_type;
  if (metric_type_ == DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }

  this->nprobe = ivfpq_param.nprobe;
  return 0;
}

RetrievalParameters *GammaIVFPQIndex::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new IVFPQRetrievalParameters(metric_type_);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  IVFPQRetrievalParameters *retrieval_params = new IVFPQRetrievalParameters();
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
  int parallel_on_queries;

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

  return retrieval_params;
}

int GammaIVFPQIndex::Indexing() {
  if (this->is_trained) {
    LOG(INFO) << "gamma ivfpq index is already trained, skip indexing";
    return 0;
  }
  RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
  size_t vectors_count = raw_vec->MetaInfo()->Size();

  size_t num;
  if ((size_t)indexing_size_ < nlist) {
    num = nlist * 39;
    LOG(WARNING) << "Because index_size[" << indexing_size_ << "] < ncentroids[" << nlist 
                 << "], index_size becomes ncentroids * 39[" << num << "].";
  } else if ((size_t)indexing_size_ <= nlist * 256) {
    if ((size_t)indexing_size_ < nlist * 39) {
      LOG(WARNING) << "Index_size[" << indexing_size_ << "] is too small. "
                   << "The appropriate range is [ncentroids * 39, ncentroids * 256]"; 
    }
    num = (size_t)indexing_size_;
  } else {
    num = nlist * 256;
    LOG(WARNING) << "Index_size[" << indexing_size_ << "] is too big. "
                 << "The appropriate range is [ncentroids * 39, ncentroids * 256]."
                 << "index_size becomes ncentroids * 256[" << num << "].";
  }
  if (num > vectors_count) {
    LOG(ERROR) << "vector total count [" << vectors_count
                << "] less then index_size[" << num << "], failed!";
    return -1;
  }
  
  ScopeVectors headers;
  std::vector<int> lens;
  raw_vec->GetVectorHeader(0, num, headers, lens);

  // merge vectors
  int raw_d = raw_vec->MetaInfo()->Dimension();
  const uint8_t *train_raw_vec = nullptr;
  utils::ScopeDeleter<uint8_t> del_train_raw_vec;
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

  const float *train_vec = nullptr;

  if (d_ > raw_d) {
    float *vec = new float[num * d_];

    ConvertVectorDim(num, raw_d, d, (const float *)train_raw_vec, vec);

    train_vec = vec;
  } else {
    train_vec = (const float *)train_raw_vec;
  }
  
  const float *xt = nullptr;
  utils::ScopeDeleter<float> del_xt;
  if (opq_ != nullptr) {
    opq_->train(num, train_vec);
    xt = opq_->apply(num, train_vec);
    del_xt.set(xt == train_vec ? nullptr : xt);
  } else {
    xt = train_vec;
  }

  faiss::IndexIVFPQ::train(num, xt);

  if (d_ > raw_d) {
    delete[] train_vec;
  }

  LOG(INFO) << "train successed!";
  return 0;
}

static float *compute_residuals(const faiss::Index *quantizer, long n,
                                const float *x, const idx_t *list_nos) {
  size_t d = quantizer->d;
  float *residuals = new float[n * d];
  for (int i = 0; i < n; i++) {
    if (list_nos[i] < 0)
      memset(residuals + i * d, 0, sizeof(*residuals) * d);
    else
      quantizer->compute_residual(x + i * d, residuals + i * d, list_nos[i]);
  }
  return residuals;
}

int GammaIVFPQIndex::Delete(const std::vector<int64_t> &ids) {
  std::vector<int> vids(ids.begin(), ids.end());
  rt_invert_index_ptr_->Delete(vids.data(), ids.size());
  return 0;
}

int GammaIVFPQIndex::Update(const std::vector<int64_t> &ids,
                            const std::vector<const uint8_t *> &vecs) {
  int raw_d = vector_->MetaInfo()->Dimension();
  for (size_t i = 0; i < ids.size(); i++) {
    const float *vec = nullptr;
    utils::ScopeDeleter<float> del_vec;
    const float *add_vec = reinterpret_cast<const float *>(vecs[i]);
    if (d_ > raw_d) {
      float *extend_vec = new float[d_];
      ConvertVectorDim(1, raw_d, d_, add_vec, extend_vec);
      vec = (const float *)extend_vec;
      del_vec.set(vec);
    } else {
      vec = add_vec;
    }
    const float *applied_vec = nullptr;
    utils::ScopeDeleter<float> del_applied;
    if (opq_ != nullptr) {
      applied_vec = opq_->apply(1, vec);
      del_applied.set(applied_vec == vec ? nullptr : applied_vec);
    } else {
      applied_vec = vec;
    }
    idx_t idx = -1;
    quantizer->assign(1, applied_vec, &idx);

    std::vector<uint8_t> xcodes;
    xcodes.resize(code_size);
    const float *to_encode = nullptr;
    utils::ScopeDeleter<float> del_to_encode;

    if (by_residual) {
      to_encode = compute_residuals(quantizer, 1, applied_vec, &idx);
      del_to_encode.set(to_encode);
    } else {
      to_encode = applied_vec;
    }
    pq.compute_codes(to_encode, xcodes.data(), 1);
    rt_invert_index_ptr_->Update(idx, ids[i], xcodes);
  }
  updated_num_ += ids.size();
  LOG(INFO) << "update index success! size=" << ids.size()
            << ", total=" << updated_num_;

  // now check id need to do compaction
  rt_invert_index_ptr_->CompactIfNeed();
  return 0;
}

bool GammaIVFPQIndex::Add(int n, const uint8_t *vec) {
#ifdef PERFORMANCE_TESTING
  double t0 = faiss::getmillisecs();
#endif
  std::map<int, std::vector<long>> new_keys;
  std::map<int, std::vector<uint8_t>> new_codes;

  idx_t *idx;
  utils::ScopeDeleter<idx_t> del_idx;
  const float *add_vec = reinterpret_cast<const float *>(vec);
  const float *add_vec_head = nullptr;
  utils::ScopeDeleter<float> del_vec;
  int raw_d = vector_->MetaInfo()->Dimension();
  if (d_ > raw_d) {
    float *vector = new float[n * d_];
    ConvertVectorDim(n, raw_d, d, add_vec, vector);
    add_vec_head = vector;
    del_vec.set(add_vec_head);
  } else {
    add_vec_head = add_vec;
  }

  const float *applied_vec = nullptr;
  utils::ScopeDeleter<float> del_applied;
  if (opq_ != nullptr) {
    applied_vec = opq_->apply(n, add_vec_head);
    del_applied.set(applied_vec == add_vec_head ? nullptr : applied_vec);
  } else {
    applied_vec = add_vec_head;
  }

  idx_t *idx0 = new idx_t[n];
  quantizer->assign(n, applied_vec, idx0);
  idx = idx0;
  del_idx.set(idx);

  uint8_t *xcodes = new uint8_t[n * code_size];
  utils::ScopeDeleter<uint8_t> del_xcodes(xcodes);

  const float *to_encode = nullptr;
  utils::ScopeDeleter<float> del_to_encode;
  
  if (by_residual) {
    to_encode = compute_residuals(quantizer, n, applied_vec, idx);
    del_to_encode.set(to_encode);
  } else {
    to_encode = applied_vec;
  }
  pq.compute_codes(to_encode, xcodes, n);

  size_t n_ignore = 0;
  long vid = indexed_vec_count_;
  for (int i = 0; i < n; i++) {
    long key = idx[i];
    assert(key < (long)nlist);
    if (key < 0) {
      n_ignore++;
      LOG(WARNING) << "ivfpq add invalid key=" << key
                   << ", vid=" << vid;
      key = vid % nlist;
    }

    // long id = (long)(indexed_vec_count_++);
    uint8_t *code = xcodes + i * code_size;

    new_keys[key].push_back(vid++);

    size_t ofs = new_codes[key].size();
    new_codes[key].resize(ofs + code_size);
    memcpy((void *)(new_codes[key].data() + ofs), (void *)code, code_size);
  }

  /* stage 2 : add invert info to invert index */
  if (!rt_invert_index_ptr_->AddKeys(new_keys, new_codes)) {
    return false;
  }
  indexed_vec_count_ = vid;
#ifdef PERFORMANCE_TESTING
  add_count_ += n;
  if (add_count_ >= 10000) {
    double t1 = faiss::getmillisecs();
    LOG(INFO) << "Add time [" << (t1 - t0) / n << "]ms, count "
              << indexed_vec_count_;
    // rt_invert_index_ptr_->PrintBucketSize();
    add_count_ = 0;
  }
#endif
  return true;
}

int GammaIVFPQIndex::Search(RetrievalContext *retrieval_context, int n,
                            const uint8_t *x, int k, float *distances,
                            idx_t *labels) {
  IVFPQRetrievalParameters *retrieval_params =
      dynamic_cast<IVFPQRetrievalParameters *>(
          retrieval_context->RetrievalParams());

  utils::ScopeDeleter1<IVFPQRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params = new IVFPQRetrievalParameters();
    del_params.set(retrieval_params);
  }

  GammaSearchCondition *condition =
      dynamic_cast<GammaSearchCondition *>(retrieval_context);
  if (condition->brute_force_search == true || is_trained == false) {
    // reset retrieval_params
    delete retrieval_context->RetrievalParams();
    retrieval_context->retrieval_params_ = new FlatRetrievalParameters(
        retrieval_params->ParallelOnQueries(), retrieval_params->GetDistanceComputeType());
    int ret =
        GammaFLATIndex::Search(retrieval_context, n, x, k, distances, labels);
    return ret;
  }

  int nprobe = this->nprobe;
  if (retrieval_params->Nprobe() > 0 &&
      (size_t)retrieval_params->Nprobe() <= this->nlist) {
    nprobe = retrieval_params->Nprobe();
  } else {
    retrieval_params->SetNprobe(this->nprobe);
  }

  const float *xq = reinterpret_cast<const float *>(x);
  const float *applied_xq = nullptr;
  utils::ScopeDeleter<float> del_applied;
  if (opq_ == nullptr) {
    applied_xq = xq;
  } else {
    applied_xq = opq_->apply(n, xq);
    del_applied.set(applied_xq == xq ? nullptr : applied_xq);
  }

  std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
  std::unique_ptr<float[]> coarse_dis(new float[n * nprobe]);

  quantizer->search(n, applied_xq, nprobe, coarse_dis.get(), idx.get());
  this->invlists->prefetch_lists(idx.get(), n * nprobe);

  search_preassigned(retrieval_context, n, xq, applied_xq, k, idx.get(), coarse_dis.get(),
                     distances, labels, nprobe, false);
  return 0;
}

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
size_t scan_one_list(GammaInvertedListScanner *scanner, idx_t key,
                     float coarse_dis_i, float *simi, idx_t *idxi, int k,
                     idx_t nlist, faiss::InvertedLists *invlists,
                     bool store_pairs, bool ivf_flat,
                     MemoryRawVector *mem_raw_vec = nullptr) {
  if (key < 0) {
    // not enough centroids for multiprobe
    return 0;
  }
  if (key >= (idx_t)nlist) {
    LOG(INFO) << "Invalid key=" << key << ", nlist=" << nlist;
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

  if (ivf_flat) {
    codes = reinterpret_cast<uint8_t *>(mem_raw_vec);
  } else {
    faiss::InvertedLists::ScopedCodes scodes(invlists, key);
    codes = scodes.get();
  }
  scanner->scan_codes(list_size, codes, ids, simi, idxi, k);

  return list_size;
};

void compute_dis(int k, const float *xi, float *simi, idx_t *idxi,
                 float *recall_simi, idx_t *recall_idxi, int recall_num,
                 bool has_rank, faiss::MetricType metric_type,
                 VectorReader *vec, RetrievalContext *retrieval_context) {
  if (has_rank == true) {
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
      const float *vec = reinterpret_cast<const float *>(scope_vecs.Get(j));
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
    int i = 0;
    reorder_result(metric_type, recall_num, recall_simi, recall_idxi);
    for (int j = 0; j < recall_num; j++) {
      if (recall_idxi[j] == -1) continue;
      float dis = recall_simi[j];

      if (retrieval_context->IsSimilarScoreValid(dis) == true) {
        simi[i] = dis;
        idxi[i] = recall_idxi[j];
        ++i;
      }
      if (i >= k) break;
    }
  }
}

}  // namespace

void GammaIVFPQIndex::search_preassigned(
    RetrievalContext *retrieval_context, int n, const float *x, const float *applied_x, int k,
    const idx_t *keys, const float *coarse_dis, float *distances, idx_t *labels,
    int nprobe, bool store_pairs, const faiss::IVFSearchParameters *params) {
  int raw_d = vector_->MetaInfo()->Dimension();
  // for opq, rerank need raw vector
  float *vec_q = nullptr;
  utils::ScopeDeleter<float> del_vec_q;
  if (d > raw_d) {
    float *vec = new float[n * d];

    ConvertVectorDim(n, raw_d, d, x, vec);

    vec_q = vec;
    del_vec_q.set(vec_q);
  } else {
    vec_q = const_cast<float *>(x);
  }
  
  float *vec_applied_q = nullptr;
  utils::ScopeDeleter<float> del_applied_q;
  if (d > raw_d) {
    float *applied_vec = new float[n * d];

    ConvertVectorDim(n, raw_d, d, applied_x, applied_vec);

    vec_applied_q = applied_vec;
    del_applied_q.set(vec_applied_q);
  } else {
    vec_applied_q = const_cast<float *>(applied_x);
  }

  GammaSearchCondition *context =
      dynamic_cast<GammaSearchCondition *>(retrieval_context);
  IVFPQRetrievalParameters *retrieval_params =
      dynamic_cast<IVFPQRetrievalParameters *>(
          retrieval_context->RetrievalParams());
  utils::ScopeDeleter1<IVFPQRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params = new IVFPQRetrievalParameters();
    del_params.set(retrieval_params);
  }

  faiss::MetricType metric_type;
  if (retrieval_params->GetDistanceComputeType() ==
      DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }
  long max_codes = params ? params->max_codes : this->max_codes;

  if (k <= 0) {
    LOG(WARNING) << "topK is should greater then 0, topK = " << k;
    return;
  }
  size_t ndis = 0;

  using HeapForIP = faiss::CMin<float, idx_t>;
  using HeapForL2 = faiss::CMax<float, idx_t>;

  int recall_num = retrieval_params->RecallNum();
  if (recall_num < k) {
    recall_num = k;
  }

  float *recall_distances = new float[n * recall_num];
  idx_t *recall_labels = new idx_t[n * recall_num];
  utils::ScopeDeleter<float> del1(recall_distances);
  utils::ScopeDeleter<idx_t> del2(recall_labels);

#ifdef PERFORMANCE_TESTING
  retrieval_context->GetPerfTool().Perf("search prepare");
#endif

  bool parallel_mode = retrieval_params->ParallelOnQueries() ? 0 : 1;

  // don't start parallel section if single query
  bool do_parallel = omp_get_max_threads() >= 2 && (parallel_mode == 0 ? n > 1 : nprobe > 1);

#pragma omp parallel if (do_parallel) reduction(+ : ndis)
  {
    GammaInvertedListScanner *scanner =
        GetInvertedListScanner(store_pairs, metric_type);
    utils::ScopeDeleter1<GammaInvertedListScanner> del(scanner);
    scanner->set_search_context(retrieval_context);

    if (parallel_mode == 0) {  // parallelize over queries
#pragma omp for
      for (int i = 0; i < n; i++) {
        // loop over queries
        const float *xi = vec_applied_q + i * d;
        scanner->set_query(xi);
        float *simi = distances + i * k;
        idx_t *idxi = labels + i * k;

        float *recall_simi = recall_distances + i * recall_num;
        idx_t *recall_idxi = recall_labels + i * recall_num;

        init_result(metric_type, k, simi, idxi);
        init_result(metric_type, recall_num, recall_simi, recall_idxi);

        long nscan = 0;

        // loop over probes
        for (int ik = 0; ik < nprobe; ik++) {
          nscan += scan_one_list(
              scanner, keys[i * nprobe + ik], coarse_dis[i * nprobe + ik],
              recall_simi, recall_idxi, recall_num, this->nlist, this->invlists,
              store_pairs, false);

          if (max_codes && nscan >= max_codes) break;
        }

        ndis += nscan;
        compute_dis(k, vec_q + i * d, simi, idxi, recall_simi, recall_idxi, recall_num,
                    context->has_rank, metric_type, vector_, retrieval_context);
      }       // parallel for
    } else {  // parallelize over inverted lists
      std::vector<idx_t> local_idx(recall_num);
      std::vector<float> local_dis(recall_num);

      for (int i = 0; i < n; i++) {
        const float *xi = vec_applied_q + i * d;
        scanner->set_query(xi);

        init_result(metric_type, recall_num, local_dis.data(),
                    local_idx.data());

#pragma omp for schedule(dynamic)
        for (int ik = 0; ik < nprobe; ik++) {
          ndis += scan_one_list(
              scanner, keys[i * nprobe + ik], coarse_dis[i * nprobe + ik],
              local_dis.data(), local_idx.data(), recall_num, this->nlist,
              this->invlists, store_pairs, false);

          // can't do the test on max_codes
        }

        // merge thread-local results

        float *simi = distances + i * k;
        idx_t *idxi = labels + i * k;

        float *recall_simi = recall_distances + i * recall_num;
        idx_t *recall_idxi = recall_labels + i * recall_num;

#pragma omp single
        {
          init_result(metric_type, k, simi, idxi);
          init_result(metric_type, recall_num, recall_simi, recall_idxi);
        }

#pragma omp barrier
#pragma omp critical
        {
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
#pragma omp barrier
#pragma omp single
        {
#ifdef PERFORMANCE_TESTING
          retrieval_context->GetPerfTool().Perf("coarse");
#endif
          compute_dis(k, vec_q + i * d, simi, idxi, recall_simi, recall_idxi, recall_num,
                      context->has_rank, metric_type, vector_,
                      retrieval_context);

#ifdef PERFORMANCE_TESTING
          retrieval_context->GetPerfTool().Perf("reorder");
#endif
        }
      }
    }
  }  // parallel

#ifdef PERFORMANCE_TESTING
  std::string compute_msg = "compute ";
  compute_msg += std::to_string(n);
  retrieval_context->GetPerfTool().Perf(compute_msg);
#endif
}  // namespace tig_gamma

void GammaIVFPQIndex::copy_subset_to(faiss::IndexIVF &other, int subset_type,
                                     idx_t a1, idx_t a2) const {
  using ScopedIds = faiss::InvertedLists::ScopedIds;
  using ScopedCodes = faiss::InvertedLists::ScopedCodes;
  FAISS_THROW_IF_NOT(nlist == other.nlist);
  FAISS_THROW_IF_NOT(code_size == other.code_size);
  // FAISS_THROW_IF_NOT(other.direct_map.no());
  FAISS_THROW_IF_NOT_FMT(
      subset_type == 0 || subset_type == 1 || subset_type == 2,
      "subset type %d not implemented", subset_type);

  int accu_n = 0;

  faiss::InvertedLists *oivf = other.invlists;

  for (size_t list_no = 0; list_no < nlist; list_no++) {
    size_t n = invlists->list_size(list_no);
    ScopedIds ids_in(invlists, list_no);

    if (subset_type == 0) {
      for (size_t i = 0; i < n; i++) {
        idx_t id = ids_in[i];
        if (a1 <= id && id < a2) {
          oivf->add_entry(list_no, invlists->get_single_id(list_no, i),
                          ScopedCodes(invlists, list_no, i).get());
          other.ntotal++;
        }
      }
    } else if (subset_type == 1) {
      for (size_t i = 0; i < n; i++) {
        idx_t id = ids_in[i];
        if (id % a1 == a2) {
          oivf->add_entry(list_no, invlists->get_single_id(list_no, i),
                          ScopedCodes(invlists, list_no, i).get());
          other.ntotal++;
        }
      }
    }
    accu_n += n;
  }
  // FAISS_ASSERT(accu_n == indexed_vec_count_);
}

string IVFPQToString(const faiss::IndexIVFPQ *ivpq, const faiss::VectorTransform *vt) {
  std::stringstream ss;
  ss << "d=" << ivpq->d << ", ntotal=" << ivpq->ntotal
     << ", is_trained=" << ivpq->is_trained
     << ", metric_type=" << ivpq->metric_type << ", nlist=" << ivpq->nlist
     << ", nprobe=" << ivpq->nprobe << ", by_residual=" << ivpq->by_residual
     << ", code_size=" << ivpq->code_size << ", pq: d=" << ivpq->pq.d
     << ", M=" << ivpq->pq.M << ", nbits=" << ivpq->pq.nbits;

  faiss::IndexHNSWFlat *hnsw_flat = dynamic_cast<faiss::IndexHNSWFlat *>(ivpq->quantizer);
  if (hnsw_flat) {
    ss << ", hnsw: efSearch=" << hnsw_flat->hnsw.efSearch
       << ", efConstruction=" << hnsw_flat->hnsw.efConstruction
       << ", search_bounded_queue=" << hnsw_flat->hnsw.search_bounded_queue;
  }

  const faiss::OPQMatrix *opq = dynamic_cast<const faiss::OPQMatrix *>(vt);
  if (opq) {
    ss << ", opq: d_in=" << opq->d_in << ", d_out=" << opq->d_out << ", M=" << opq->M;
  }
  return ss.str();
}

int GammaIVFPQIndex::Dump(const std::string &dir) {
  if (!this->is_trained) {
    LOG(INFO) << "gamma index is not trained, skip dumping";
    return 0;
  }
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  string index_dir = dir + "/" + index_name;
  if (utils::make_dir(index_dir.c_str())) {
    LOG(ERROR) << "mkdir error, index dir=" << index_dir;
    return IO_ERR;
  }

  string index_file = index_dir + "/ivfpq.index";
  faiss::IOWriter *f = new FileIOWriter(index_file.c_str());
  utils::ScopeDeleter1<FileIOWriter> del((FileIOWriter *)f);
  const IndexIVFPQ *ivpq = static_cast<const IndexIVFPQ *>(this);
  uint32_t h = faiss::fourcc("IwPQ");
  WRITE1(h);
  tig_gamma::write_ivf_header(ivpq, f);
  WRITE1(ivpq->by_residual);
  WRITE1(ivpq->code_size);
  tig_gamma::write_product_quantizer(&ivpq->pq, f);

  if (opq_ != nullptr)
    write_opq(opq_, f);

  if (WriteInvertedLists(f, rt_invert_index_ptr_)) {
    LOG(ERROR) << "write invert list error, index name=" << index_name;
    return INTERNAL_ERR;
  }

  LOG(INFO) << "dump:" << IVFPQToString(ivpq, opq_) << ", indexed count=" << indexed_vec_count_;
  return 0;
}

int GammaIVFPQIndex::Load(const std::string &index_dir) {
  std::string index_name = vector_->MetaInfo()->AbsoluteName();
  string index_file = index_dir + "/" + index_name + "/ivfpq.index";
  if (!utils::file_exist(index_file)) {
    LOG(INFO) << index_file << " isn't existed, skip loading";
    return 0;  // it should train again after load
  }

  faiss::IOReader *f = new FileIOReader(index_file.c_str());
  utils::ScopeDeleter1<FileIOReader> del((FileIOReader *)f);
  uint32_t h;
  READ1(h);
  assert(h == faiss::fourcc("IwPQ"));
  IndexIVFPQ *ivpq = static_cast<IndexIVFPQ *>(this);
  tig_gamma::read_ivf_header(ivpq, f, nullptr);  // not legacy
  READ1(ivpq->by_residual);
  READ1(ivpq->code_size);
  tig_gamma::read_product_quantizer(&ivpq->pq, f);

  faiss::IndexHNSWFlat *hnsw_flat = dynamic_cast<faiss::IndexHNSWFlat *>(ivpq->quantizer);
  if(hnsw_flat) {
    hnsw_flat->hnsw.search_bounded_queue = false;
    quantizer_type_ = 1;
  }
  if(opq_) {
    read_opq(opq_, f);
  }

  int ret = ReadInvertedLists(f, rt_invert_index_ptr_, indexed_vec_count_);
  if (ret == FORMAT_ERR) {
    indexed_vec_count_ = 0;
    LOG(INFO) << "unsupported inverted list format, it need rebuilding!";
  } else if (ret == 0) {
    // if (indexed_vec_count_ < 0 ||
    //     indexed_vec_count_ > (int)vector_->MetaInfo()->size_) {
    //   LOG(ERROR) << "invalid indexed count [" << indexed_vec_count_ 
    //              << "] vector size [" << vector_->MetaInfo()->size_ << "]";
    //   return INTERNAL_ERR;
    // }
    // precomputed table not stored. It is cheaper to recompute it
    ivpq->use_precomputed_table = 0;
    if (ivpq->by_residual) ivpq->precompute_table();
    LOG(INFO) << "load: " << IVFPQToString(ivpq, opq_)
              << ", indexed vector count=" << indexed_vec_count_;
  } else {
    LOG(ERROR) << "read invert list error, index name=" << index_name;
    return INTERNAL_ERR;
  }
  if (ivpq->metric_type == faiss::METRIC_INNER_PRODUCT) {
    metric_type_ = DistanceComputeType::INNER_PRODUCT;
  } else {
    metric_type_ = DistanceComputeType::L2;
  }
  assert(this->is_trained);
  return indexed_vec_count_;
}

}  // namespace tig_gamma
