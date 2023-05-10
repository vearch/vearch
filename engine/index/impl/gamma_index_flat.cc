/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This faiss source code is licensed under the MIT license.
 * https://github.com/facebookresearch/faiss/blob/master/LICENSE
 *
 *
 * The works below are modified based on faiss:
 * 1. Add the numeric field and bitmap filters in the process of searching
 *
 * Modified works copyright 2019 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#include "gamma_index_flat.h"

#include "omp.h"
#include "vector/memory_raw_vector.h"
#include "vector/mmap_raw_vector.h"

using idx_t = faiss::Index::idx_t;

namespace tig_gamma {

REGISTER_MODEL(FLAT, GammaFLATIndex);

struct FLATModelParams {
  DistanceComputeType metric_type;

  FLATModelParams() { metric_type = DistanceComputeType::INNER_PRODUCT; }

  int Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      LOG(ERROR) << "parse FLAT retrieval parameters error: " << str;
      return -1;
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
    }
    return 0;
  }
};

GammaFLATIndex::GammaFLATIndex() {}

GammaFLATIndex::~GammaFLATIndex() {}

int GammaFLATIndex::Init(const std::string &model_parameters,
                         int indexing_size) {
  indexing_size_ = indexing_size;
  auto raw_vec_type = dynamic_cast<MemoryRawVector *>(vector_);
  if (raw_vec_type == nullptr) {
    LOG(ERROR) << "FLAT can only work in memory only mode";
    return -1;
  }
  FLATModelParams flat_param;
  if (model_parameters != "" && flat_param.Parse(model_parameters.c_str())) {
    return -1;
  }
  metric_type_ = flat_param.metric_type;
  return 0;
}

RetrievalParameters *GammaFLATIndex::Parse(const std::string &parameters) {
  if (parameters == "") {
    return new FlatRetrievalParameters(metric_type_);
  }

  utils::JsonParser jp;
  if (jp.Parse(parameters.c_str())) {
    LOG(ERROR) << "parse retrieval parameters error: " << parameters;
    return nullptr;
  }

  std::string metric_type;
  DistanceComputeType type = metric_type_;
  if (!jp.GetString("metric_type", metric_type)) {
    if (strcasecmp("L2", metric_type.c_str()) &&
        strcasecmp("InnerProduct", metric_type.c_str())) {
      LOG(ERROR) << "invalid metric_type = " << metric_type
                 << ", so use default value.";
    }

    if (!strcasecmp("L2", metric_type.c_str())) {
      type = DistanceComputeType::L2;
    } else {
      type = DistanceComputeType::INNER_PRODUCT;
    }
  }

  int parallel_on_queries = 1;
  jp.GetInt("parallel_on_queries", parallel_on_queries);

  FlatRetrievalParameters *retrieval_params = new FlatRetrievalParameters(
      parallel_on_queries == 0 ? false : true, type);

  return retrieval_params;
}

int GammaFLATIndex::Indexing() { return 0; }

bool GammaFLATIndex::Add(int n, const uint8_t *vec) { return true; }

int GammaFLATIndex::Search(RetrievalContext *retrieval_context, int n,
                           const uint8_t *x, int k, float *distances,
                           int64_t *labels) {
  FlatRetrievalParameters *retrieval_params =
      dynamic_cast<FlatRetrievalParameters *>(
          retrieval_context->RetrievalParams());
  utils::ScopeDeleter1<FlatRetrievalParameters> del_params;
  if (retrieval_params == nullptr) {
    retrieval_params =
        new FlatRetrievalParameters(true, DistanceComputeType::L2);
    del_params.set(retrieval_params);
  }

  auto raw_vec = dynamic_cast<RawVector *>(vector_);
  if (raw_vec == nullptr) {
    LOG(ERROR) << "raw vector is null";
    return -1;
  }

  const float *xq = reinterpret_cast<const float *>(x);
  if (xq == nullptr) {
    LOG(ERROR) << "search feature is null";
    return -1;
  }

  int num_vectors = vector_->MetaInfo()->Size();

  int d = vector_->MetaInfo()->Dimension();

  faiss::MetricType metric_type;
  if (retrieval_params->GetDistanceComputeType() ==
      DistanceComputeType::INNER_PRODUCT) {
    metric_type = faiss::METRIC_INNER_PRODUCT;
  } else {
    metric_type = faiss::METRIC_L2;
  }
  using HeapForIP = faiss::CMin<float, idx_t>;
  using HeapForL2 = faiss::CMax<float, idx_t>;

  {
    // we must obtain the num of threads in *THE* parallel area.
    int num_threads = omp_get_max_threads();

    /*****************************************************
     * Depending on parallel_mode, there are two possible ways
     * to organize the search. Here we define local functions
     * that are in common between the two
     ******************************************************/

    auto init_result = [&](int k, float *simi, idx_t *idxi) {
      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        faiss::heap_heapify<HeapForIP>(k, simi, idxi);
      } else {
        faiss::heap_heapify<HeapForL2>(k, simi, idxi);
      }
    };

    auto reorder_result = [&](int k, float *simi, idx_t *idxi) {
      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        faiss::heap_reorder<HeapForIP>(k, simi, idxi);
      } else {
        faiss::heap_reorder<HeapForL2>(k, simi, idxi);
      }
    };

    auto search_impl = [&](const float *xi, int start_vid, int nsearch,
                           float *simi, idx_t *idxi, int k) {
      if (metric_type == faiss::METRIC_INNER_PRODUCT) {
        for (int vid = start_vid; vid < start_vid + nsearch; ++vid) {
          if (!retrieval_context->IsValid(vid)) {
            continue;
          }

          ScopeVector scope_vec;
          raw_vec->GetVector(vid, scope_vec);
          const float *yi = reinterpret_cast<const float *>(scope_vec.Get());
          float dis = -2;
          if (yi != nullptr) {
            dis = faiss::fvec_inner_product(xi, yi, d);
          }

          if (!retrieval_context->IsSimilarScoreValid(dis)) {
            continue;
          }

          if (HeapForIP::cmp(simi[0], dis)) {
            faiss::heap_pop<HeapForIP>(k, simi, idxi);
            faiss::heap_push<HeapForIP>(k, simi, idxi, dis, vid);
          }
        }
      } else {
        for (int vid = start_vid; vid < start_vid + nsearch; ++vid) {
          if (!retrieval_context->IsValid(vid)) {
            continue;
          }

          ScopeVector scope_vec;
          raw_vec->GetVector(vid, scope_vec);
          const float *yi = reinterpret_cast<const float *>(scope_vec.Get());
          float dis = -2;
          if (yi != nullptr) {
            dis = faiss::fvec_L2sqr(xi, yi, d);
          }

          if (!retrieval_context->IsSimilarScoreValid(dis)) {
            continue;
          }

          if (HeapForL2::cmp(simi[0], dis)) {
            faiss::heap_pop<HeapForL2>(k, simi, idxi);
            faiss::heap_push<HeapForL2>(k, simi, idxi, dis, vid);
          }
        }
      }
    };
    bool parallel_on_queries =
        (retrieval_params->ParallelOnQueries() == true) && (n > 1);

    if (parallel_on_queries) {  // parallelize over queries
#pragma omp for
      for (int i = 0; i < n; i++) {
        const float *xi = xq + i * d;

        float *simi = distances + i * k;
        idx_t *idxi = (idx_t *)labels + i * k;

        init_result(k, simi, idxi);

        search_impl(xi, 0, num_vectors, simi, idxi, k);

        reorder_result(k, simi, idxi);
      }
    } else {  // parallelize over vectors

      size_t num_vectors_per_thread = num_vectors / num_threads;

      for (int i = 0; i < n; i++) {
        const float *xi = xq + i * d;

        // merge thread-local results
        float *simi = distances + i * k;
        idx_t *idxi = (idx_t *)labels + i * k;
        init_result(k, simi, idxi);

#pragma omp parallel for schedule(dynamic)
        for (int ik = 0; ik < num_threads; ik++) {
          std::vector<idx_t> local_idx(k);
          std::vector<float> local_dis(k);
          init_result(k, local_dis.data(), local_idx.data());

          size_t ny = num_vectors_per_thread;

          if (ik == num_threads - 1) {
            ny += num_vectors % num_threads;  // the rest
          }

          int offset = ik * num_vectors_per_thread;

          search_impl(xi, offset, ny, local_dis.data(), local_idx.data(), k);

#pragma omp critical
          {
            if (metric_type == faiss::METRIC_INNER_PRODUCT) {
              faiss::heap_addn<HeapForIP>(k, simi, idxi, local_dis.data(),
                                          local_idx.data(), k);
            } else {
              faiss::heap_addn<HeapForL2>(k, simi, idxi, local_dis.data(),
                                          local_idx.data(), k);
            }
          }
        }
        reorder_result(k, simi, idxi);
      }
    }
  }  // parallel

#ifdef PERFORMANCE_TESTING
  std::string compute_msg = "flat compute ";
  compute_msg += std::to_string(n);
  retrieval_context->GetPerfTool().Perf(compute_msg);
#endif  // PERFORMANCE_TESTING
  return 0;
}

long GammaFLATIndex::GetTotalMemBytes() { return 0; }

int GammaFLATIndex::Update(const std::vector<idx_t> &ids,
                           const std::vector<const uint8_t *> &vecs) {
  return 0;
}

int GammaFLATIndex::Dump(const std::string &dir) { return 0; }

int GammaFLATIndex::Load(const std::string &index_dir) { return 0; }

}  // namespace tig_gamma
