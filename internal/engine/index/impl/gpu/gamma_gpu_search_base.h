/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/utils/Heap.h>

#include <functional>

#include "gamma_gpu_index_base.h"

namespace vearch {
namespace gpu {

/**
 * Common GPU search functionality for different index types
 */
template <typename CPUIndexType, typename GPURetrievalParamsType>
class GammaGPUSearchBase : public GammaGPUIndexBase<CPUIndexType> {
 public:
  using GammaGPUIndexBase<CPUIndexType>::ParseFilters;
  using GammaGPUIndexBase<CPUIndexType>::FilteredByRangeFilter;
  using GammaGPUIndexBase<CPUIndexType>::FilteredByTermFilter;
  using GammaGPUIndexBase<CPUIndexType>::kMaxReqNum;
  using GammaGPUIndexBase<CPUIndexType>::kMaxRecallNum;
  using GammaGPUIndexBase<CPUIndexType>::search_queue_;
  using GammaGPUIndexBase<CPUIndexType>::gpu_threads_;
  using GammaGPUIndexBase<CPUIndexType>::d_;
  using GammaGPUIndexBase<CPUIndexType>::metric_type_;
  using GammaGPUIndexBase<CPUIndexType>::vector_;

 protected:
  /**
   * Common search implementation with filter support
   */
  int CommonSearch(RetrievalContext *retrieval_context, int n, const uint8_t *x,
                   int k, float *distances, long *labels, int default_nprobe,
                   size_t nlist, bool enable_rerank = false) {
    if (gpu_threads_.size() == 0) {
      LOG(ERROR) << "gpu index not indexed!";
      return -1;
    }

    if (n > kMaxReqNum) {
      LOG(ERROR) << "req num [" << n << "] should not larger than ["
                 << kMaxReqNum << "]";
      return -1;
    }

    GPURetrievalParamsType *retrieval_params =
        dynamic_cast<GPURetrievalParamsType *>(
            retrieval_context->RetrievalParams());
    utils::ScopeDeleter1<GPURetrievalParamsType> del_params;
    if (retrieval_params == nullptr) {
      retrieval_params = CreateDefaultRetrievalParams(default_nprobe);
      del_params.set(retrieval_params);
    }

    const float *xq = reinterpret_cast<const float *>(x);
    if (xq == nullptr) {
      LOG(ERROR) << "search feature is null";
      return -1;
    }

    RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
    int raw_d = raw_vec->MetaInfo()->Dimension();
    const float *vec_q = xq;

    // Get recall number and rerank flag
    int recall_num = GetRecallNum(retrieval_params, k, enable_rerank);
    bool rerank = enable_rerank && (recall_num >= k);

    if (recall_num > kMaxRecallNum) {
      LOG(ERROR) << "topK num [" << recall_num << "] should not larger than ["
                 << kMaxRecallNum << "]";
      return -1;
    }

    // Get nprobe
    int nprobe = GetNprobe(retrieval_params, default_nprobe, nlist);

    std::vector<float> D(n * recall_num);
    std::vector<long> I(n * recall_num);

#ifdef PERFORMANCE_TESTING
    if (retrieval_context->GetPerfTool()) {
      retrieval_context->GetPerfTool()->Perf("GPUSearch prepare");
    }
#endif

    GPUSearchItem *item =
        new GPUSearchItem(n, vec_q, recall_num, D.data(), I.data(), nprobe);

    search_queue_.enqueue(item);
    item->WaitForDone();
    delete item;

#ifdef PERFORMANCE_TESTING
    if (retrieval_context->GetPerfTool()) {
      retrieval_context->GetPerfTool()->Perf("GPU thread");
    }
#endif

    // Apply filters and compute final results
    return ApplyFiltersAndCompute(retrieval_context, retrieval_params, n, k, xq,
                                  raw_d, recall_num, rerank, D, I, distances,
                                  labels);
  }

 private:
  /**
   * Apply filters and compute final search results
   */
  int ApplyFiltersAndCompute(RetrievalContext *retrieval_context,
                             GPURetrievalParamsType *retrieval_params, int n,
                             int k, const float *xq, int raw_d, int recall_num,
                             bool rerank, std::vector<float> &D,
                             std::vector<long> &I, float *distances,
                             long *labels) {
    bool right_filter = false;
    SearchCondition *condition =
        dynamic_cast<SearchCondition *>(retrieval_context);

    std::vector<enum DataType> range_filter_types(
        condition->range_filters.size());
    std::vector<std::vector<std::string>> all_term_items(
        condition->term_filters.size());

    if (!ParseFilters(condition, range_filter_types, all_term_items)) {
      right_filter = true;
    }

    // set filter
    auto is_filterable = [&](long vid) -> bool {
      int docid = vid;
      return (retrieval_context->IsValid(vid) == false) ||
             (right_filter &&
              (FilteredByRangeFilter(condition, range_filter_types, docid) ||
               FilteredByTermFilter(condition, all_term_items, docid)));
    };

    using HeapForIP = faiss::CMin<float, faiss::idx_t>;
    using HeapForL2 = faiss::CMax<float, faiss::idx_t>;

    auto init_result = [&](int topk, float *simi, faiss::idx_t *idxi) {
      if (retrieval_params->GetDistanceComputeType() ==
          DistanceComputeType::INNER_PRODUCT) {
        faiss::heap_heapify<HeapForIP>(topk, simi, idxi);
      } else {
        faiss::heap_heapify<HeapForL2>(topk, simi, idxi);
      }
    };

    auto reorder_result = [&](int topk, float *simi, faiss::idx_t *idxi) {
      if (retrieval_params->GetDistanceComputeType() ==
          DistanceComputeType::INNER_PRODUCT) {
        faiss::heap_reorder<HeapForIP>(topk, simi, idxi);
      } else {
        faiss::heap_reorder<HeapForL2>(topk, simi, idxi);
      }
    };

    std::function<void(std::vector<const uint8_t *>)> compute_vec;

    if (rerank == true) {
      compute_vec = [&](std::vector<const uint8_t *> vecs) {
        for (int i = 0; i < n; ++i) {
          const float *xi = xq + i * d_;  // query

          float *simi = distances + i * k;
          long *idxi = labels + i * k;
          init_result(k, simi, idxi);

          for (int j = 0; j < recall_num; ++j) {
            long vid = I[i * recall_num + j];
            if (vid < 0) {
              continue;
            }

            if (is_filterable(vid) == true) {
              continue;
            }
            const float *vec =
                reinterpret_cast<const float *>(vecs[i * recall_num + j]);
            float dist = -1;
            if (retrieval_params->GetDistanceComputeType() ==
                DistanceComputeType::INNER_PRODUCT) {
              dist = faiss::fvec_inner_product(xi, vec, raw_d);
            } else {
              dist = faiss::fvec_L2sqr(xi, vec, raw_d);
            }

            if (retrieval_context->IsSimilarScoreValid(dist) == true) {
              if (retrieval_params->GetDistanceComputeType() ==
                  DistanceComputeType::INNER_PRODUCT) {
                if (HeapForIP::cmp(simi[0], dist)) {
                  faiss::heap_pop<HeapForIP>(k, simi, idxi);
                  faiss::heap_push<HeapForIP>(k, simi, idxi, dist, vid);
                }
              } else {
                if (HeapForL2::cmp(simi[0], dist)) {
                  faiss::heap_pop<HeapForL2>(k, simi, idxi);
                  faiss::heap_push<HeapForL2>(k, simi, idxi, dist, vid);
                }
              }
            }
          }
          reorder_result(k, simi, idxi);
        }  // parallel
      };
    } else {
      compute_vec = [&](std::vector<const uint8_t *> vecs) {
        for (int i = 0; i < n; ++i) {
          float *simi = distances + i * k;
          long *idxi = labels + i * k;
          int idx = 0;
          memset(simi, -1, sizeof(float) * k);
          memset(idxi, -1, sizeof(long) * k);

          for (int j = 0; j < recall_num; ++j) {
            long vid = I[i * recall_num + j];
            if (vid < 0) {
              continue;
            }

            if (is_filterable(vid) == true) {
              continue;
            }

            float dist = D[i * recall_num + j];

            if (retrieval_context->IsSimilarScoreValid(dist) == true) {
              simi[idx] = dist;
              idxi[idx] = vid;
              idx++;
            }
            if (idx >= k) break;
          }
        }
      };
    }

    std::function<void()> compute_dis;

    if (rerank == true) {
      // calculate inner product for selected possible vectors
      compute_dis = [&]() {
        RawVector *raw_vec = dynamic_cast<RawVector *>(vector_);
        ScopeVectors scope_vecs;
        if (raw_vec->Gets(I, scope_vecs)) {
          LOG(ERROR) << "get raw vector error!";
          return;
        }
        compute_vec(scope_vecs.Get());
      };
    } else {
      compute_dis = [&]() {
        std::vector<const uint8_t *> vecs;
        compute_vec(vecs);
      };
    }

    compute_dis();

#ifdef PERFORMANCE_TESTING
    if (retrieval_context->GetPerfTool()) {
      retrieval_context->GetPerfTool()->Perf("reorder");
    }
#endif
    return 0;
  }

  // Abstract methods to be implemented by derived classes
  virtual GPURetrievalParamsType *CreateDefaultRetrievalParams(
      int default_nprobe) = 0;
  virtual int GetRecallNum(GPURetrievalParamsType *params, int k,
                           bool enable_rerank) = 0;
  virtual int GetNprobe(GPURetrievalParamsType *params, int default_nprobe,
                        size_t nlist) = 0;
};

}  // namespace gpu
}  // namespace vearch
