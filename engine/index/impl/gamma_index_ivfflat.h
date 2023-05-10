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

#ifndef GAMMA_INDEX_IVF_FLAT_H_
#define GAMMA_INDEX_IVF_FLAT_H_

#pragma once

#include <faiss/IndexIVFFlat.h>
#include <faiss/utils/distances.h>
#include "gamma_scanner.h"
#include "realtime/realtime_invert_index.h"

namespace tig_gamma {

template <faiss::MetricType metric, class C>
struct GammaIVFFlatScanner1 : GammaInvertedListScanner {
  size_t d;
  GammaIVFFlatScanner1(size_t d) : d(d) {}

  const float *xi;
  void set_query(const float *query) override { this->xi = query; }

  idx_t list_no;
  void set_list(idx_t list_no, float /* coarse_dis */) override {
    this->list_no = list_no;
  }

  float distance_to_code(const uint8_t *code) const override {
    const float *yj = (float *)code;
    float dis = metric == faiss::METRIC_INNER_PRODUCT
                    ? faiss::fvec_inner_product(xi, yj, d)
                    : faiss::fvec_L2sqr(xi, yj, d);
    return dis;
  }

  size_t scan_codes(size_t list_size, const uint8_t *codes, const idx_t *ids,
                    float *simi, idx_t *idxi, size_t k) const override {
    const float *list_vecs = (const float *)codes;
    size_t nup = 0;
    for (size_t j = 0; j < list_size; j++) {
      if (ids[j] & realtime::kDelIdxMask) {
        continue;
      }
      idx_t vid = ids[j] & realtime::kRecoverIdxMask;
      if (!retrieval_context_->IsValid(vid)) {
        continue;
      }
      const float *yj = list_vecs + d * j;
      float dis = metric == faiss::METRIC_INNER_PRODUCT
                      ? faiss::fvec_inner_product(xi, yj, d)
                      : faiss::fvec_L2sqr(xi, yj, d);
      if (retrieval_context_->IsSimilarScoreValid(dis) && C::cmp(simi[0], dis)) {
        faiss::heap_pop<C>(k, simi, idxi);
        faiss::heap_push<C>(k, simi, idxi, dis, vid);
        nup++;
      }
    }
    return nup;
  }
};

class IVFFlatRetrievalParameters : public RetrievalParameters {
 public:
  IVFFlatRetrievalParameters() : RetrievalParameters() {
    parallel_on_queries_ = true;
    nprobe_ = -1;
  }

  IVFFlatRetrievalParameters(bool parallel_on_queries, int nprobe,
                             enum DistanceComputeType type)
      : RetrievalParameters() {
    parallel_on_queries_ = parallel_on_queries;
    nprobe_ = nprobe;
    distance_compute_type_ = type;
  }

  IVFFlatRetrievalParameters(enum DistanceComputeType type) {
    parallel_on_queries_ = true;
    nprobe_ = -1;
    distance_compute_type_ = type;
  }

  virtual ~IVFFlatRetrievalParameters() {}

  int Nprobe() { return nprobe_; }

  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

  bool ParallelOnQueries() { return parallel_on_queries_; }

  void SetParallelOnQueries(bool parallel_on_queries) {
    parallel_on_queries_ = parallel_on_queries;
  }

 protected:
  // parallelize over queries or ivf lists
  bool parallel_on_queries_;
  int nprobe_;
};

struct GammaIndexIVFFlat : faiss::IndexIVFFlat, public RetrievalModel {
  GammaIndexIVFFlat();
  virtual ~GammaIndexIVFFlat();

  void search_preassigned(RetrievalContext *retrieval_context, idx_t n,
                          const float *x, int k, const idx_t *keys,
                          const float *coarse_dis, float *distances,
                          idx_t *labels, int nprobe, bool store_pairs) const;

  int Init(const std::string &model_parameters, int indexing_size) override;
  RetrievalParameters *Parse(const std::string &parameters) override;
  int Indexing() override;
  bool Add(int n, const uint8_t *vec) override;
  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;
  int Delete(const std::vector<int64_t> &ids);
  // int AddRTVecsToIndex() override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *ids) override;

  virtual void SearchPreassgined(RetrievalContext *retrieval_context, idx_t n,
                                 const float *x, int k, const idx_t *keys,
                                 const float *coarse_dis, float *distances,
                                 idx_t *labels, int nprobe, bool store_pairs);

  long GetTotalMemBytes() override { return 0; };

  int Dump(const std::string &dir) override;
  int Load(const std::string &dir) override;

  void train(int64_t n, const float *x) { faiss::IndexIVFFlat::train(n, x); }

 private:
  GammaInvertedListScanner *GetGammaInvertedListScanner(
      bool store_pairs, faiss::MetricType metric_type) const;

 protected:
  int indexed_vec_count_;
  bool check_vector_ = true;

 private:
  realtime::RTInvertIndex *rt_invert_index_ptr_;
  uint64_t updated_num_;
#ifdef PERFORMANCE_TESTING
  int add_count_;
#endif
};

}  // namespace tig_gamma

#endif
