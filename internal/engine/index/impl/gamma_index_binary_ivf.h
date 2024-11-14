/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/IndexBinaryIVF.h>
#include <faiss/utils/utils.h>

#include <atomic>

#include "c_api/api_data/request.h"
#include "index/index_model.h"
#include "index/realtime/realtime_invert_index.h"
#include "vector/raw_vector.h"

namespace vearch {

using idx_t = faiss::Index::idx_t;

class BinaryIVFRetrievalParameters : public RetrievalParameters {
 public:
  BinaryIVFRetrievalParameters() : RetrievalParameters() { nprobe_ = 20; }

  BinaryIVFRetrievalParameters(int nprobe) : RetrievalParameters() {
    nprobe_ = nprobe;
  }

  ~BinaryIVFRetrievalParameters() {}

  int Nprobe() { return nprobe_; }

  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

 protected:
  int nprobe_;
};

struct GammaBinaryInvertedListScanner {
  GammaBinaryInvertedListScanner() { retrieval_context_ = nullptr; }

  /// from now on we handle this query.
  virtual void set_query(const uint8_t *query_vector) = 0;

  /// following codes come from this inverted list
  virtual void set_list(idx_t list_no, uint8_t coarse_dis) = 0;

  /// compute a single query-to-code distance
  // virtual uint32_t distance_to_code(const uint8_t *code) const = 0;

  /** compute the distances to codes. (distances, labels) should be
   * organized as a min- or max-heap
   *
   * @param n      number of codes to scan
   * @param codes  codes to scan (n * code_size)
   * @param ids        corresponding ids (ignored if store_pairs)
   * @param distances  heap distances (size k)
   * @param labels     heap labels (size k)
   * @param k          heap size
   */
  virtual size_t scan_codes(size_t n, const uint8_t *codes, const idx_t *ids,
                            int32_t *distances, idx_t *labels,
                            size_t k) const = 0;

  virtual ~GammaBinaryInvertedListScanner() {}

  void set_search_context(RetrievalContext *retrieval_context) {
    retrieval_context_ = retrieval_context;
  }

  RetrievalContext *retrieval_context_;
};

class GammaIndexBinaryIVF : public IndexModel, faiss::IndexBinaryIVF {
 public:
  GammaIndexBinaryIVF();

  virtual ~GammaIndexBinaryIVF();

  Status Init(const std::string &model_parameters,
              int training_threshold) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec) override;

  // assign the vectors, then call search_preassign
  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *labels) override;

  long GetTotalMemBytes() override;

  Status Dump(const std::string &dir) override { return Status::OK(); }
  Status Load(const std::string &index_dir, int64_t &load_num) override {
    load_num = 0;
    return Status::OK();
  }

  int Delete(const std::vector<int64_t> &ids) override;

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override {
    return 0;
  }

 private:
  void search_knn_hamming_heap(
      RetrievalContext *retrieval_context, size_t n, const uint8_t *x, int k,
      const idx_t *keys, const int32_t *coarse_dis, int32_t *distances,
      idx_t *labels, int nprobe, bool store_pairs,
      const faiss::IVFSearchParameters *params = nullptr);

  void search_preassigned(RetrievalContext *retrieval_context, int n,
                          const uint8_t *x, int k, const idx_t *idx,
                          const int32_t *coarse_dis, int32_t *distances,
                          idx_t *labels, int nprobe, bool store_pairs,
                          const faiss::IVFSearchParameters *params = nullptr);

  virtual GammaBinaryInvertedListScanner *get_GammaInvertedListScanner(
      bool store_pairs = false) const;

  int64_t indexed_vec_count_;
  realtime::RTInvertIndex *rt_invert_index_ptr_;

#ifdef PERFORMANCE_TESTING
  int64_t add_count_;
#endif
};
}  // namespace vearch
