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

#ifndef GAMMA_INDEX_IVFPQ_H_
#define GAMMA_INDEX_IVFPQ_H_

#include <unistd.h>
#include <atomic>
#include "field_range_index.h"
#include "gamma_common_data.h"
#include "gamma_index.h"
#include "log.h"
#include "raw_vector.h"
#include "realtime_invert_index.h"

#include "faiss/IndexIVF.h"
#include "faiss/IndexIVFPQ.h"
#include "faiss/InvertedLists.h"
#include "faiss/impl/FaissAssert.h"
#include "faiss/impl/io.h"
#include "faiss/index_io.h"
#include "faiss/utils/Heap.h"
#include "faiss/utils/distances.h"
#include "faiss/utils/hamming.h"
#include "faiss/utils/utils.h"

namespace tig_gamma {

/// statistics are robust to internal threading, but not if
/// IndexIVFPQ::search_preassigned is called by multiple threads
struct IndexIVFPQStats {
  size_t nrefine;  // nb of refines (IVFPQR)

  size_t n_hamming_pass;
  // nb of passed Hamming distance tests (for polysemous)

  // timings measured with the CPU RTC
  // on all threads
  size_t search_cycles;
  size_t refine_cycles;  // only for IVFPQR

  IndexIVFPQStats() { reset(); }
  void reset(){};
};

// global var that collects them all
extern IndexIVFPQStats indexIVFPQ_stats;

// namespace {

using idx_t = faiss::Index::idx_t;

static uint64_t get_cycles() {
#ifdef __x86_64__
  uint32_t high, low;
  asm volatile("rdtsc \n\t" : "=a"(low), "=d"(high));
  return ((uint64_t)high << 32) | (low);
#else
  return 0;
#endif
}

#define TIC t0 = get_cycles()
#define TOC get_cycles() - t0

/** QueryTables manages the various ways of searching an
 * IndexIVFPQ. The code contains a lot of branches, depending on:
 * - metric_type: are we computing L2 or Inner product similarity?
 * - by_residual: do we encode raw vectors or residuals?
 * - use_precomputed_table: are x_R|x_C tables precomputed?
 * - polysemous_ht: are we filtering with polysemous codes?
 */
struct QueryTables {
  /*****************************************************
   * General data from the IVFPQ
   *****************************************************/

  const faiss::IndexIVFPQ &ivfpq;
  const faiss::IVFSearchParameters *params;

  // copied from IndexIVFPQ for easier access
  int d;
  const faiss::ProductQuantizer &pq;
  faiss::MetricType metric_type;
  bool by_residual;
  int use_precomputed_table;
  int polysemous_ht;

  // pre-allocated data buffers
  float *sim_table, *sim_table_2;
  float *residual_vec, *decoded_vec;

  // single data buffer
  std::vector<float> mem;

  // for table pointers
  std::vector<const float *> sim_table_ptrs;

  explicit QueryTables(const faiss::IndexIVFPQ &ivfpq,
                       const faiss::IVFSearchParameters *params)
      : ivfpq(ivfpq),
        d(ivfpq.d),
        pq(ivfpq.pq),
        metric_type(ivfpq.metric_type),
        by_residual(ivfpq.by_residual),
        use_precomputed_table(ivfpq.use_precomputed_table) {
    mem.resize(pq.ksub * pq.M * 2 + d * 2);
    sim_table = mem.data();
    sim_table_2 = sim_table + pq.ksub * pq.M;
    residual_vec = sim_table_2 + pq.ksub * pq.M;
    decoded_vec = residual_vec + d;

    // for polysemous
    polysemous_ht = ivfpq.polysemous_ht;
    if (auto ivfpq_params =
            dynamic_cast<const faiss::IVFPQSearchParameters *>(params)) {
      polysemous_ht = ivfpq_params->polysemous_ht;
    }
    if (polysemous_ht != 0) {
      q_code.resize(pq.code_size);
    }
    init_list_cycles = 0;
    sim_table_ptrs.resize(pq.M);
  }

  /*****************************************************
   * What we do when query is known
   *****************************************************/

  // field specific to query
  const float *qi;

  // query-specific intialization
  void init_query(const float *qi) {
    this->qi = qi;
    if (metric_type == faiss::METRIC_INNER_PRODUCT)
      init_query_IP();
    else
      init_query_L2();
    if (!by_residual && polysemous_ht != 0) pq.compute_code(qi, q_code.data());
  }

  void init_query_IP() {
    // precompute some tables specific to the query qi
    pq.compute_inner_prod_table(qi, sim_table);
  }

  void init_query_L2() {
    if (!by_residual) {
      pq.compute_distance_table(qi, sim_table);
    } else if (use_precomputed_table) {
      pq.compute_inner_prod_table(qi, sim_table_2);
    }
  }

  /*****************************************************
   * When inverted list is known: prepare computations
   *****************************************************/

  // fields specific to list
  long key;
  float coarse_dis;
  std::vector<uint8_t> q_code;

  uint64_t init_list_cycles;

  /// once we know the query and the centroid, we can prepare the
  /// sim_table that will be used for accumulation
  /// and dis0, the initial value
  float precompute_list_tables() {
    float dis0 = 0;
    uint64_t t0;
    TIC;
    if (by_residual) {
      if (metric_type == faiss::METRIC_INNER_PRODUCT)
        dis0 = precompute_list_tables_IP();
      else
        dis0 = precompute_list_tables_L2();
    }
    init_list_cycles += TOC;
    return dis0;
  }

  float precompute_list_table_pointers() {
    float dis0 = 0;
    uint64_t t0;
    TIC;
    if (by_residual) {
      if (metric_type == faiss::METRIC_INNER_PRODUCT)
        FAISS_THROW_MSG("not implemented");
      else
        dis0 = precompute_list_table_pointers_L2();
    }
    init_list_cycles += TOC;
    return dis0;
  }

  /*****************************************************
   * compute tables for inner prod
   *****************************************************/

  float precompute_list_tables_IP() {
    // prepare the sim_table that will be used for accumulation
    // and dis0, the initial value
    ivfpq.quantizer->reconstruct(key, decoded_vec);
    // decoded_vec = centroid
    float dis0 = faiss::fvec_inner_product(qi, decoded_vec, d);

    if (polysemous_ht) {
      for (int i = 0; i < d; i++) {
        residual_vec[i] = qi[i] - decoded_vec[i];
      }
      pq.compute_code(residual_vec, q_code.data());
    }
    return dis0;
  }

  /*****************************************************
   * compute tables for L2 distance
   *****************************************************/

  float precompute_list_tables_L2() {
    float dis0 = 0;

    if (use_precomputed_table == 0 || use_precomputed_table == -1) {
      ivfpq.quantizer->compute_residual(qi, residual_vec, key);
      pq.compute_distance_table(residual_vec, sim_table);

      if (polysemous_ht != 0) {
        pq.compute_code(residual_vec, q_code.data());
      }

    } else if (use_precomputed_table == 1) {
      dis0 = coarse_dis;

      faiss::fvec_madd(pq.M * pq.ksub,
                       &ivfpq.precomputed_table[key * pq.ksub * pq.M], -2.0,
                       sim_table_2, sim_table);

      if (polysemous_ht != 0) {
        ivfpq.quantizer->compute_residual(qi, residual_vec, key);
        pq.compute_code(residual_vec, q_code.data());
      }

    } else if (use_precomputed_table == 2) {
      dis0 = coarse_dis;

      const faiss::MultiIndexQuantizer *miq =
          dynamic_cast<const faiss::MultiIndexQuantizer *>(ivfpq.quantizer);
      FAISS_THROW_IF_NOT(miq);
      const faiss::ProductQuantizer &cpq = miq->pq;
      int Mf = pq.M / cpq.M;

      const float *qtab = sim_table_2;  // query-specific table
      float *ltab = sim_table;          // (output) list-specific table

      long k = key;
      for (size_t cm = 0; cm < cpq.M; cm++) {
        // compute PQ index
        int ki = k & ((uint64_t(1) << cpq.nbits) - 1);
        k >>= cpq.nbits;

        // get corresponding table
        const float *pc =
            &ivfpq.precomputed_table[(ki * pq.M + cm * Mf) * pq.ksub];

        if (polysemous_ht == 0) {
          // sum up with query-specific table
          faiss::fvec_madd(Mf * pq.ksub, pc, -2.0, qtab, ltab);
          ltab += Mf * pq.ksub;
          qtab += Mf * pq.ksub;
        } else {
          for (size_t m = cm * Mf; m < (cm + 1) * Mf; m++) {
            q_code[m] =
                faiss::fvec_madd_and_argmin(pq.ksub, pc, -2, qtab, ltab);
            pc += pq.ksub;
            ltab += pq.ksub;
            qtab += pq.ksub;
          }
        }
      }
    }

    return dis0;
  }

  float precompute_list_table_pointers_L2() {
    float dis0 = 0;

    if (use_precomputed_table == 1) {
      dis0 = coarse_dis;

      const float *s = &ivfpq.precomputed_table[key * pq.ksub * pq.M];
      for (size_t m = 0; m < pq.M; m++) {
        sim_table_ptrs[m] = s;
        s += pq.ksub;
      }
    } else if (use_precomputed_table == 2) {
      dis0 = coarse_dis;

      const faiss::MultiIndexQuantizer *miq =
          dynamic_cast<const faiss::MultiIndexQuantizer *>(ivfpq.quantizer);
      FAISS_THROW_IF_NOT(miq);
      const faiss::ProductQuantizer &cpq = miq->pq;
      int Mf = pq.M / cpq.M;

      long k = key;
      int m0 = 0;
      for (size_t cm = 0; cm < cpq.M; cm++) {
        int ki = k & ((uint64_t(1) << cpq.nbits) - 1);
        k >>= cpq.nbits;

        const float *pc =
            &ivfpq.precomputed_table[(ki * pq.M + cm * Mf) * pq.ksub];

        for (int m = m0; m < m0 + Mf; m++) {
          sim_table_ptrs[m] = pc;
          pc += pq.ksub;
        }
        m0 += Mf;
      }
    } else {
      FAISS_THROW_MSG("need precomputed tables");
    }

    if (polysemous_ht) {
      FAISS_THROW_MSG("not implemented");
      // Not clear that it makes sense to implemente this,
      // because it costs M * ksub, which is what we wanted to
      // avoid with the tables pointers.
    }

    return dis0;
  }
};

template <class C>
struct KnnSearchResults {
  idx_t key;
  const idx_t *ids;

  // heap params
  size_t k;
  float *heap_sim;
  idx_t *heap_ids;

  size_t nup;

  inline void add(idx_t j, float dis) {
    if (C::cmp(heap_sim[0], dis)) {
      faiss::heap_pop<C>(k, heap_sim, heap_ids);
      idx_t id = ids ? ids[j] : (key << 32 | j);
      faiss::heap_push<C>(k, heap_sim, heap_ids, dis, id);
      nup++;
    }
  }
};

/*****************************************************
 * Scaning the codes.
 * The scanning functions call their favorite precompute_*
 * function to precompute the tables they need.
 *****************************************************/
template <typename IDType, faiss::MetricType METRIC_TYPE>
struct IVFPQScannerT : QueryTables {
  const uint8_t *list_codes;
  const IDType *list_ids;
  size_t list_size;

  explicit IVFPQScannerT(const faiss::IndexIVFPQ &ivfpq,
                         const faiss::IVFSearchParameters *params)
      : QueryTables(ivfpq, params) {
    FAISS_THROW_IF_NOT(pq.nbits == 8);
    assert(METRIC_TYPE == metric_type);
  }

  float dis0;

  void init_list(idx_t list_no, float coarse_dis, int mode) {
    this->key = list_no;
    this->coarse_dis = coarse_dis;

    if (mode == 2) {
      dis0 = precompute_list_tables();
    } else if (mode == 1) {
      dis0 = precompute_list_table_pointers();
    }
  }

  /// tables are not precomputed, but pointers are provided to the
  /// relevant X_c|x_r tables
  template <class SearchResultType>
  void scan_list_with_pointer(size_t ncode, const uint8_t *codes,
                              SearchResultType &res) const {
    for (size_t j = 0; j < ncode; j++) {
      float dis = dis0;
      const float *tab = sim_table_2;

      for (size_t m = 0; m < pq.M; m++) {
        int ci = *codes++;
        dis += sim_table_ptrs[m][ci] - 2 * tab[ci];
        tab += pq.ksub;
      }
      res.add(j, dis);
    }
  }

  /// nothing is precomputed: access residuals on-the-fly
  template <class SearchResultType>
  void scan_on_the_fly_dist(size_t ncode, const uint8_t *codes,
                            SearchResultType &res) const {
    const float *dvec;
    float dis0 = 0;
    if (by_residual) {
      if (METRIC_TYPE == faiss::METRIC_INNER_PRODUCT) {
        ivfpq.quantizer->reconstruct(key, residual_vec);
        dis0 = faiss::fvec_inner_product(residual_vec, qi, d);
      } else {
        ivfpq.quantizer->compute_residual(qi, residual_vec, key);
      }
      dvec = residual_vec;
    } else {
      dvec = qi;
      dis0 = 0;
    }

    for (size_t j = 0; j < ncode; j++) {
      pq.decode(codes, decoded_vec);
      codes += pq.code_size;

      float dis;
      if (METRIC_TYPE == faiss::METRIC_INNER_PRODUCT) {
        dis = dis0 + faiss::fvec_inner_product(decoded_vec, qi, d);
      } else {
        dis = faiss::fvec_L2sqr(decoded_vec, dvec, d);
      }
      res.add(j, dis);
    }
  }

  /*****************************************************
   * Scanning codes with polysemous filtering
   *****************************************************/

  template <class HammingComputer, class SearchResultType>
  void scan_list_polysemous_hc(size_t ncode, const uint8_t *codes,
                               SearchResultType &res) const {
    int ht = ivfpq.polysemous_ht;
    size_t n_hamming_pass = 0;

    int code_size = pq.code_size;

    HammingComputer hc(q_code.data(), code_size);

    for (size_t j = 0; j < ncode; j++) {
      const uint8_t *b_code = codes;
      int hd = hc.hamming(b_code);
      if (hd < ht) {
        n_hamming_pass++;

        float dis = dis0;
        const float *tab = sim_table;

        for (size_t m = 0; m < pq.M; m++) {
          dis += tab[*b_code++];
          tab += pq.ksub;
        }
        res.add(j, dis);
      }
      codes += code_size;
    }
#pragma omp critical
    { indexIVFPQ_stats.n_hamming_pass += n_hamming_pass; }
  }

  template <class SearchResultType>
  void scan_list_polysemous(size_t ncode, const uint8_t *codes,
                            SearchResultType &res) const {
    switch (pq.code_size) {
#define HANDLE_CODE_SIZE(cs)                                               \
  case cs:                                                                 \
    scan_list_polysemous_hc<faiss::HammingComputer##cs, SearchResultType>( \
        ncode, codes, res);                                                \
    break
      HANDLE_CODE_SIZE(4);
      HANDLE_CODE_SIZE(8);
      HANDLE_CODE_SIZE(16);
      HANDLE_CODE_SIZE(20);
      HANDLE_CODE_SIZE(32);
      HANDLE_CODE_SIZE(64);
#undef HANDLE_CODE_SIZE
      default:
        if (pq.code_size % 8 == 0)
          scan_list_polysemous_hc<faiss::HammingComputerM8, SearchResultType>(
              ncode, codes, res);
        else
          scan_list_polysemous_hc<faiss::HammingComputerM4, SearchResultType>(
              ncode, codes, res);
        break;
    }
  }
};

struct GammaInvertedListScanner : faiss::InvertedListScanner {
  GammaInvertedListScanner() {
    docids_bitmap_ = nullptr;
    raw_vec_ = nullptr;
    range_index_ptr_ = nullptr;
  }

  virtual size_t scan_codes_pointer(size_t ncode, const uint8_t **codes,
                                    const idx_t *ids, float *heap_sim,
                                    idx_t *heap_ids, size_t k) = 0;

  inline void SetVecFilter(const char *docids_bitmap,
                           const RawVector *raw_vec) {
    if (docids_bitmap == nullptr) {
      LOG(ERROR) << "docids_bitmap is NULL!";
      return;
    }

    if (!docids_bitmap_) {
      docids_bitmap_ = docids_bitmap;
    }

    if (!raw_vec_) {
      raw_vec_ = raw_vec;
    }

    return;
  }

  inline void set_search_condition(const GammaSearchCondition *condition) {
    this->range_index_ptr_ = condition->range_query_result;
  }

  const char *docids_bitmap_;
  const RawVector *raw_vec_;
  MultiRangeQueryResults *range_index_ptr_;
};

template <faiss::MetricType METRIC_TYPE, class C, int precompute_mode>
struct GammaIVFPQScanner : IVFPQScannerT<idx_t, METRIC_TYPE>,
                           GammaInvertedListScanner {
  bool store_pairs_;

  GammaIVFPQScanner(const faiss::IndexIVFPQ &ivfpq, bool store_pairs)
      : IVFPQScannerT<idx_t, METRIC_TYPE>(ivfpq, nullptr) {
    store_pairs_ = store_pairs;
  }

  template <class SearchResultType>
  void scan_list_with_table(size_t ncode, const uint8_t *codes,
                            SearchResultType &res) const {
    assert(this->pq.M % 4 == 0);

    // set filter func
    std::function<bool(int)> is_filterable;

    if (range_index_ptr_ != nullptr) {
      is_filterable = [this](int doc_id) -> bool {
        return (bitmap::test(docids_bitmap_, doc_id) ||
                (not range_index_ptr_->Has(doc_id)));
      };
    } else {
      is_filterable = [this](int doc_id) -> bool {
        return (bitmap::test(docids_bitmap_, doc_id));
      };
    }

    // set compute distance func
    std::function<float(const uint8_t *)> calc_dis;

    if (this->pq.M % 4 == 0) {
      calc_dis = [this](const uint8_t *codes) -> float {
        float dis = this->dis0;
        const float *tab = this->sim_table;

        for (size_t m = 0; m < this->pq.M; m += 4) {
          dis += tab[*codes++], tab += this->pq.ksub;
          dis += tab[*codes++], tab += this->pq.ksub;
          dis += tab[*codes++], tab += this->pq.ksub;
          dis += tab[*codes++], tab += this->pq.ksub;
        }

        return dis;
      };
    } else {
      calc_dis = [this](const uint8_t *codes) -> float {
        float dis = this->dis0;
        const float *tab = this->sim_table;

        for (size_t m = 0; m < this->pq.M; m++) {
          dis += tab[*codes++], tab += this->pq.ksub;
        }

        return dis;
      };
    }

#define HANDLE_ONE                                 \
  do {                                             \
    int doc_id = raw_vec_->vid2docid_[res.ids[j]]; \
    if ((range_index_ptr_ != nullptr &&            \
         (not range_index_ptr_->Has(doc_id))) ||   \
        bitmap::test(docids_bitmap_, doc_id)) {    \
      codes += this->pq.M; /* increment pointer */ \
      j++;                 /* increment j*/        \
      continue;                                    \
    }                                              \
                                                   \
    float dis = this->dis0;                        \
    const float *tab = this->sim_table;            \
    for (size_t m = 0; m < this->pq.M; m += 4) {   \
      dis += tab[*codes++], tab += this->pq.ksub;  \
      dis += tab[*codes++], tab += this->pq.ksub;  \
      dis += tab[*codes++], tab += this->pq.ksub;  \
      dis += tab[*codes++], tab += this->pq.ksub;  \
    }                                              \
                                                   \
    res.add(j, dis);                               \
                                                   \
    j++; /* increment j */                         \
  } while (0)
    size_t j = 0;
    size_t loops = ncode / 8;
    for (size_t i = 0; i < loops; i++) {
      HANDLE_ONE;  // 1
      HANDLE_ONE;  // 2
      HANDLE_ONE;  // 3
      HANDLE_ONE;  // 4
      HANDLE_ONE;  // 5
      HANDLE_ONE;  // 6
      HANDLE_ONE;  // 7
      HANDLE_ONE;  // 8
    }

    switch (ncode % 8) {
      case 7:
        HANDLE_ONE;
      case 6:
        HANDLE_ONE;
      case 5:
        HANDLE_ONE;
      case 4:
        HANDLE_ONE;
      case 3:
        HANDLE_ONE;
      case 2:
        HANDLE_ONE;
      case 1:
        HANDLE_ONE;
    }

    assert(j == ncode);

#undef HANDLE_ONE
  }

  template <class SearchResultType>
  void scan_list_with_table(size_t ncode, const uint8_t **codes,
                            SearchResultType &res) const {
    assert(this->pq.M % 4 == 0);

#define HANDLE_ONE                               \
  do {                                           \
    float dis = this->dis0;                      \
    const float *tab = this->sim_table;          \
    const uint8_t *code = codes[j];              \
    for (size_t m = 0; m < this->pq.M; m += 4) { \
      dis += tab[*code++], tab += this->pq.ksub; \
      dis += tab[*code++], tab += this->pq.ksub; \
      dis += tab[*code++], tab += this->pq.ksub; \
      dis += tab[*code++], tab += this->pq.ksub; \
    }                                            \
                                                 \
    res.add(j, dis);                             \
                                                 \
    j++; /* increment j */                       \
  } while (0)

    size_t j = 0;
    size_t loops = ncode / 8;
    for (size_t i = 0; i < loops; i++) {
      HANDLE_ONE;  // 1
      HANDLE_ONE;  // 2
      HANDLE_ONE;  // 3
      HANDLE_ONE;  // 4
      HANDLE_ONE;  // 5
      HANDLE_ONE;  // 6
      HANDLE_ONE;  // 7
      HANDLE_ONE;  // 8
    }

    switch (ncode % 8) {
      case 7:
        HANDLE_ONE;
      case 6:
        HANDLE_ONE;
      case 5:
        HANDLE_ONE;
      case 4:
        HANDLE_ONE;
      case 3:
        HANDLE_ONE;
      case 2:
        HANDLE_ONE;
      case 1:
        HANDLE_ONE;
    }

    assert(j == ncode);

#undef HANDLE_ONE
  }

  inline void set_query(const float *query) override {
    this->init_query(query);
  }

  inline void set_list(idx_t list_no, float coarse_dis) override {
    this->init_list(list_no, coarse_dis, precompute_mode);
  }

  inline float distance_to_code(const uint8_t *code) const override {
    assert(precompute_mode == 2);
    float dis = this->dis0;
    const float *tab = this->sim_table;

    for (size_t m = 0; m < this->pq.M; m++) {
      dis += tab[*code++];
      tab += this->pq.ksub;
    }
    return dis;
  }

  inline size_t scan_codes(size_t ncode, const uint8_t *codes, const idx_t *ids,
                           float *heap_sim, idx_t *heap_ids,
                           size_t k) const override {
    KnnSearchResults<C> res = {/* key */ this->key,
                               /* ids */ this->store_pairs_ ? nullptr : ids,
                               /* k */ k,
                               /* heap_sim */ heap_sim,
                               /* heap_ids */ heap_ids,
                               /* nup */ 0};

    if (this->polysemous_ht > 0) {
      assert(precompute_mode == 2);
      this->scan_list_polysemous(ncode, codes, res);
    } else if (precompute_mode == 2) {
      this->scan_list_with_table(ncode, codes, res);
    } else if (precompute_mode == 1) {
      this->scan_list_with_pointer(ncode, codes, res);
    } else if (precompute_mode == 0) {
      this->scan_on_the_fly_dist(ncode, codes, res);
    } else {
      FAISS_THROW_MSG("bad precomp mode");
    }
    return 0;
  }

  inline size_t scan_codes_pointer(size_t ncode, const uint8_t **codes,
                                   const idx_t *ids, float *heap_sim,
                                   idx_t *heap_ids, size_t k) {
    KnnSearchResults<C> res = {/* key */ this->key,
                               /* ids */ this->store_pairs_ ? nullptr : ids,
                               /* k */ k,
                               /* heap_sim */ heap_sim,
                               /* heap_ids */ heap_ids,
                               /* nup */ 0};

    if (precompute_mode == 2) {
      this->scan_list_with_table(ncode, codes, res);
    } else {
      FAISS_THROW_MSG("bad precomp mode");
    }
    return 0;
  }
};

//} // anonymous namespace

struct RTInvertedLists : faiss::InvertedLists {
  RTInvertedLists(realtime::RTInvertIndex *rt_invert_index_ptr, size_t nlist,
                  size_t code_size);

  /*************************
   *  Read only functions */

  // get the size of a list
  size_t list_size(size_t list_no) const override;

  /** get the codes for an inverted list
   * must be released by release_codes
   *
   * @return codes    size list_size * code_size
   */
  const uint8_t *get_codes(size_t list_no) const override;

  /** get the ids for an inverted list
   * must be released by release_ids
   *
   * @return ids      size list_size
   */
  const idx_t *get_ids(size_t list_no) const override;

  /*************************
   * writing functions     */

  size_t add_entries(size_t list_no, size_t n_entry, const idx_t *ids,
                     const uint8_t *code) override;

  void resize(size_t list_no, size_t new_size) override;

  void update_entries(size_t list_no, size_t offset, size_t n_entry,
                      const idx_t *ids_in, const uint8_t *codes_in) override;

  realtime::RTInvertIndex *rt_invert_index_ptr_;
};

/*************************************************************
 * I/O macros
 *
 * we use macros so that we have a line number to report in abort
 * (). This makes debugging a lot easier. The IOReader or IOWriter is
 * always called f and thus is not passed in as a macro parameter.
 **************************************************************/

#define WRITEANDCHECK(ptr, n)                                                 \
  {                                                                           \
    size_t ret = (*f)(ptr, sizeof(*(ptr)), n);                                \
    FAISS_THROW_IF_NOT_FMT(ret == (n), "write error in %s: %ld != %ld (%s)",  \
                           f->name.c_str(), ret, size_t(n), strerror(errno)); \
  }

#define READANDCHECK(ptr, n)                                                  \
  {                                                                           \
    size_t ret = (*f)(ptr, sizeof(*(ptr)), n);                                \
    FAISS_THROW_IF_NOT_FMT(ret == (n), "read error in %s: %ld != %ld (%s)",   \
                           f->name.c_str(), ret, size_t(n), strerror(errno)); \
  }

#define WRITE1(x) WRITEANDCHECK(&(x), 1)
#define READ1(x) READANDCHECK(&(x), 1)

#define WRITEVECTOR(vec)               \
  {                                    \
    size_t size = (vec).size();        \
    WRITEANDCHECK(&size, 1);           \
    WRITEANDCHECK((vec).data(), size); \
  }

// will fail if we write 256G of data at once...
#define READVECTOR(vec)                                 \
  {                                                     \
    size_t size;                                        \
    READANDCHECK(&size, 1);                             \
    FAISS_THROW_IF_NOT(size >= 0 && size < (1L << 40)); \
    (vec).resize(size);                                 \
    READANDCHECK((vec).data(), size);                   \
  }

/****************************************************************
 * Write
 *****************************************************************/
static void write_index_header(const faiss::Index *idx, faiss::IOWriter *f) {
  WRITE1(idx->d);
  WRITE1(idx->ntotal);
  faiss::Index::idx_t dummy = 1 << 20;
  WRITE1(dummy);
  WRITE1(dummy);
  WRITE1(idx->is_trained);
  WRITE1(idx->metric_type);
}

static void write_ivf_header(const faiss::IndexIVF *ivf, faiss::IOWriter *f) {
  write_index_header(ivf, f);
  WRITE1(ivf->nlist);
  WRITE1(ivf->nprobe);
  faiss::write_index(ivf->quantizer, f);
  WRITE1(ivf->maintain_direct_map);
  WRITEVECTOR(ivf->direct_map);
}

static void read_index_header(faiss::Index *idx, faiss::IOReader *f) {
  READ1(idx->d);
  READ1(idx->ntotal);
  faiss::Index::idx_t dummy;
  READ1(dummy);
  READ1(dummy);
  READ1(idx->is_trained);
  READ1(idx->metric_type);
  idx->verbose = false;
}

static void read_ivf_header(
    faiss::IndexIVF *ivf, faiss::IOReader *f,
    std::vector<std::vector<faiss::Index::idx_t>> *ids = nullptr) {
  read_index_header(ivf, f);
  READ1(ivf->nlist);
  READ1(ivf->nprobe);
  ivf->quantizer = faiss::read_index(f);
  ivf->own_fields = true;
  if (ids) {  // used in legacy "Iv" formats
    ids->resize(ivf->nlist);
    for (size_t i = 0; i < ivf->nlist; i++) READVECTOR((*ids)[i]);
  }
  READ1(ivf->maintain_direct_map);
  READVECTOR(ivf->direct_map);
}

static void write_ProductQuantizer(const faiss::ProductQuantizer *pq,
                                   faiss::IOWriter *f) {
  WRITE1(pq->d);
  WRITE1(pq->M);
  WRITE1(pq->nbits);
  WRITEVECTOR(pq->centroids);
}

static void read_ProductQuantizer(faiss::ProductQuantizer *pq,
                                  faiss::IOReader *f) {
  READ1(pq->d);
  READ1(pq->M);
  READ1(pq->nbits);
  pq->set_derived_values();
  READVECTOR(pq->centroids);
}

// namespace {

struct FileIOReader : faiss::IOReader {
  FILE *f = nullptr;
  bool need_close = false;

  FileIOReader(FILE *rf) : f(rf) {}

  FileIOReader(const char *fname) {
    name = fname;
    f = fopen(fname, "rb");
    FAISS_THROW_IF_NOT_FMT(f, "could not open %s for reading: %s", fname,
                           strerror(errno));
    need_close = true;
  }

  ~FileIOReader() override {
    if (need_close) {
      int ret = fclose(f);
      if (ret != 0) {  // we cannot raise and exception in the destructor
        fprintf(stderr, "file %s close error: %s", name.c_str(),
                strerror(errno));
      }
    }
  }

  size_t operator()(void *ptr, size_t size, size_t nitems) override {
    return fread(ptr, size, nitems, f);
  }

  int fileno() override { return ::fileno(f); }
};

struct FileIOWriter : faiss::IOWriter {
  FILE *f = nullptr;
  bool need_close = false;

  FileIOWriter(FILE *wf) : f(wf) {}

  FileIOWriter(const char *fname) {
    name = fname;
    f = fopen(fname, "wb");
    FAISS_THROW_IF_NOT_FMT(f, "could not open %s for writing: %s", fname,
                           strerror(errno));
    need_close = true;
  }

  ~FileIOWriter() override {
    if (need_close) {
      int ret = fclose(f);
      if (ret != 0) {
        // we cannot raise and exception in the destructor
        fprintf(stderr, "file %s close error: %s", name.c_str(),
                strerror(errno));
      }
    }
  }

  size_t operator()(const void *ptr, size_t size, size_t nitems) override {
    return fwrite(ptr, size, nitems, f);
  }
  int fileno() override { return ::fileno(f); }
};

// } // anonymous namespace

struct GammaIVFPQIndex : GammaIndex, faiss::IndexIVFPQ {
  GammaIVFPQIndex(faiss::Index *quantizer, size_t d, size_t nlist, size_t M,
                  size_t nbits_per_idx, const char *docids_bitmap,
                  RawVector *raw_vec, int nprobe);
  ~GammaIVFPQIndex();

  faiss::InvertedListScanner *get_InvertedListScanner(
      bool store_pairs) const override;

  GammaInvertedListScanner *GetGammaInvertedListScanner(bool store_pairs) const;

  int Indexing() override;

  int AddRTVecsToIndex() override;

  bool Add(int n, const float *vec) override;

  int Search(const VectorQuery *query, const GammaSearchCondition *condition,
             VectorResult &result) override;

  void search_preassigned(int n, const float *x,
                          const GammaSearchCondition *condition,
                          const idx_t *assign, const float *centroid_dis,
                          float *distances, idx_t *labels, int *total,
                          bool store_pairs,
                          const faiss::IVFSearchParameters *params = nullptr);

  // assign the vectors, then call search_preassign
  void SearchIVFPQ(int n, const float *x, const GammaSearchCondition *condition,
                   float *distances, idx_t *labels, int *total);

  void SearchDirectly(int n, const float *x,
                      const GammaSearchCondition *condition, float *distances,
                      idx_t *labels, int *total);

  long GetTotalMemBytes() override {
    if (!rt_invert_index_ptr_) {
      return 0;
    }
    return rt_invert_index_ptr_->GetTotalMemBytes();
  }

  int Dump(const std::string &dir, int max_vid) override {
    if (!rt_invert_index_ptr_) {
      LOG(INFO) << "realtime invert index ptr is null";
      return -1;
    }
    if (!this->is_trained) {
      LOG(INFO) << "gamma index is not trained, skip dumping";
      return 0;
    }
    string vec_name = raw_vec_->GetName();
    string info_file = dir + "/" + vec_name + ".index.param";
    faiss::IOWriter *f = new FileIOWriter(info_file.c_str());
    const IndexIVFPQ *ivpq = static_cast<const IndexIVFPQ *>(this);
    write_ivf_header(ivpq, f);
    WRITE1(ivpq->by_residual);
    WRITE1(ivpq->code_size);
    tig_gamma::write_ProductQuantizer(&ivpq->pq, f);
    delete f;

    LOG(INFO) << "dump: d=" << ivpq->d << ", ntotal=" << ivpq->ntotal
              << ", is_trained=" << ivpq->is_trained
              << ", metric_type=" << ivpq->metric_type
              << ", nlist=" << ivpq->nlist << ", nprobe=" << ivpq->nprobe
              << ", maintain_direct_map=" << ivpq->maintain_direct_map
              << ", by_residual=" << ivpq->by_residual
              << ", code_size=" << ivpq->code_size << ", pq: d=" << ivpq->pq.d
              << ", M=" << ivpq->pq.M << ", nbits=" << ivpq->pq.nbits;

    if (indexed_vec_count_ <= 0) {
      LOG(INFO) << "no vector is indexed, do not need dump";
      return 0;
    }

    return rt_invert_index_ptr_->Dump(
        dir, vec_name, std::min(max_vid, indexed_vec_count_ - 1));
  }

  int Load(const std::vector<std::string> &index_dirs) {
    if (!rt_invert_index_ptr_) {
      return -1;
    }

    string vec_name = raw_vec_->GetName();
    string info_file =
        index_dirs[index_dirs.size() - 1] + "/" + vec_name + ".index.param";
    if (access(info_file.c_str(), F_OK) != 0) {
      LOG(INFO) << info_file << " isn't existed, skip loading";
      return 0;  // it should train again after load
    }

    faiss::IOReader *f = new FileIOReader(info_file.c_str());
    IndexIVFPQ *ivpq = static_cast<IndexIVFPQ *>(this);
    read_ivf_header(ivpq, f, nullptr);  // not legacy
    READ1(ivpq->by_residual);
    READ1(ivpq->code_size);
    read_ProductQuantizer(&ivpq->pq, f);

    // precomputed table not stored. It is cheaper to recompute it
    ivpq->use_precomputed_table = 0;
    if (ivpq->by_residual) ivpq->precompute_table();
    delete f;

    if (!this->is_trained) {
      LOG(ERROR) << "unexpected, gamma index information is loaded, but it "
                    "isn't trained";
      return 0;  // it should train again after load
    }

    indexed_vec_count_ = rt_invert_index_ptr_->Load(index_dirs, vec_name);

    LOG(INFO) << "load: d=" << ivpq->d << ", ntotal=" << ivpq->ntotal
              << ", is_trained=" << ivpq->is_trained
              << ", metric_type=" << ivpq->metric_type
              << ", nlist=" << ivpq->nlist << ", nprobe=" << ivpq->nprobe
              << ", maintain_direct_map=" << ivpq->maintain_direct_map
              << ", by_residual=" << ivpq->by_residual
              << ", code_size=" << ivpq->code_size << ", pq: d=" << ivpq->pq.d
              << ", M=" << ivpq->pq.M << ", nbits=" << ivpq->pq.nbits
              << ", indexed vector count=" << indexed_vec_count_;

    return indexed_vec_count_;
  }

  int indexed_vec_count_;
  realtime::RTInvertIndex *rt_invert_index_ptr_;

#ifdef PERFORMANCE_TESTING
  std::atomic<uint64_t> search_count_;
#endif
};

}  // namespace tig_gamma

#endif
