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

#include "faiss/IndexIVF.h"
#include "faiss/IndexIVFPQ.h"
#include "faiss/VectorTransform.h"
#include "faiss/IndexHNSW.h"
#include "faiss/invlists/DirectMap.h"
#include "faiss/invlists/InvertedLists.h"
#include "faiss/impl/FaissAssert.h"
#include "faiss/impl/io.h"
#include "faiss/index_io.h"
#include "faiss/utils/Heap.h"
#include "faiss/utils/distances.h"
#include "faiss/utils/hamming.h"
#include "faiss/utils/utils.h"
#include "table/field_range_index.h"
#include "common/gamma_common_data.h"
#include "gamma_index_flat.h"
#include "gamma_scanner.h"
#include "util/log.h"
#include "vector/memory_raw_vector.h"
#include "vector/raw_vector.h"
#include "realtime/realtime_invert_index.h"
#include "index/retrieval_model.h"
#include "util/utils.h"

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

#define TIC t0 = faiss::get_cycles()
#define TOC faiss::get_cycles() - t0

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
                       const faiss::IVFSearchParameters *params,
                       faiss::MetricType metric_type)
      : ivfpq(ivfpq),
        d(ivfpq.d),
        pq(ivfpq.pq),
        metric_type(metric_type),
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
                       ivfpq.precomputed_table.data() + key * pq.ksub * pq.M, -2.0,
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
            ivfpq.precomputed_table.data() + (ki * pq.M + cm * Mf) * pq.ksub;

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

      const float *s = ivfpq.precomputed_table.data() + key * pq.ksub * pq.M;
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
            ivfpq.precomputed_table.data() + (ki * pq.M + cm * Mf) * pq.ksub;

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
      idx_t id = ids ? ids[j] : faiss::lo_build(key, j);
      faiss::heap_replace_top<C>(k, heap_sim, heap_ids, dis, id);
      nup++;
    }
  }
};

/*****************************************************
 * Scaning the codes.
 * The scanning functions call their favorite precompute_*
 * function to precompute the tables they need.
 *****************************************************/
template <typename IDType, faiss::MetricType METRIC_TYPE, class PQDecoder>
struct IVFPQScannerT : QueryTables {
  const uint8_t *list_codes;
  const IDType *list_ids;
  size_t list_size;

  explicit IVFPQScannerT(const faiss::IndexIVFPQ &ivfpq,
                         const faiss::IVFSearchParameters *params)
      : QueryTables(ivfpq, params, METRIC_TYPE) {
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

  /// version of the scan where we use precomputed tables
  template <class SearchResultType>
  void scan_list_with_table(size_t ncode, const uint8_t* codes,
            SearchResultType& res) const {
    for (size_t j = 0; j < ncode; j++) {
      PQDecoder decoder(codes, pq.nbits);
      codes += pq.code_size;
      float dis = dis0;
      const float* tab = sim_table;

      for (size_t m = 0; m < pq.M; m++) {
        dis += tab[decoder.decode()];
        tab += pq.ksub;
      }

      res.add(j, dis);
    }
  } 

  /// tables are not precomputed, but pointers are provided to the
  /// relevant X_c|x_r tables
  template <class SearchResultType>
  void scan_list_with_pointer(size_t ncode, const uint8_t *codes,
                              SearchResultType &res) const {
    for (size_t j = 0; j < ncode; j++) {
      PQDecoder decoder(codes, pq.nbits);
      codes += pq.code_size;
      
      float dis = dis0;
      const float *tab = sim_table_2;

      for (size_t m = 0; m < pq.M; m++) {
        int ci = decoder.decode();
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
        PQDecoder decoder(codes, pq.nbits);

        float dis = dis0;
        const float *tab = sim_table;

        for (size_t m = 0; m < pq.M; m++) {
          dis += tab[decoder.decode()];
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

struct GammaIVFPQIndex;

template <faiss::MetricType METRIC_TYPE, class C, class PQDecoder>
struct GammaIVFPQScanner : IVFPQScannerT<idx_t, METRIC_TYPE, PQDecoder>,
                           GammaInvertedListScanner {
  int precompute_mode;
  bool store_pairs;
  const GammaIVFPQIndex &gamma_ivfpq_;

  GammaIVFPQScanner(const GammaIVFPQIndex &gamma_ivfpq, bool store_pairs, int precompute_mode)
      : IVFPQScannerT<idx_t, METRIC_TYPE, PQDecoder>(gamma_ivfpq, nullptr),
        precompute_mode(precompute_mode), store_pairs(store_pairs), 
        gamma_ivfpq_(gamma_ivfpq) {
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
    PQDecoder decoder(code, this->pq.nbits);

    for (size_t m = 0; m < this->pq.M; m++) {
      dis += tab[decoder.decode()];
      tab += this->pq.ksub;
    }
    return dis;
  }

  /// version of the scan where we use precomputed tables
  template <class SearchResultType>
  void scan_list_with_table(size_t ncode, const uint8_t* codes,
            SearchResultType& res) const {
    for (size_t j = 0; j < ncode; j++) {
      if (res.ids[j] & realtime::kDelIdxMask) {
        codes += this->pq.code_size;
        continue;
      }

      if (!retrieval_context_->IsValid(res.ids[j] &
                                       realtime::kRecoverIdxMask)) {
        codes += this->pq.code_size;
        continue;
      }
      PQDecoder decoder(codes, this->pq.nbits);
      codes += this->pq.code_size;
      float dis = this->dis0;
      const float* tab = this->sim_table;

      for (size_t m = 0; m < this->pq.M; m++) {
        dis += tab[decoder.decode()];
        tab += this->pq.ksub;
      }

      res.add(j, dis);
    }
  }

  inline size_t scan_codes(size_t ncode, const uint8_t *codes, const idx_t *ids,
                           float *heap_sim, idx_t *heap_ids,
                           size_t k) const override {
    KnnSearchResults<C> res = {/* key */ this->key,
                               /* ids */ this->store_pairs ? nullptr : ids,
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
    return res.nup;
  }
};

class IVFPQRetrievalParameters : public RetrievalParameters {
 public:
  IVFPQRetrievalParameters() : RetrievalParameters() {
    parallel_on_queries_ = true;
    recall_num_ = 100;
    nprobe_ = -1;
  }

  IVFPQRetrievalParameters(bool parallel_on_queries, int recall_num, int nprobe,
                           enum DistanceComputeType type) {
    parallel_on_queries_ = parallel_on_queries;
    recall_num_ = recall_num;
    nprobe_ = nprobe;
    distance_compute_type_ = type;
  }

  IVFPQRetrievalParameters(enum DistanceComputeType type) {
    parallel_on_queries_ = true;
    recall_num_ = 100;
    nprobe_ = -1;
    distance_compute_type_ = type;
  }

  virtual ~IVFPQRetrievalParameters() {}

  int RecallNum() { return recall_num_; }

  void SetRecallNum(int recall_num) { recall_num_ = recall_num; }

  int Nprobe() { return nprobe_; }

  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

  bool ParallelOnQueries() { return parallel_on_queries_; }

  void SetParallelOnQueries(bool parallel_on_queries) {
    parallel_on_queries_ = parallel_on_queries;
  }

 protected:
  // parallelize over queries or ivf lists
  bool parallel_on_queries_;
  int recall_num_;
  int nprobe_;
};

struct IVFPQModelParams {
  int ncentroids;     // coarse cluster center number
  int nsubvector;     // number of sub cluster center
  bool support_indivisible_nsubvector;     // Support for nsubvectors that are not divisible by dimensions
  int nbits_per_idx;  // bit number of sub cluster center
  int nprobe;         // search how many bucket
  DistanceComputeType metric_type;
  bool has_hnsw;
  int nlinks;          // link number for hnsw graph
  int efConstruction;  // construction parameter for building hnsw graph
  int efSearch;        // search parameter for search in hnsw graph
  bool has_opq;
  int opq_nsubvector;  // number of sub cluster center of opq
  int bucket_init_size; // original size of RTInvertIndex bucket
  int bucket_max_size; // max size of RTInvertIndex bucket

  IVFPQModelParams() {
    ncentroids = 2048;
    nsubvector = 64;
    support_indivisible_nsubvector = false;
    nbits_per_idx = 8;
    nprobe = 80;
    metric_type = DistanceComputeType::INNER_PRODUCT;
    has_hnsw = false;
    nlinks = 32;
    efConstruction = 200;
    efSearch = 64;
    has_opq = false;
    opq_nsubvector = 64;
    bucket_init_size = 1000;
    bucket_max_size = 1280000;
  }

  int Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      LOG(ERROR) << "parse IVFPQ retrieval parameters error: " << str;
      return -1;
    }

    int ncentroids;
    int nsubvector;
    int nbits_per_idx;
    int nprobe;
    int support_indivisible_nsubvector;

    // -1 as default
    if (!jp.GetInt("ncentroids", ncentroids)) {
      if (ncentroids < -1) {
        LOG(ERROR) << "invalid ncentroids =" << ncentroids;
        return -1;
      }
      if (ncentroids > 0) this->ncentroids = ncentroids;
    } else {
      LOG(ERROR) << "cannot get ncentroids for ivfpq, set it when create space";
      return -1;
    }

    if (!jp.GetInt("nsubvector", nsubvector)) {
      if (nsubvector < -1) {
        LOG(ERROR) << "invalid nsubvector =" << nsubvector;
        return -1;
      }
      if (nsubvector > 0) this->nsubvector = nsubvector;
    } else {
      LOG(ERROR) << "cannot get nsubvector for ivfpq, set it when create space";
      return -1;
    }

    if (!jp.GetInt("nbits_per_idx", nbits_per_idx)) {
      if (nbits_per_idx < -1) {
        LOG(ERROR) << "invalid nbits_per_idx =" << nbits_per_idx;
        return -1;
      }
      if (nbits_per_idx > 0) this->nbits_per_idx = nbits_per_idx;
    }

    if (!jp.GetInt("nprobe", nprobe)) {
      if (nprobe < -1) {
        LOG(ERROR) << "invalid nprobe =" << nprobe;
        return -1;
      }
      if (nprobe > 0) this->nprobe = nprobe;
      if (this->nprobe > this->ncentroids) {
        LOG(ERROR) << "nprobe should less than ncentroids";
        return -1;
      }
    }

    if (!jp.GetInt("support_indivisible_nsubvector", support_indivisible_nsubvector)) {
      if (support_indivisible_nsubvector != 0) 
        this->support_indivisible_nsubvector = true;
    }

    int bucket_init_size;
    int bucket_max_size;

    // -1 as default
    if (!jp.GetInt("bucket_init_size", bucket_init_size)) {
      if (bucket_init_size < -1) {
        LOG(ERROR) << "invalid bucket_init_size =" << bucket_init_size;
        return -1;
      }
      if (bucket_init_size > 0) this->bucket_init_size = bucket_init_size;
    }

    if (!jp.GetInt("bucket_max_size", bucket_max_size)) {
      if (bucket_max_size < -1) {
        LOG(ERROR) << "invalid bucket_max_size =" << bucket_max_size;
        return -1;
      }
      if (bucket_max_size > 0) this->bucket_max_size = bucket_max_size;
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

    utils::JsonParser jp_hnsw;
    if (!jp.GetObject("hnsw", jp_hnsw)) {
      has_hnsw = true;
      int nlinks;
      int efConstruction;
      int efSearch;
      // -1 as default
      if (!jp_hnsw.GetInt("nlinks", nlinks)) {
        if (nlinks < -1) {
          LOG(ERROR) << "invalid nlinks = " << nlinks;
          return -1;
        }
        if(nlinks > 0) this->nlinks = nlinks;
      }

      if (!jp_hnsw.GetInt("efConstruction", efConstruction)) {
        if (efConstruction < -1) {
          LOG(ERROR) << "invalid efConstruction = " << efConstruction;
          return -1;
        }
        if(efConstruction > 0) this->efConstruction = efConstruction;
      }

      if (!jp_hnsw.GetInt("efSearch", efSearch)) {
        if (efSearch < -1) {
          LOG(ERROR) << "invalid efSearch = " << efSearch;
          return -1;
        }
        if(efSearch > 0) this->efSearch = efSearch;
      }
    }

    utils::JsonParser jp_opq;
    if (!jp.GetObject("opq", jp_opq)) {
      has_opq = true;
      int opq_nsubvector;
      // -1 as default
      if (!jp_opq.GetInt("nsubvector", opq_nsubvector)) {
        if (nsubvector < -1) {
          LOG(ERROR) << "invalid opq_nsubvector = " << opq_nsubvector;
          return -1;
        }
        if (opq_nsubvector > 0) this->opq_nsubvector = opq_nsubvector;
      } 
    }

    if (!Validate()) return -1;
    return 0;
  }

  bool Validate() {
    if (ncentroids <= 0 || nsubvector <= 0 || nbits_per_idx <= 0) return false;
    //if (nbits_per_idx != 8) {
    //  LOG(ERROR) << "only support 8 now, nbits_per_idx=" << nbits_per_idx;
    //  return false;
    //}

    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids =" << ncentroids << ", ";
    ss << "nsubvector =" << nsubvector << ", ";
    ss << "support_indivisible_nsubvector =" << support_indivisible_nsubvector << ", ";
    ss << "nbits_per_idx =" << nbits_per_idx << ", ";
    ss << "nprobe =" << nprobe << ", ";
    ss << "metric_type =" << (int)metric_type << ", ";
    ss << "bucket_init_size =" << bucket_init_size << ", ";
    ss << "bucket_max_size =" << bucket_max_size;

    if (has_hnsw) {
      ss << ", hnsw: nlinks=" << nlinks << ", ";
      ss << "efConstrction=" << efConstruction << ", ";
      ss << "efSearch=" << efSearch;
    }
    if (has_opq) {
      ss << ", opq: nsubvector=" << opq_nsubvector;
    }

    return ss.str();
  }

  int ToJson(utils::JsonParser &jp) { return 0; }
};

struct GammaIVFPQIndex : GammaFLATIndex, faiss::IndexIVFPQ {
  GammaIVFPQIndex();

  virtual ~GammaIVFPQIndex();

  GammaInvertedListScanner *GetInvertedListScanner(
      bool store_pairs, faiss::MetricType metric_type);

  template <class PQDecoder>
  GammaInvertedListScanner *GetGammaInvertedListScanner(
      bool store_pairs, faiss::MetricType metric_type);

  int Init(const std::string &model_parameters, int indexing_size) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec);

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs);

  // assign the vectors, then call search_preassign
  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, idx_t *labels);

  void search_preassigned(RetrievalContext *retrieval_context, int n,
                          const float *x, const float *applied_x, int k, const idx_t *keys,
                          const float *coarse_dis, float *distances,
                          idx_t *labels, int nprobe, bool store_pairs,
                          const faiss::IVFSearchParameters *params = nullptr);

  long GetTotalMemBytes() override {
    if (!rt_invert_index_ptr_) {
      return 0;
    }
    return rt_invert_index_ptr_->GetTotalMemBytes();
  }

  int Dump(const std::string &dir) override;

  int Load(const std::string &index_dir) override;

  virtual void copy_subset_to(faiss::IndexIVF &other, int subset_type, idx_t a1,
                              idx_t a2) const;

  int Delete(const std::vector<int64_t> &ids);

  void train(int64_t n, const float *x) { faiss::IndexIVFPQ::train(n, x); }

  int indexed_vec_count_;
  realtime::RTInvertIndex *rt_invert_index_ptr_;
  bool compaction_;
  size_t compact_bucket_no_;
  uint64_t compacted_num_;
  uint64_t updated_num_;
  int d_;
  DistanceComputeType metric_type_;

  faiss::VectorTransform *opq_;
  // 0 is FlatL2, 1 is HNSWFlat
  int quantizer_type_;
#ifdef PERFORMANCE_TESTING
  std::atomic<uint64_t> search_count_;
  int add_count_;
#endif
  IVFPQModelParams *model_param_;
};

}  // namespace tig_gamma

#endif
