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

#pragma once

#include <unistd.h>
#include <atomic>

#include <faiss/impl/IDSelector.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFRaBitQ.h>
#include <faiss/MetricType.h>
#include <faiss/VectorTransform.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/io.h>
#include <faiss/impl/RaBitQStats.h>
#include <faiss/impl/RaBitQuantizer.h>
#include <faiss/impl/ResultHandler.h>
#include <faiss/impl/RaBitQUtils.h>
#include <faiss/index_io.h>
#include <faiss/invlists/DirectMap.h>
#include <faiss/invlists/InvertedLists.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/hamming.h>
#include <faiss/utils/utils.h>

#include "common/gamma_common_data.h"
#include "gamma_index_flat.h"
#include "index/index_model.h"
#include "index/realtime/realtime_invert_index.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/memory_raw_vector.h"
#include "vector/raw_vector.h"
 

namespace vearch {

using idx_t = faiss::idx_t;

struct GammaRaBitInvertedListScanner : faiss::InvertedListScanner {
  const faiss::IndexIVFRaBitQ& ivf_rabitq;

  std::vector<float> reconstructed_centroid;
  std::vector<float> query_vector;

  std::unique_ptr<faiss::FlatCodesDistanceComputer> dc;
  faiss::RaBitQDistanceComputer* rabitq_dc =
          nullptr; // For multi-bit adaptive filtering

  uint8_t qb = 0;
  bool centered = false;

  // for gamma index adaptive filtering
  RetrievalContext* retrieval_context_ = nullptr;

  explicit GammaRaBitInvertedListScanner(
          const faiss::IndexIVFRaBitQ& ivf_rabitq_in,
          bool store_pairs = false,
          const faiss::IDSelector* sel = nullptr,
          uint8_t qb_in = 0,
          bool centered = false,
          RetrievalContext* retrieval_context = nullptr)
          : faiss::InvertedListScanner(store_pairs, sel),
            ivf_rabitq{ivf_rabitq_in},
            qb{qb_in},
            centered(centered),
            retrieval_context_(retrieval_context) {
      keep_max = is_similarity_metric(ivf_rabitq.metric_type);
      code_size = ivf_rabitq.code_size;
  }

  /// from now on we handle this query.
  void set_query(const float* query_vector_in) override {
      query_vector.assign(query_vector_in, query_vector_in + ivf_rabitq.d);

      internal_try_setup_dc();
  }

  /// following codes come from this inverted list
  void set_list(idx_t list_no, float coarse_dis) override {
      this->list_no = list_no;

      reconstructed_centroid.resize(ivf_rabitq.d);
      ivf_rabitq.quantizer->reconstruct(
              list_no, reconstructed_centroid.data());

      internal_try_setup_dc();
  }

  /// compute a single query-to-code distance
  float distance_to_code(const uint8_t* code) const override {
      return dc->distance_to_code(code);
  }

  // redefiniing the scan_codes allows to inline the distance_to_code
  // (this is unlikely to matter because it contains a virtual function call)
  size_t scan_codes_1bit(
    size_t list_size,
    const uint8_t* codes,
    const idx_t* ids,
    faiss::ResultHandler& handler) const {
    if (keep_max) {
        return scan_codes_impl<faiss::CMin<float, int64_t>>(
            list_size, codes, ids, handler);
    } else {
        return scan_codes_impl<faiss::CMax<float, int64_t>>(
            list_size, codes, ids, handler);
    }
  }

private:
  // templateized scan implementation, reference expanded_scanners.h::run_scan_codes1
  template<class C>
  size_t scan_codes_impl(
    size_t list_size,
    const uint8_t* codes,
    const idx_t* ids,
    faiss::ResultHandler& handler) const {
    size_t nup = 0;
    float threshold = handler.threshold;

    for (size_t j = 0; j < list_size; j++) {
        if (sel != nullptr) {
            int64_t id = store_pairs ? faiss::lo_build(list_no, j) : ids[j];
            // skip code without computing distance
            if (!sel->is_member(id)) {
                codes += code_size;
                continue;
            }
        }

        if (!retrieval_context_->IsValid(ids[j] & realtime::kRecoverIdxMask)) {
          codes += code_size;
          continue;
        }

        float dis = distance_to_code(codes); // will be inlined if final
        if (!retrieval_context_->IsSimilarScoreValid(dis)) {
          continue;
        }
        if (C::cmp(threshold, dis)) {
            int64_t id = store_pairs ? faiss::lo_build(list_no, j) : ids[j];
            handler.add_result(dis, id);
            threshold = handler.threshold;
            nup++;
        }
        codes += code_size;
    }

    return nup;
  }

  /// Override scan_codes to implement adaptive filtering for multi-bit codes
  size_t scan_codes(
    size_t list_size,
    const uint8_t* codes,
    const idx_t* ids,
    faiss::ResultHandler& handler) const override {
    size_t ex_bits = ivf_rabitq.rabitq.nb_bits - 1;

    // For 1-bit codes, use default implementation
    if (ex_bits == 0 || rabitq_dc == nullptr) {
        return scan_codes_1bit(list_size, codes, ids, handler);
    }

    // Multi-bit: Two-stage search with adaptive filtering
    size_t nup = 0;

    // Stats tracking for multi-bit two-stage search
    // n_1bit_evaluations: candidates evaluated using 1-bit lower bound
    // n_multibit_evaluations: candidates requiring full multi-bit distance
    size_t local_1bit_evaluations = 0;
    size_t local_multibit_evaluations = 0;

    for (size_t j = 0; j < list_size; j++) {
        if (sel != nullptr) {
            int64_t id = store_pairs ? faiss::lo_build(list_no, j) : ids[j];
            if (!sel->is_member(id)) {
                codes += code_size;
                continue;
            }
        }

        if (!retrieval_context_->IsValid(ids[j] & realtime::kRecoverIdxMask)) {
          codes += code_size;
          continue;
        }

        local_1bit_evaluations++;

        // Stage 1: Compute distance bound using 1-bit codes
        // For L2 (min-heap): use lower_bound to safely skip if it's
        //                    already worse than heap worst
        // For IP (max-heap): use upper_bound because with a lower bound,
        //                    we can't safely skip any candidate
        float est_distance = rabitq_dc->distance_to_code_1bit(codes);

        // Extract f_error and g_error for filtering
        size_t code_size_base = (ivf_rabitq.d + 7) / 8;
        const faiss::rabitq_utils::SignBitFactorsWithError* base_fac =
                reinterpret_cast<
                        const faiss::rabitq_utils::SignBitFactorsWithError*>(
                        codes + code_size_base);

        bool should_refine = faiss::rabitq_utils::should_refine_candidate(
                est_distance,
                base_fac->f_error,
                rabitq_dc->g_error,
                handler.threshold,
                keep_max);
        if (should_refine) {
            local_multibit_evaluations++;
            // Lower bound is promising, compute full distance
            float dis = distance_to_code(codes);
            if (!retrieval_context_->IsSimilarScoreValid(dis)) {
              continue;
            }
            int64_t id = store_pairs ? faiss::lo_build(list_no, j) : ids[j];

            if (handler.add_result(dis, id)) {
                nup++;
            }
        }
        codes += code_size;
    }

    // Update global stats atomically
    #pragma omp atomic
    faiss::rabitq_stats.n_1bit_evaluations += local_1bit_evaluations;
    #pragma omp atomic
    faiss::rabitq_stats.n_multibit_evaluations += local_multibit_evaluations;

    return nup;
  }

  void internal_try_setup_dc() {
      if (!query_vector.empty() && !reconstructed_centroid.empty()) {
          // both query_vector and centroid are available!
          // set up DistanceComputer
          dc.reset(ivf_rabitq.rabitq.get_distance_computer(
                  qb, reconstructed_centroid.data(), centered));

          dc->set_query(query_vector.data());

          // Try to cast to RaBitQDistanceComputer for multi-bit support
          rabitq_dc = dynamic_cast<faiss::RaBitQDistanceComputer*>(dc.get());
      }
  }
};

class IVFRABITQRetrievalParameters : public RetrievalParameters {
public:
IVFRABITQRetrievalParameters() : RetrievalParameters() {
    parallel_on_queries_ = true;
    recall_num_ = -1;
    nprobe_ = -1;
    collect_metrics_ = 0;
    qb_ = 4;
    centered_ = false;
  }

  IVFRABITQRetrievalParameters(bool parallel_on_queries, int recall_num, int nprobe,
                          enum DistanceComputeType type, int collect_metrics, int qb, bool centered) {
    parallel_on_queries_ = parallel_on_queries;
    recall_num_ = recall_num;
    nprobe_ = nprobe;
    distance_compute_type_ = type;
    collect_metrics_ = collect_metrics;
    qb_ = qb;
    centered_ = centered;
  }

  IVFRABITQRetrievalParameters(int nprobe, enum DistanceComputeType type) {
    parallel_on_queries_ = true;
    recall_num_ = -1;
    nprobe_ = nprobe;
    qb_ = 4;
    centered_ = false;
    distance_compute_type_ = type;
    collect_metrics_ = 0;
  }

  virtual ~IVFRABITQRetrievalParameters() {}

  int RecallNum() { return recall_num_; }

  void SetRecallNum(int recall_num) { recall_num_ = recall_num; }

  int Nprobe() { return nprobe_; }

  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

  bool ParallelOnQueries() { return parallel_on_queries_; }

  void SetParallelOnQueries(bool parallel_on_queries) {
    parallel_on_queries_ = parallel_on_queries;
  }

  int Qb() { return qb_; }

  void SetQb(int qb) { qb_ = qb; }

  bool Centered() { return centered_; }

  void SetCentered(bool centered) { centered_ = centered; }

protected:
  // parallelize over queries or ivf lists
  bool parallel_on_queries_;
  int recall_num_;
  int nprobe_;
  int qb_;
  bool centered_;
};

struct IVFRABITQModelParams {
  int ncentroids;                       // coarse cluster center number
  int nb_bits;                          // number of bits per dimension
  int qb;                               // for searching
  int code_size;                         // code size, just for debug
  int d;                                  // dimension, just for debug
  int nprobe;                           // search how many bucket
  DistanceComputeType metric_type;
  bool has_hnsw;
  int nlinks;          // link number for hnsw graph
  int efConstruction;  // construction parameter for building hnsw graph
  int efSearch;        // search parameter for search in hnsw graph
  int bucket_init_size;  // original size of RTInvertIndex bucket
  int bucket_max_size;   // max size of RTInvertIndex bucket
  int training_threshold;

  IVFRABITQModelParams() {
    ncentroids = 2048;
    nb_bits = 4;
    qb = 4;
    code_size = 0;
    d = 0;
    nprobe = 80;
    metric_type = DistanceComputeType::INNER_PRODUCT;
    has_hnsw = false;
    nlinks = 32;
    efConstruction = 200;
    efSearch = 64;
    bucket_init_size = 1000;
    bucket_max_size = 1280000;
  }

  Status Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      std::string msg =
          std::string("parse IVFRabitQ retrieval parameters error: ") + str;
      LOG(ERROR) << msg;
      return Status::ParamError(msg);
    }

    int ncentroids;
    int nb_bits;
    int nprobe;
    int qb;

    // -1 as default
    if (!jp.GetInt("ncentroids", ncentroids)) {
      if (ncentroids < -1) {
        std::string msg =
            std::string("invalid ncentroids =") + std::to_string(ncentroids);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (ncentroids > 0) this->ncentroids = ncentroids;
    }

    if (!jp.GetInt("nb_bits", nb_bits)) {
      if (nb_bits < 1 || nb_bits > 9) {
        std::string msg = std::string("invalid nb_bits =") + std::to_string(nb_bits) + " should be integer in [1, 9]";
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      } else {
        this->nb_bits = nb_bits;
      }
    }

    if (!jp.GetInt("qb", qb)) {
      if (qb < 0 || qb > 8) {
        std::string msg = std::string("invalid qb =") + std::to_string(qb) + " should be integer in [0, 8]";
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      } else {
        this->qb = qb;
      }
    }

    if (!jp.GetInt("nprobe", nprobe)) {
      if (nprobe < -1) {
        std::string msg =
            std::string("invalid nprobe =") + std::to_string(nprobe);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (nprobe > 0) this->nprobe = nprobe;
      if (this->nprobe > this->ncentroids) {
        std::string msg = "nprobe should less than ncentroids";
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
    }

    int bucket_init_size;
    int bucket_max_size;

    // -1 as default
    if (!jp.GetInt("bucket_init_size", bucket_init_size)) {
      if (bucket_init_size < -1) {
        std::string msg = std::string("invalid bucket_init_size =") +
                          std::to_string(bucket_init_size);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (bucket_init_size > 0) this->bucket_init_size = bucket_init_size;
    }

    if (!jp.GetInt("bucket_max_size", bucket_max_size)) {
      if (bucket_max_size < -1) {
        std::string msg = std::string("invalid bucket_max_size =") +
                          std::to_string(bucket_max_size);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (bucket_max_size > 0) this->bucket_max_size = bucket_max_size;
    }

    std::string metric_type;

    if (!jp.GetString("metric_type", metric_type)) {
      if (strcasecmp("L2", metric_type.c_str()) &&
          strcasecmp("InnerProduct", metric_type.c_str())) {
        std::string msg = std::string("invalid metric_type = ") + metric_type;
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
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
          std::string msg =
              std::string("invalid nlinks = ") + std::to_string(nlinks);
          LOG(ERROR) << msg;
          return Status::ParamError(msg);
        }
        if (nlinks > 0) this->nlinks = nlinks;
      }

      if (!jp_hnsw.GetInt("efConstruction", efConstruction)) {
        if (efConstruction < -1) {
          std::string msg = std::string("invalid efConstruction = ") +
                            std::to_string(efConstruction);
          LOG(ERROR) << msg;
          return Status::ParamError(msg);
        }
        if (efConstruction > 0) this->efConstruction = efConstruction;
      }

      if (!jp_hnsw.GetInt("efSearch", efSearch)) {
        if (efSearch < -1) {
          std::string msg =
              std::string("invalid efSearch = ") + std::to_string(efSearch);
          LOG(ERROR) << msg;
          return Status::ParamError(msg);
        }
        if (efSearch > 0) this->efSearch = efSearch;
      }
    }

    if (!Validate()) return Status::ParamError();
    return Status::OK();
  }

  bool Validate() {
    if (ncentroids <= 0 || nb_bits <= 0 || nb_bits > 9 || qb < 0 || qb > 8) return false;

    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids=" << ncentroids << ", ";
    ss << "nb_bits=" << nb_bits << ", ";
    ss << "qb=" << qb << ", ";
    ss << "code_size=" << code_size << ", ";
    ss << "d=" << d << ", ";
    ss << "nprobe=" << nprobe << ", ";
    ss << "metric_type=" << (int)metric_type << ", ";
    ss << "bucket_init_size=" << bucket_init_size << ", ";
    ss << "bucket_max_size=" << bucket_max_size << ", ";
    ss << "training_threshold=" << training_threshold;

    if (has_hnsw) {
      ss << ", hnsw: nlinks=" << nlinks << ", ";
      ss << "efConstrction=" << efConstruction << ", ";
      ss << "efSearch=" << efSearch;
    }

    return ss.str();
  }

  int ToJson(utils::JsonParser &jp) { return 0; }
};

struct GammaIVFRABITQIndex : GammaFLATIndex, faiss::IndexIVFRaBitQ {
  GammaIVFRABITQIndex();

  virtual ~GammaIVFRABITQIndex();

  Status Init(const std::string &model_parameters,
              int training_threshold) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec) override;

  int Update(const std::vector<int64_t> &ids,
            const std::vector<const uint8_t *> &vecs) override;

  // assign the vectors, then call search_preassign
  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
            int k, float *distances, idx_t *labels) override;

  void search_preassigned(RetrievalContext *retrieval_context, idx_t n,
    const float *x, int k, const idx_t *keys,
    const float *coarse_dis, float *distances,
    idx_t *labels, int nprobe, bool store_pairs);

  long GetTotalMemBytes() override {
    if (!rt_invert_index_ptr_) {
      return 0;
    }
    return rt_invert_index_ptr_->GetTotalMemBytes();
  }

  Status Dump(const std::string &dir) override;

  Status Load(const std::string &index_dir, int64_t &load_num) override;

  int Delete(const std::vector<int64_t> &ids) override;

  void train(int64_t n, const float *x) override {
    faiss::IndexIVFRaBitQ::train(n, x);
  }

  faiss::InvertedListScanner* GetInvertedListScanner(
    bool store_pairs,
    const faiss::IDSelector* sel,
    uint8_t qb,
    bool centered,
    RetrievalContext* retrieval_context) {
    return new GammaRaBitInvertedListScanner(
          *this, store_pairs, sel, qb, centered, retrieval_context);
  }

  void Describe() override;

  int64_t indexed_vec_count_;
  realtime::RTInvertIndex *rt_invert_index_ptr_;
  bool compaction_;
  size_t compact_bucket_no_;
  uint64_t compacted_num_;
  uint64_t updated_num_;
  int d_;
  DistanceComputeType metric_type_;

  // 0 is FlatL2, 1 is HNSWFlat
  int quantizer_type_;
#ifdef PERFORMANCE_TESTING
  int add_count_;
#endif
  IVFRABITQModelParams *model_param_;
};

}  // namespace vearch
 