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

#include "common/gamma_common_data.h"
#include "faiss/IndexHNSW.h"
#include "faiss/IndexIVF.h"
#include "faiss/IndexIVFPQFastScan.h"
#include "faiss/VectorTransform.h"
#include "faiss/impl/FaissAssert.h"
#include "faiss/impl/io.h"
#include "faiss/index_io.h"
#include "faiss/invlists/DirectMap.h"
#include "faiss/invlists/InvertedLists.h"
#include "faiss/utils/Heap.h"
#include "faiss/utils/distances.h"
#include "faiss/utils/hamming.h"
#include "faiss/utils/utils.h"
#include "gamma_index_flat.h"
#include "gamma_scanner.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "index/index_model.h"
#include "index/realtime/realtime_invert_index.h"
#include "table/field_range_index.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/memory_raw_vector.h"
#include "vector/raw_vector.h"

namespace vearch {

struct IVFPQFastScanModelParams : IVFPQModelParams {
  int bbs;
  IVFPQFastScanModelParams() : IVFPQModelParams() {
    bbs = 32;
    nbits_per_idx = 4;
  }
  Status Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      std::string msg =
          std::string("parse IVFPQ retrieval parameters error: ") + str;
      LOG(ERROR) << msg;
      return Status::ParamError(msg);
    }

    int ncentroids;
    int nsubvector;
    // int nbits_per_idx;
    int nprobe;
    int bbs;

    // -1 as default
    if (!jp.GetInt("ncentroids", ncentroids)) {
      if (ncentroids < -1) {
        std::string msg =
            std::string("invalid ncentroids =") + std::to_string(ncentroids);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (ncentroids > 0) this->ncentroids = ncentroids;
    } else {
      std::string msg =
          "cannot get ncentroids for ivfpq, set it when create space";
      LOG(ERROR) << msg;
      return Status::ParamError(msg);
    }

    if (!jp.GetInt("nsubvector", nsubvector)) {
      if (nsubvector < -1) {
        std::string msg =
            std::string("invalid nsubvector =") + std::to_string(nsubvector);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (nsubvector > 0) this->nsubvector = nsubvector;
    } else {
      std::string msg =
          "cannot get nsubvector for ivfpq, set it when create space";
      LOG(ERROR) << msg;
      return Status::ParamError(msg);
    }
    /*
    if (!jp.GetInt("nbits_per_idx", nbits_per_idx)) {
      if (nbits_per_idx < -1) {
        LOG(ERROR) << "invalid nbits_per_idx =" << nbits_per_idx;
        return -1;
      }
      if (nbits_per_idx > 0) this->nbits_per_idx = nbits_per_idx;
    }
    */
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

    if (!jp.GetInt("bbs", bbs)) {
      if (bbs < -1) {
        std::string msg = std::string("invalid bbs =") + std::to_string(bbs);
        LOG(ERROR) << msg;
        return Status::ParamError(msg);
      }
      if (bbs > 0) this->bbs = bbs;
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

    utils::JsonParser jp_opq;
    if (!jp.GetObject("opq", jp_opq)) {
      has_opq = true;
      int opq_nsubvector;
      // -1 as default
      if (!jp_opq.GetInt("nsubvector", opq_nsubvector)) {
        if (nsubvector < -1) {
          std::string msg = std::string("invalid opq_nsubvector = ") +
                            std::to_string(opq_nsubvector);
          LOG(ERROR) << msg;
          return Status::ParamError(msg);
        }
        if (opq_nsubvector > 0) this->opq_nsubvector = opq_nsubvector;
      }
    }

    if (!Validate()) return Status::ParamError();
    return Status::OK();
  }

  bool Validate() {
    if (ncentroids <= 0 || nsubvector <= 0 || nbits_per_idx <= 0) return false;
    if (nbits_per_idx != 4) {
      LOG(ERROR) << "only support 4 now, nbits_per_idx=" << nbits_per_idx;
      return false;
    }
    if (bbs % 32 != 0) {
      LOG(ERROR)
          << "bbs only supports the case where 32 can be divided evenly, bbs="
          << bbs;
      return false;
    }

    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids =" << ncentroids << ", ";
    ss << "nsubvector =" << nsubvector << ", ";
    ss << "nbits_per_idx =" << nbits_per_idx << ", ";
    ss << "bbs =" << bbs << ", ";
    ss << "nprobe =" << nprobe << ", ";
    ss << "metric_type =" << (int)metric_type << ", ";
    ss << "bucket_init_size =" << bucket_init_size << ", ";
    ss << "bucket_max_size =" << bucket_max_size << ", ";
    ss << "training_threshold = " << training_threshold;

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
};

struct GammaIVFPQFastScanIndex : GammaFLATIndex, faiss::IndexIVFPQFastScan {
  GammaIVFPQFastScanIndex();

  virtual ~GammaIVFPQFastScanIndex();

  Status Init(const std::string &model_parameters,
              int training_threshold) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec) override;

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, idx_t *labels) override;

  long GetTotalMemBytes() override {
    if (!rt_invert_index_ptr_) {
      return 0;
    }
    return rt_invert_index_ptr_->GetTotalMemBytes();
  }

  Status Dump(const std::string &dir) override;

  Status Load(const std::string &index_dir, int &load_num) override;

  int Delete(const std::vector<int64_t> &ids) override;

  void train(int64_t n, const float *x) override {
    faiss::IndexIVFPQFastScan::train(n, x);
  }

  int indexed_vec_count_;
  realtime::RTInvertIndex *rt_invert_index_ptr_ = nullptr;
  bool compaction_;
  size_t compact_bucket_no_;
  uint64_t compacted_num_;
  uint64_t updated_num_;
  int d_;
  DistanceComputeType metric_type_;
  pthread_rwlock_t shared_mutex_;
  faiss::VectorTransform *opq_;
  // 0 is FlatL2, 1 is HNSWFlat
  int quantizer_type_;
#ifdef PERFORMANCE_TESTING
  int add_count_;
#endif
  IVFPQFastScanModelParams *model_param_;
};

}  // namespace vearch
