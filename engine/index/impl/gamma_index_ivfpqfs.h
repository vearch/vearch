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

#ifndef GAMMA_INDEX_IVFPQFS_H_
#define GAMMA_INDEX_IVFPQFS_H_

#include <unistd.h>
#include <atomic>

#include "faiss/IndexIVF.h"
#include "faiss/IndexIVFPQFastScan.h"
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
#include "index/impl/gamma_index_ivfpq.h"
#include "util/utils.h"

namespace tig_gamma {

struct IVFPQFastScanModelParams : IVFPQModelParams {
  int bbs;
  IVFPQFastScanModelParams() : IVFPQModelParams() {
    bbs = 32;
    nbits_per_idx = 4;
  }
    int Parse(const char *str) {
    utils::JsonParser jp;
    if (jp.Parse(str)) {
      LOG(ERROR) << "parse IVFPQ retrieval parameters error: " << str;
      return -1;
    }

    int ncentroids;
    int nsubvector;
    //int nbits_per_idx;
    int nprobe;
    int bbs;

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
        LOG(ERROR) << "invalid nprobe =" << nprobe;
        return -1;
      }
      if (nprobe > 0) this->nprobe = nprobe;
      if (this->nprobe > this->ncentroids) {
        LOG(ERROR) << "nprobe should less than ncentroids";
        return -1;
      }
    }

    if (!jp.GetInt("bbs", bbs)) {
      if (bbs < -1) {
        LOG(ERROR) << "invalid bbs =" << bbs;
        return -1;
      }
      if (bbs > 0) this->bbs = bbs;
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
    if (nbits_per_idx != 4) {
      LOG(ERROR) << "only support 4 now, nbits_per_idx=" << nbits_per_idx;
      return false;
    }
    if (bbs % 32 != 0) {
      LOG(ERROR) << "bbs only supports the case where 32 can be divided evenly, bbs=" << bbs;
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
};

struct GammaIVFPQFastScanIndex : GammaFLATIndex, faiss::IndexIVFPQFastScan {
  GammaIVFPQFastScanIndex();

  virtual ~GammaIVFPQFastScanIndex();

  int Init(const std::string &model_parameters, int indexing_size) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec);

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs);

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, idx_t *labels);

  long GetTotalMemBytes() override {
    if (!rt_invert_index_ptr_) {
      return 0;
    }
    return rt_invert_index_ptr_->GetTotalMemBytes();
  }

  int Dump(const std::string &dir) override;

  int Load(const std::string &index_dir) override;

  int Delete(const std::vector<int64_t> &ids);

  void train(int64_t n, const float *x) { faiss::IndexIVFPQFastScan::train(n, x); }

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
  std::atomic<uint64_t> search_count_;
  int add_count_;
#endif
  IVFPQFastScanModelParams *model_param_;
};

}  // namespace tig_gamma

#endif
