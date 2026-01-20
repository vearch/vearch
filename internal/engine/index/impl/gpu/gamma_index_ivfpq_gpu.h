/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <shared_mutex>

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "faiss/Index.h"
#include "gamma_gpu_search_base.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "index/index_model.h"
#include "table/field_range_index.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"

namespace vearch {
namespace gpu {

class IVFPQGPURetrievalParameters : public GPURetrievalParametersBase {
 public:
  IVFPQGPURetrievalParameters() : GPURetrievalParametersBase() { recall_num_ = 100; }

  IVFPQGPURetrievalParameters(size_t recall_num, size_t nprobe,
                         DistanceComputeType type)
      : GPURetrievalParametersBase(nprobe, type) {
    recall_num_ = recall_num;
  }

  IVFPQGPURetrievalParameters(DistanceComputeType type)
      : GPURetrievalParametersBase() {
    recall_num_ = 100;
    distance_compute_type_ = type;
  }

  virtual ~IVFPQGPURetrievalParameters() = default;

  int RecallNum() { return recall_num_; }
  void SetRecallNum(int recall_num) { recall_num_ = recall_num; }

 protected:
  int recall_num_;
};

class GammaIVFPQGPUIndex
    : public GammaGPUSearchBase<GammaIVFPQIndex, IVFPQGPURetrievalParameters> {
 public:
  GammaIVFPQGPUIndex();

  virtual ~GammaIVFPQGPUIndex();

  Status Init(const std::string &model_parameters,
              int training_threshold) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec) override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *labels);

 protected:
  // Implement abstract methods from GammaGPUIndexBase
  faiss::Index *CreateGPUIndex() override;
  int CreateSearchThread() override;
  int GPUThread() override;

  // Implement abstract methods from GammaGPUSearchBase
  IVFPQGPURetrievalParameters *CreateDefaultRetrievalParams(
      int default_nprobe) override;
  int GetRecallNum(IVFPQGPURetrievalParameters *params, int k,
                   bool enable_rerank) override;
  int GetNprobe(IVFPQGPURetrievalParameters *params, int default_nprobe,
                size_t nlist) override;

 private:
  size_t nlist_;
  int nprobe_;
  int nsubvector_;     // number of sub cluster center
  int nbits_per_idx_;  // bit number of sub cluster center
};

}  // namespace gpu
}  // namespace vearch
