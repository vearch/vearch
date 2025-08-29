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
#include "gamma_gpu_cloner.h"
#include "gamma_gpu_search_base.h"
#include "index/impl/gamma_index_ivfpq.h"
#include "index/index_model.h"
#include "table/field_range_index.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"

namespace vearch {
namespace gpu {

class GPURetrievalParameters : public GPURetrievalParametersBase {
 public:
  GPURetrievalParameters() : GPURetrievalParametersBase() { recall_num_ = 100; }

  GPURetrievalParameters(size_t recall_num, size_t nprobe,
                         DistanceComputeType type)
      : GPURetrievalParametersBase(nprobe, type) {
    recall_num_ = recall_num;
  }

  GPURetrievalParameters(DistanceComputeType type)
      : GPURetrievalParametersBase() {
    recall_num_ = 100;
    distance_compute_type_ = type;
  }

  ~GPURetrievalParameters() {}

  int RecallNum() { return recall_num_; }
  void SetRecallNum(int recall_num) { recall_num_ = recall_num; }

 protected:
  int recall_num_;
};

class GammaIVFPQGPUIndex
    : public GammaGPUSearchBase<GammaIVFPQIndex, GPURetrievalParameters> {
 public:
  GammaIVFPQGPUIndex();

  virtual ~GammaIVFPQGPUIndex();

  Status Init(const std::string &model_parameters,
              int training_threshold) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  int AddRTVecsToIndex() override;

  bool Add(int n, const uint8_t *vec) override;

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *labels);

  int Delete(const std::vector<int64_t> &ids) override;

  long GetTotalMemBytes() override;

  Status Dump(const std::string &dir) override;
  Status Load(const std::string &index_dir, int64_t &load_num) override;

 protected:
  // Implement abstract methods from GammaGPUIndexBase
  faiss::Index *CreateGPUIndex() override;
  int CreateSearchThread() override;
  int GPUThread() override;

  // Implement abstract methods from GammaGPUSearchBase
  GPURetrievalParameters *CreateDefaultRetrievalParams(
      int default_nprobe) override;
  int GetRecallNum(GPURetrievalParameters *params, int k,
                   bool enable_rerank) override;
  int GetNprobe(GPURetrievalParameters *params, int default_nprobe,
                size_t nlist) override;

 private:
  size_t nlist_;
  int nprobe_;
};

}  // namespace gpu
}  // namespace vearch
