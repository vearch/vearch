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
#include "index/impl/gamma_index_ivfpq.h"
#include "index/index_model.h"
#include "table/field_range_index.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"

namespace vearch {
namespace gpu {

class GPUItem;

class GPURetrievalParameters : public RetrievalParameters {
 public:
  GPURetrievalParameters() : RetrievalParameters() {
    recall_num_ = 100;
    nprobe_ = 80;
  }

  GPURetrievalParameters(size_t recall_num, size_t nprobe,
                         DistanceComputeType type) {
    recall_num_ = recall_num;
    nprobe_ = nprobe;
    distance_compute_type_ = type;
  }

  GPURetrievalParameters(DistanceComputeType type) {
    recall_num_ = 100;
    nprobe_ = 80;
    distance_compute_type_ = type;
  }

  ~GPURetrievalParameters() {}

  int RecallNum() { return recall_num_; }
  void SetRecallNum(int recall_num) { recall_num_ = recall_num; }
  int Nprobe() { return nprobe_; }
  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

 protected:
  int recall_num_;
  int nprobe_;
};

class GammaIVFPQGPUIndex : public IndexModel {
 public:
  GammaIVFPQGPUIndex();

  virtual ~GammaIVFPQGPUIndex();

  Status Init(const std::string &model_parameters, int training_threshold);

  RetrievalParameters *Parse(const std::string &parameters);

  int Indexing() override;

  int AddRTVecsToIndex();

  bool Add(int n, const uint8_t *vec) override;

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *labels);

  int Delete(const std::vector<int64_t> &ids) override;

  long GetTotalMemBytes() override;

  Status Dump(const std::string &dir) override;
  Status Load(const std::string &index_dir, int64_t &load_num) override;

 private:
  int GPUThread();

  faiss::Index *CreateGPUIndex();

  int CreateSearchThread();

  size_t nlist_;
  int nprobe_;

  moodycamel::BlockingConcurrentQueue<GPUItem *> id_queue_;

  faiss::Index *gpu_index_;
  GammaIVFPQIndex *cpu_index_;

  std::vector<faiss::gpu::StandardGpuResources *> resources_;
  std::vector<std::thread> gpu_threads_;

  bool b_exited_;

  bool is_trained_;

  int d_;
  DistanceComputeType metric_type_;
  std::mutex cpu_mutex_;
  std::mutex indexing_mutex_;
  std::shared_mutex gpu_index_mutex_;
};

}  // namespace gpu
}  // namespace vearch
