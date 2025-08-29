/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <shared_mutex>

#include "gamma_gpu_cloner.h"
#include "gamma_gpu_search_base.h"
#include "index/impl/gamma_index_ivfflat.h"
#include "third_party/nlohmann/json.hpp"

namespace vearch {
namespace gpu {

/**
 * IVFFLAT GPU retrieval parameters
 */
class IVFFlatGPURetrievalParameters : public GPURetrievalParametersBase {
 public:
  IVFFlatGPURetrievalParameters() : GPURetrievalParametersBase() {
    parallel_on_queries_ = true;
  }

  IVFFlatGPURetrievalParameters(int nprobe, bool parallel_on_queries,
                                DistanceComputeType type)
      : GPURetrievalParametersBase(nprobe, type),
        parallel_on_queries_(parallel_on_queries) {}

  virtual ~IVFFlatGPURetrievalParameters() = default;

  bool ParallelOnQueries() const { return parallel_on_queries_; }
  void SetParallelOnQueries(bool parallel_on_queries) {
    parallel_on_queries_ = parallel_on_queries;
  }

 private:
  bool parallel_on_queries_;
};

/**
 * IVFFLAT model parameters
 */
struct IVFFlatGPUModelParams {
  int ncentroids;
  int nprobe;
  DistanceComputeType metric_type;
  int bucket_init_size;
  int bucket_max_size;
  int training_threshold;

  IVFFlatGPUModelParams() {
    ncentroids = 2048;
    nprobe = 80;
    metric_type = DistanceComputeType::INNER_PRODUCT;
    bucket_init_size = 1000;
    bucket_max_size = 1000000;
    training_threshold = 100000;
  }

  Status Parse(const char *str) {
    if (!str || strlen(str) == 0) return Status::OK();

    nlohmann::json j;
    try {
      j = nlohmann::json::parse(str);
    } catch (const nlohmann::json::parse_error &e) {
      LOG(ERROR) << "Parse IVFFLAT GPU model parameters error: " << e.what();
      return Status::ParamError("Parse model parameters error");
    }

    if (j.contains("ncentroids")) {
      ncentroids = j.value("ncentroids", ncentroids);
    }

    if (j.contains("nprobe")) {
      nprobe = j.value("nprobe", nprobe);
    }

    if (j.contains("bucket_init_size")) {
      bucket_init_size = j.value("bucket_init_size", bucket_init_size);
    }

    if (j.contains("bucket_max_size")) {
      bucket_max_size = j.value("bucket_max_size", bucket_max_size);
    }

    std::string metric_type_str;
    if (j.contains("metric_type")) {
      metric_type_str = j.value("metric_type", "");
      if (strcasecmp("L2", metric_type_str.c_str()) == 0) {
        metric_type = DistanceComputeType::L2;
      } else if (strcasecmp("InnerProduct", metric_type_str.c_str()) == 0) {
        metric_type = DistanceComputeType::INNER_PRODUCT;
      }
    }

    if (!Validate()) return Status::ParamError("Invalid parameters");
    return Status::OK();
  }

  bool Validate() {
    if (ncentroids <= 0 || nprobe <= 0) return false;
    if (bucket_init_size <= 0 || bucket_max_size <= 0) return false;
    if (bucket_init_size > bucket_max_size) return false;
    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ncentroids=" << ncentroids << ", ";
    ss << "nprobe=" << nprobe << ", ";
    ss << "bucket_init_size=" << bucket_init_size << ", ";
    ss << "bucket_max_size=" << bucket_max_size << ", ";
    ss << "training_threshold=" << training_threshold;
    return ss.str();
  }
};

/**
 * GPU IVFFLAT Index Implementation
 */
class GammaIVFFlatGPUIndex
    : public GammaGPUSearchBase<GammaIVFFlatIndex,
                                IVFFlatGPURetrievalParameters> {
 public:
  GammaIVFFlatGPUIndex();
  virtual ~GammaIVFFlatGPUIndex();

  Status Init(const std::string &model_parameters,
              int training_threshold) override;
  RetrievalParameters *Parse(const std::string &parameters) override;
  int Indexing() override;

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, long *labels);

 protected:
  // Implement abstract methods from GammaGPUIndexBase
  faiss::Index *CreateGPUIndex() override;
  int CreateSearchThread() override;
  int AddRTVecsToIndex() override;
  int GPUThread() override;

  // Implement abstract methods from GammaGPUSearchBase
  IVFFlatGPURetrievalParameters *CreateDefaultRetrievalParams(
      int default_nprobe) override;
  int GetRecallNum(IVFFlatGPURetrievalParameters *params, int k,
                   bool enable_rerank) override;
  int GetNprobe(IVFFlatGPURetrievalParameters *params, int default_nprobe,
                size_t nlist) override;

 private:
  size_t nlist_;
  int nprobe_;
};

}  // namespace gpu
}  // namespace vearch
