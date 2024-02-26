/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This faiss source code is licensed under the MIT license.
 * https://github.com/facebookresearch/faiss/blob/master/LICENSE
 *
 *
 * The works below are modified based on faiss:
 * 1. Add the numeric field and bitmap filters in the process of searching
 *
 * Modified works copyright 2019 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#pragma once

#include <string>
#include <vector>

#include "util/bitmap.h"
#include "faiss/Index.h"
#include "faiss/impl/FaissAssert.h"
#include "faiss/utils/Heap.h"
#include "faiss/utils/distances.h"
#include "faiss/utils/hamming.h"
#include "faiss/utils/utils.h"
#include "table/field_range_index.h"
#include "common/gamma_common_data.h"
#include "index/retrieval_model.h"
#include "util/log.h"
#include "util/utils.h"

namespace tig_gamma {

class FlatRetrievalParameters : public RetrievalParameters {
 public:
  FlatRetrievalParameters() : RetrievalParameters() {
    parallel_on_queries_ = true;
  }

  FlatRetrievalParameters(bool parallel_on_queries,
                          enum DistanceComputeType type) {
    parallel_on_queries_ = parallel_on_queries;
    distance_compute_type_ = type;
  }

  FlatRetrievalParameters(enum DistanceComputeType type) {
    parallel_on_queries_ = true;
    distance_compute_type_ = type;
  }

  ~FlatRetrievalParameters() {}

  bool ParallelOnQueries() { return parallel_on_queries_; }
  void SetParallelOnQueries(bool parallel_on_queries) { 
      parallel_on_queries_ = parallel_on_queries; 
  }
 private:
  // parallelize over queries or vectors
  bool parallel_on_queries_;
};

class GammaFLATIndex : public RetrievalModel {
 public:

  GammaFLATIndex();

  ~GammaFLATIndex();

  int Init(const std::string &model_parameters, int indexing_size) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec) override;

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;

  int Delete(const std::vector<int64_t> &ids) override { return 0; };

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *labels) override;

  long GetTotalMemBytes() override;

  int Dump(const std::string &dir) override;

  int Load(const std::string &index_dir) override;

  DistanceComputeType metric_type_;
  
  int rerank_ = 0;
};

}  // namespace tig_gamma
