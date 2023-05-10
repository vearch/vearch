/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <pthread.h>

#include <algorithm>
#include <string>
#include <vector>

#include "util/bitmap.h"
#include "table/field_range_index.h"
#include "common/gamma_common_data.h"
#include "index/impl/gamma_index_flat.h"
#include "hnswlib.h"
#include "util/log.h"
#include "vector/raw_vector.h"
#include "index/retrieval_model.h"
#include "vector/memory_raw_vector.h"

using namespace hnswlib;

namespace tig_gamma {

class HNSWLIBRetrievalParameters : public RetrievalParameters {
 public:
  HNSWLIBRetrievalParameters() : RetrievalParameters() { 
    efSearch_ = 64; 
    do_efSearch_check_ = 1;
  }

  HNSWLIBRetrievalParameters(enum DistanceComputeType type, int efSearch, int do_efSearch_check) {
    distance_compute_type_ = type;
    efSearch_ = efSearch;
    do_efSearch_check_ = do_efSearch_check;
  }

  ~HNSWLIBRetrievalParameters() {}

  int EfSearch() { return efSearch_; }

  void SetEfSearch(int efSearch) { efSearch_ = efSearch; }

  int DoEfSearchCheck() { return do_efSearch_check_; }
  void SetDoEfSearchCheck(int do_efSearch_check) { do_efSearch_check_ = do_efSearch_check; }

 private:
  int efSearch_;
  int do_efSearch_check_;
};

struct GammaIndexHNSWLIB : public GammaFLATIndex,
                           hnswlib::HierarchicalNSW<float> {
  GammaIndexHNSWLIB();

  GammaIndexHNSWLIB(VectorReader *vec);

  virtual ~GammaIndexHNSWLIB();

  int Init(const std::string &model_parameters, int indexing_size) override;

  RetrievalParameters *Parse(const std::string &parameters) override;

  int Indexing() override;

  bool Add(int n, const uint8_t *vec) override;

  int AddVertices(size_t n0, size_t n, const float *x);

  int Update(const std::vector<int64_t> &ids,
             const std::vector<const uint8_t *> &vecs) override;

  int Delete(const std::vector<int64_t> &ids);

  int Search(RetrievalContext *retrieval_context, int n, const uint8_t *x,
             int k, float *distances, int64_t *labels);

  long GetTotalMemBytes() override;

  int Dump(const std::string &dir) override;

  int Load(const std::string &index_dir) override;

  /*
  virtual char *getDataByInternalId(tableint internal_id) const override {
    ScopeVector svec;
    dynamic_cast<const RawVector *>(vector_)->GetVector(internal_id, svec);
    return (char *)svec.Get();
  }
  */
 
  virtual char *getDataByInternalId(tableint internal_id) const override {
    return (char *)raw_vec_->GetFromMem(internal_id);
  }

  int indexed_vec_count_;
  int updated_num_;
  int deleted_num_;

  int d;
  int ntotal;
  SpaceInterface<float> *space_interface_ = nullptr;
  SpaceInterface<float> *space_interface_ip_ = nullptr;
  DistanceComputeType metric_type_;
  int do_efSearch_check_;
  MemoryRawVector *raw_vec_ = nullptr;

  // for dump
  std::mutex dump_mutex_;

#ifdef PERFORMANCE_TESTING
  int add_count_ = 0;
#endif
};

}  // namespace tig_gamma
