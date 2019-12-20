/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef GAMMA_COMMON_DATA_H_
#define GAMMA_COMMON_DATA_H_

#include "field_range_index.h"
#include "gamma_api.h"
#include "log.h"
#include "online_logger.h"

namespace tig_gamma {

const std::string EXTRA_VECTOR_FIELD_SOURCE = "source";
const std::string EXTRA_VECTOR_FIELD_SCORE = "score";
const std::string EXTRA_VECTOR_FIELD_NAME = "field";
const std::string EXTRA_VECTOR_RESULT = "vector_result";

const float GAMMA_INDEX_RECALL_RATIO = 1.0f;

enum class ResultCode : std::uint16_t {
#define DefineResultCode(Name, Value) Name = Value,
#include "definition_list.h"
#undef DefineResultCode
  Undefined
};

enum VectorStorageType {Mmap, RocksDB};
enum RetrievalModel { IVFPQ };

struct VectorDocField {
  std::string name;
  double score;
  char *source;
  int source_len;
};

struct VectorDoc {
  VectorDoc() {
    docid = -1;
    score = 0.0f;
  }

  ~VectorDoc() {
    if (fields) {
      delete[] fields;
      fields = nullptr;
    }
  }

  bool init(std::string *vec_names, int vec_num) {
    if (vec_num <= 0) {
      fields = nullptr;
      fields_len = 0;
      return true;
    }
    fields = new (std::nothrow) VectorDocField[vec_num];
    if (fields == nullptr) {
      return false;
    }
    for (int i = 0; i < vec_num; i++) {
      fields[i].name = vec_names[i];
    }
    fields_len = vec_num;
    return true;
  }

  int docid;
  double score;
  struct VectorDocField *fields;
  int fields_len;
};

struct GammaSearchCondition {
  GammaSearchCondition() {
    range_query_result = nullptr;
    topn = 0;
    has_rank = false;
    multi_vector_rank = false;
    metric_type = InnerProduct;
    sort_by_docid = false;
    min_dist = -1;
    max_dist = -1;
    recall_num = 0;
    parallel_mode = 1;  // default to parallelize over inverted list
    use_direct_search = false;
  }

  GammaSearchCondition(GammaSearchCondition *condition) {
    range_query_result = condition->range_query_result;
    topn = condition->topn;
    has_rank = condition->has_rank;
    multi_vector_rank = condition->multi_vector_rank;
    metric_type = condition->metric_type;
    sort_by_docid = condition->sort_by_docid;
    min_dist = condition->min_dist;
    max_dist = condition->max_dist;
    recall_num = condition->recall_num;
    parallel_mode = condition->parallel_mode;
    use_direct_search = condition->use_direct_search;
  }

  ~GammaSearchCondition() {
    range_query_result = nullptr;  // should not delete
  }

  MultiRangeQueryResults *range_query_result;

  int topn;
  bool has_rank;
  bool multi_vector_rank;
  DistanceMetricType metric_type;
  bool sort_by_docid;
  float min_dist;
  float max_dist;
  int recall_num;
  int parallel_mode;
  bool use_direct_search;
};

struct GammaQuery {
  GammaQuery() {
    vec_query = nullptr;
    vec_num = 0;
    condition = nullptr;
    logger = nullptr;
  }

  ~GammaQuery() {}
  VectorQuery **vec_query;
  int vec_num;
  GammaSearchCondition *condition;
  utils::OnlineLogger *logger;
};

struct GammaResult {
  GammaResult() {
    topn = 0;
    total = 0;
    results_count = 0;
    docs = nullptr;
  }
  ~GammaResult() {
    if (docs) {
      for (int i = 0; i < topn; i++) {
        if (docs[i]) {
          delete docs[i];
          docs[i] = nullptr;
        }
      }
      delete[] docs;
      docs = nullptr;
    }
  }

  bool init(int n, std::string *vec_names, int vec_num) {
    topn = n;
    docs = new (std::nothrow) VectorDoc*[topn];
    if (!docs) {
      // LOG(ERROR) << "docs in CommonDocs init error!";
      return false;
    }
    for (int i = 0; i < n; i++) {
      docs[i] = new VectorDoc();
      if (!docs[i]->init(vec_names, vec_num)) {
        return false;
      }
    }
    return true;
  }

  int topn;
  int total;
  int results_count;

  VectorDoc **docs;
};

struct IVFPQParamHelper {
  IVFPQParamHelper(IVFPQParameters *ivfpq_param) { ivfpq_param_ = ivfpq_param; }
  void SetDefaultValue() {
    if (ivfpq_param_->metric_type == -1)
      ivfpq_param_->metric_type = InnerProduct;
    if (ivfpq_param_->nprobe == -1) ivfpq_param_->nprobe = 20;
    if (ivfpq_param_->ncentroids == -1) ivfpq_param_->ncentroids = 256;
    if (ivfpq_param_->nsubvector == -1) ivfpq_param_->nsubvector = 64;
    if (ivfpq_param_->nbits_per_idx == -1) ivfpq_param_->nbits_per_idx = 8;
  }

  bool Validate() {
    if (ivfpq_param_->metric_type < InnerProduct ||
        ivfpq_param_->metric_type > L2 || ivfpq_param_->nprobe <= 0 ||
        ivfpq_param_->ncentroids <= 0 || ivfpq_param_->nsubvector <= 0 ||
        ivfpq_param_->nbits_per_idx <= 0)
      return false;
    if (ivfpq_param_->nsubvector % 4 != 0) {
      LOG(ERROR) << "only support multiple of 4 now, nsubvector="
                 << ivfpq_param_->nsubvector;
      return false;
    }
    if (ivfpq_param_->nbits_per_idx != 8) {
      LOG(ERROR) << "only support 8 now, nbits_per_idx="
                 << ivfpq_param_->nbits_per_idx;
      return false;
    }
    if (ivfpq_param_->nprobe > ivfpq_param_->ncentroids) {
      LOG(ERROR) << "nprobe=" << ivfpq_param_->nprobe
                 << " > ncentroids=" << ivfpq_param_->ncentroids;
      return false;
    }
    return true;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "ivfpq parameters: metric_type=" << ivfpq_param_->metric_type
       << ", nprobe=" << ivfpq_param_->nprobe
       << ", ncentroids=" << ivfpq_param_->ncentroids
       << ", nsubvector=" << ivfpq_param_->nsubvector
       << ", nbits_per_idx=" << ivfpq_param_->nbits_per_idx;
    return ss.str();
  }

  IVFPQParameters *ivfpq_param_;
};

}  // namespace tig_gamma

#endif
