/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>

#include "common/common_query_data.h"
#include "index/index_model.h"
#include "table/field_range_index.h"
#include "table/table.h"
#include "util/log.h"
#include "util/utils.h"
#include "vector/raw_vector.h"

namespace vearch {

const float GAMMA_INDEX_RECALL_RATIO = 1.0f;

const int min_points_per_centroid = 39;
const int default_points_per_centroid = 100;
const int max_points_per_centroid = 256;

const int brute_force_search_threshold = 100;

enum class VectorStorageType : std::uint8_t { MemoryOnly, RocksDB };

class SearchCondition : public RetrievalContext {
 public:
  SearchCondition(PerfTool *perf_tool) {
    range_query_result = nullptr;
    topn = 0;
    multi_vector_rank = false;
    metric_type = DistanceComputeType::INNER_PRODUCT;
    sort_by_docid = false;
    brute_force_search = false;
    l2_sqrt = false;
    min_score = std::numeric_limits<float>::min();
    max_score = std::numeric_limits<float>::max();
    perf_tool_ = perf_tool;
    table = nullptr;
    ranker = nullptr;
  }

  SearchCondition(SearchCondition *condition) {
    range_query_result = condition->range_query_result;
    topn = condition->topn;
    multi_vector_rank = condition->multi_vector_rank;
    metric_type = condition->metric_type;
    sort_by_docid = condition->sort_by_docid;
    brute_force_search = condition->brute_force_search;
    l2_sqrt = condition->l2_sqrt;
    perf_tool_ = condition->perf_tool_;

    range_filters = condition->range_filters;
    term_filters = condition->term_filters;
    table = condition->table;
    ranker = condition->ranker;
  }

  ~SearchCondition() {
    range_query_result = nullptr;  // should not delete
    table = nullptr;               // should not delete
    ranker = nullptr;              // should not delete
  }

  MultiRangeQueryResults *range_query_result;

  int filter_operator;
  std::vector<struct RangeFilter> range_filters;
  std::vector<struct TermFilter> term_filters;

  Table *table;

  int topn;
  bool multi_vector_rank;
  enum DistanceComputeType metric_type;
  bool sort_by_docid;
  bool brute_force_search;
  bool l2_sqrt;
  std::string index_params;
  float min_score;
  float max_score;
  Ranker *ranker;

  bool IsSimilarScoreValid(float score) const override {
    return (score <= max_score) && (score >= min_score);
  };

  bool IsValid(int64_t id) const override {
#ifndef FAISSLIKE_INDEX
    if ((range_query_result != nullptr && not range_query_result->Has(id)) ||
        docids_bitmap->Test(id)) {
      return false;
    }
#endif
    return true;
  };

  void Init(float min_score, float max_score,
            bitmap::BitmapManager *docids_bitmap, RawVector *raw_vec) {
    this->min_score = min_score;
    this->max_score = max_score;
    this->docids_bitmap = docids_bitmap;
    this->raw_vec = raw_vec;
  }

  MultiRangeQueryResults *RangeQueryResult() { return range_query_result; }

 private:
  bitmap::BitmapManager *docids_bitmap;
  const RawVector *raw_vec;
};

struct GammaQuery {
  GammaQuery() { condition = nullptr; }

  ~GammaQuery() {
    if (condition) {
      delete condition;
      condition = nullptr;
    }
  }

  std::vector<struct VectorQuery> vec_query;
  SearchCondition *condition;
};

}  // namespace vearch
