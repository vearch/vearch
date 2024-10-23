/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "common/common_query_data.h"
#include "idl/fbs-gen/c/response_generated.h"
#include "raw_data.h"
#include "util/status.h"

namespace vearch {

enum class SearchResultCode : std::uint16_t {
  SUCCESS = 0,
  INDEX_NOT_TRAINED,
  SEARCH_ERROR
};

struct ResultItem {
  ResultItem() : score(-1) {}

  ResultItem(const ResultItem &other) = default;
  ResultItem &operator=(const ResultItem &other) = default;

  ResultItem(ResultItem &&other) noexcept = default;
  ResultItem &operator=(ResultItem &&other) noexcept = default;

  double score;
  std::vector<std::string> names;
  std::vector<std::string> values;
};

struct SearchResult {
  SearchResult() : total(0) {}

  SearchResult(const SearchResult &other) = default;
  SearchResult &operator=(const SearchResult &other) = default;

  SearchResult(SearchResult &&other) noexcept = default;
  SearchResult &operator=(SearchResult &&other) noexcept = default;

  int total;
  SearchResultCode result_code;
  std::string msg;
  std::vector<ResultItem> result_items;
};

class Response : public RawData {
 public:
  Response();
  explicit Response(bool trace);

  virtual ~Response();

  virtual int Serialize(char **out, int *out_len) { return 0; }

  virtual int Serialize(const std::string &space_name,
                        std::vector<std::string> &fields_name, Status &status,
                        char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  void AddResults(const SearchResult &result);
  void AddResults(SearchResult &&result);

  std::vector<SearchResult> &Results();

  void SetEngineInfo(void *table, void *vector_mgr, GammaResult *gamma_results,
                     int req_num);

  void *GetPerfTool() { return perf_tool_; }

  int PackResults(std::vector<std::string> &fields_name);

  int PackResultItem(const VectorDoc *vec_doc,
                     std::vector<std::string> &fields_name,
                     ResultItem &result_item);

 private:
  gamma_api::Response *response_ = nullptr;
  std::vector<SearchResult> results_;
  GammaResult *gamma_results_ = nullptr;
  void *table_ = nullptr;
  void *vector_mgr_ = nullptr;
  int req_num_ = 0;
  void *perf_tool_ = nullptr;
};

}  // namespace vearch
