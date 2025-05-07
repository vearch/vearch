/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "request.h"

#include "idl/pb-gen/raftcmd.pb.h"
#include "util/status.h"

namespace vearch {

int Request::Serialize(char **out, int *out_len) { return 0; }

void Request::Deserialize(const char *data, int len) {
  vearchpb::SearchRequest sr;
  bool parse_success = sr.ParseFromString(std::string(data, len));
  if (not parse_success) {
    LOG(ERROR) << "parse search request failed";
    return;
  }

  auto it_request_id = sr.head().params().find("request_id");
  if (it_request_id != sr.head().params().end()) {
    request_id_ = it_request_id->second;
  }

  req_num_ = sr.req_num();
  topn_ = sr.topn();
  brute_force_search_ = sr.is_brute_search();

  for (int i = 0; i < sr.vec_fields_size(); i++) {
    auto fbs_vector_query = sr.vec_fields(i);
    struct VectorQuery vector_query;
    vector_query.name = fbs_vector_query.name();
    vector_query.value = fbs_vector_query.value();
    vector_query.min_score = fbs_vector_query.min_score();
    vector_query.max_score = fbs_vector_query.max_score();
    vector_query.index_type = fbs_vector_query.index_type();

    vec_fields_.emplace_back(vector_query);
  }

  for (int i = 0; i < sr.fields_size(); i++) {
    auto fbs_field = sr.fields(i);
    fields_.emplace_back(fbs_field);
  }

  for (int i = 0; i < sr.range_filters_size(); i++) {
    auto fbs_range_filter = sr.range_filters(i);
    struct RangeFilter range_filter;
    range_filter.field = fbs_range_filter.field();
    range_filter.lower_value = fbs_range_filter.lower_value();
    range_filter.upper_value = fbs_range_filter.upper_value();
    range_filter.include_lower = fbs_range_filter.include_lower();
    range_filter.include_upper = fbs_range_filter.include_upper();
    range_filter.is_union = fbs_range_filter.is_union();

    range_filters_.emplace_back(range_filter);
  }

  for (int i = 0; i < sr.term_filters_size(); i++) {
    auto fbs_term_filter = sr.term_filters(i);
    struct TermFilter term_filter;
    term_filter.field = fbs_term_filter.field();
    term_filter.value = fbs_term_filter.value();
    term_filter.is_union = fbs_term_filter.is_union();

    term_filters_.emplace_back(term_filter);
  }

  filter_operator_ = sr.operator_();
  index_params_ = sr.index_params();
  multi_vector_rank_ = sr.multi_vector_rank();
  l2_sqrt_ = sr.l2_sqrt();
  trace_ = sr.trace();

  if (vec_fields_.size() > 1 && sr.ranker() != "") {
    ranker_ = new WeightedRanker(sr.ranker(), vec_fields_.size());
  }
}

int Request::ReqNum() { return req_num_; }

void Request::SetReqNum(int req_num) { req_num_ = req_num; }

int Request::TopN() { return topn_; }

void Request::SetTopN(int topn) { topn_ = topn; }

int Request::BruteForceSearch() { return brute_force_search_; }

void Request::SetBruteForceSearch(int brute_force_search) {
  brute_force_search_ = brute_force_search;
}

void Request::AddVectorQuery(struct VectorQuery &vec_fields) {
  vec_fields_.emplace_back(vec_fields);
}

void Request::AddField(const std::string &field) {
  fields_.emplace_back(field);
}

void Request::AddRangeFilter(struct RangeFilter &range_filter) {
  range_filters_.emplace_back(range_filter);
}

void Request::AddTermFilter(struct TermFilter &term_filter) {
  term_filters_.emplace_back(term_filter);
}

std::vector<struct VectorQuery> &Request::VecFields() { return vec_fields_; }

std::vector<std::string> &Request::Fields() { return fields_; }

std::vector<struct RangeFilter> &Request::RangeFilters() {
  return range_filters_;
}

std::vector<struct TermFilter> &Request::TermFilters() { return term_filters_; }

int Request::FilterOperator() { return filter_operator_; }

const std::string &Request::IndexParams() { return index_params_; }

void Request::SetIndexParams(const std::string &index_params) {
  index_params_ = index_params;
}

int Request::MultiVectorRank() { return multi_vector_rank_; }

void Request::SetMultiVectorRank(int multi_vector_rank) {
  multi_vector_rank_ = multi_vector_rank;
}

bool Request::L2Sqrt() { return l2_sqrt_; }

void Request::SetL2Sqrt(bool l2_sqrt) { l2_sqrt_ = l2_sqrt; }

bool Request::Trace() { return trace_; }

void Request::SetTrace(bool trace) { trace_ = trace; }

vearch::Ranker *Request::Ranker() { return ranker_; }

int Request::SetRanker(std::string params, int weight_num) {
  ranker_ = new WeightedRanker(params, weight_num);
  if (params == "") return 0;
  Status status = ranker_->Parse();
  if (!status.ok()) {
    delete ranker_;
    ranker_ = nullptr;
    return -1;
  }
  return 0;
}

int QueryRequest::Serialize(char **out, int *out_len) { return 0; }

void QueryRequest::Deserialize(const char *data, int len) {
  vearchpb::QueryRequest qr;
  bool parse_success = qr.ParseFromString(std::string(data, len));
  if (not parse_success) {
    LOG(ERROR) << "parse query request failed";
    return;
  }
  topn_ = qr.limit();

  for (int i = 0; i < qr.document_ids().size(); i++) {
    auto fbs_document_id = qr.document_ids(i);
    document_ids_.emplace_back(fbs_document_id);
  }

  for (int i = 0; i < qr.fields_size(); i++) {
    auto fbs_field = qr.fields(i);
    fields_.emplace_back(fbs_field);
  }

  for (int i = 0; i < qr.range_filters_size(); i++) {
    auto fbs_range_filter = qr.range_filters(i);
    struct RangeFilter range_filter;
    range_filter.field = fbs_range_filter.field();
    range_filter.lower_value = fbs_range_filter.lower_value();
    range_filter.upper_value = fbs_range_filter.upper_value();
    range_filter.include_lower = fbs_range_filter.include_lower();
    range_filter.include_upper = fbs_range_filter.include_upper();
    range_filter.is_union = fbs_range_filter.is_union();

    range_filters_.emplace_back(range_filter);
  }

  for (int i = 0; i < qr.term_filters_size(); i++) {
    auto fbs_term_filter = qr.term_filters(i);
    struct TermFilter term_filter;
    term_filter.field = fbs_term_filter.field();
    term_filter.value = fbs_term_filter.value();
    term_filter.is_union = fbs_term_filter.is_union();

    term_filters_.emplace_back(term_filter);
  }

  filter_operator_ = qr.operator_();
  partition_id_ = qr.partition_id();
}

void QueryRequest::AddDocumentId(const std::string &document_id) {
  document_ids_.emplace_back(document_id);
}

std::vector<std::string> &QueryRequest::DocumentIds() { return document_ids_; }

std::vector<std::string> &QueryRequest::Fields() { return fields_; }

std::vector<struct RangeFilter> &QueryRequest::RangeFilters() {
  return range_filters_;
}

std::vector<struct TermFilter> &QueryRequest::TermFilters() {
  return term_filters_;
}

int QueryRequest::FilterOperator() { return filter_operator_; }

int QueryRequest::PartitionId() { return partition_id_; }

void QueryRequest::SetPartitionId(int partition_id) {
  partition_id_ = partition_id;
}

}  // namespace vearch
