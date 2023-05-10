/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_request.h"

namespace tig_gamma {

int Request::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<gamma_api::VectorQuery>> vec_fields_vector;

  for (struct VectorQuery &vec_field : vec_fields_) {
    std::string &name = vec_field.name;

    std::vector<uint8_t> value(vec_field.value.size());
    memcpy(value.data(), vec_field.value.data(), vec_field.value.size());

    double min_score = vec_field.min_score;
    double max_score = vec_field.max_score;
    double boost = vec_field.boost;
    int has_boost = vec_field.has_boost;

    auto vector_query = gamma_api::CreateVectorQuery(
        builder, builder.CreateString(name), builder.CreateVector(value),
        min_score, max_score, boost, has_boost,
        builder.CreateString(vec_field.retrieval_type));
    vec_fields_vector.push_back(vector_query);
  }

  std::vector<flatbuffers::Offset<flatbuffers::String>> fields_vector;

  for (std::string &field : fields_) {
    fields_vector.emplace_back(builder.CreateString(field));
  }

  std::vector<flatbuffers::Offset<gamma_api::RangeFilter>> range_filter_vector;

  for (struct RangeFilter &range_filter : range_filters_) {
    std::string &field = range_filter.field;
    std::vector<uint8_t> lower_value(range_filter.lower_value.size());
    memcpy(lower_value.data(), range_filter.lower_value.data(),
           range_filter.lower_value.size());
    std::vector<uint8_t> upper_value(range_filter.upper_value.size());
    memcpy(upper_value.data(), range_filter.upper_value.data(),
           range_filter.upper_value.size());
    bool include_lower = range_filter.include_lower;
    bool include_upper = range_filter.include_upper;
    range_filter_vector.emplace_back(gamma_api::CreateRangeFilter(
        builder, builder.CreateString(field), builder.CreateVector(lower_value),
        builder.CreateVector(upper_value), include_lower, include_upper));
  }

  std::vector<flatbuffers::Offset<gamma_api::TermFilter>> term_filter_vector;

  for (struct TermFilter &term_filter : term_filters_) {
    std::string &field = term_filter.field;
    std::vector<uint8_t> value(term_filter.value.size());
    memcpy(value.data(), term_filter.value.data(), term_filter.value.size());
    int is_union = term_filter.is_union;
    term_filter_vector.emplace_back(
        gamma_api::CreateTermFilter(builder, builder.CreateString(field),
                                    builder.CreateVector(value), is_union));
  }

  auto res = gamma_api::CreateRequest(
      builder, req_num_, topn_, brute_force_search_,
      builder.CreateVector(vec_fields_vector),
      builder.CreateVector(fields_vector),
      builder.CreateVector(range_filter_vector),
      builder.CreateVector(term_filter_vector),
      builder.CreateString(retrieval_params_), has_rank_,
      builder.CreateString(online_log_level_), multi_vector_rank_, l2_sqrt_);

  builder.Finish(res);
  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);
  return 0;
}

void Request::Deserialize(const char *data, int len) {
  assert(request_ == nullptr);
  request_ = const_cast<gamma_api::Request *>(gamma_api::GetRequest(data));

  req_num_ = request_->req_num();
  topn_ = request_->topn();
  brute_force_search_ = request_->brute_force_search();

  for (size_t i = 0; i < request_->vec_fields()->size(); ++i) {
    auto fbs_vector_query = request_->vec_fields()->Get(i);
    struct VectorQuery vector_query;
    vector_query.name = fbs_vector_query->name()->str();
    vector_query.value = std::string(
        reinterpret_cast<const char *>(fbs_vector_query->value()->Data()),
        fbs_vector_query->value()->size());
    vector_query.min_score = fbs_vector_query->min_score();
    vector_query.max_score = fbs_vector_query->max_score();
    vector_query.boost = fbs_vector_query->boost();
    vector_query.has_boost = fbs_vector_query->has_boost();
    vector_query.retrieval_type = fbs_vector_query->retrieval_type()->str();

    vec_fields_.emplace_back(vector_query);
  }

  for (size_t i = 0; i < request_->fields()->size(); ++i) {
    auto fbs_field = request_->fields()->Get(i);
    fields_.emplace_back(fbs_field->str());
  }

  for (size_t i = 0; i < request_->range_filters()->size(); ++i) {
    auto fbs_range_filter = request_->range_filters()->Get(i);
    struct RangeFilter range_filter;
    range_filter.field = fbs_range_filter->field()->str();
    range_filter.lower_value = std::string(
        reinterpret_cast<const char *>(fbs_range_filter->lower_value()->Data()),
        fbs_range_filter->lower_value()->size());
    range_filter.upper_value = std::string(
        reinterpret_cast<const char *>(fbs_range_filter->upper_value()->Data()),
        fbs_range_filter->upper_value()->size());
    range_filter.include_lower = fbs_range_filter->include_lower();
    range_filter.include_upper = fbs_range_filter->include_upper();

    range_filters_.emplace_back(range_filter);
  }

  for (size_t i = 0; i < request_->term_filters()->size(); ++i) {
    auto fbs_term_filter = request_->term_filters()->Get(i);
    struct TermFilter term_filter;
    term_filter.field = fbs_term_filter->field()->str();
    term_filter.value = std::string(
        reinterpret_cast<const char *>(fbs_term_filter->value()->Data()),
        fbs_term_filter->value()->size());
    term_filter.is_union = fbs_term_filter->is_union();

    term_filters_.emplace_back(term_filter);
  }

  retrieval_params_ = request_->retrieval_params()->str();
  online_log_level_ = request_->online_log_level()->str();
  has_rank_ = request_->has_rank();
  multi_vector_rank_ = request_->multi_vector_rank();
  l2_sqrt_ = request_->l2_sqrt();
}

int Request::ReqNum() {
  if (request_)
    return request_->req_num();
  else
    return req_num_;
}

void Request::SetReqNum(int req_num) { req_num_ = req_num; }

int Request::TopN() {
  if (request_)
    return request_->topn();
  else
    return topn_;
}

void Request::SetTopN(int topn) { topn_ = topn; }

int Request::BruteForceSearch() {
  if (request_)
    return request_->brute_force_search();
  else
    return brute_force_search_;
}

void Request::SetBruteForceSearch(int brute_force_search) {
  brute_force_search_ = brute_force_search;
}

void Request::AddVectorQuery(struct VectorQuery &vec_fields) {
  vec_fields_.emplace_back(vec_fields);
}

void Request::AddField(const std::string &field) { fields_.emplace_back(field); }

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

const std::string &Request::RetrievalParams() { return retrieval_params_; }

void Request::SetRetrievalParams(const std::string &retrieval_params) {
  retrieval_params_ = retrieval_params;
}

std::string Request::OnlineLogLevel() { return online_log_level_; }

bool Request::HasRank() { return has_rank_; }

void Request::SetHasRank(bool has_rank) { has_rank_ = has_rank; }

int Request::MultiVectorRank() {
  if (request_)
    return request_->multi_vector_rank();
  else
    return multi_vector_rank_; 
}

void Request::SetMultiVectorRank(int multi_vector_rank) {
  multi_vector_rank_ = multi_vector_rank_;
}

void Request::SetOnlineLogLevel(const std::string &online_log_level) {
  online_log_level_ = online_log_level;
}

bool Request::L2Sqrt() {
  if (request_)
    return request_->l2_sqrt();
  else
    return l2_sqrt_;
}

void Request::SetL2Sqrt(bool l2_sqrt) { l2_sqrt_ = l2_sqrt; }

}  // namespace tig_gamma
