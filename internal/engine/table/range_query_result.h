/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <roaring/roaring64map.hh>
#include <shared_mutex>
#include <string>
#include <vector>

#include "util/log.h"

namespace vearch {

// do intersection immediately
class RangeQueryResult {
 public:
  RangeQueryResult() { Clear(); }

  RangeQueryResult(RangeQueryResult&& other) noexcept
      : b_not_in_(other.b_not_in_), doc_bitmap_(std::move(other.doc_bitmap_)) {}

  RangeQueryResult& operator=(RangeQueryResult&& other) noexcept {
    if (this != &other) {
      b_not_in_ = other.b_not_in_;
      doc_bitmap_ = std::move(other.doc_bitmap_);
    }
    return *this;
  }

  ~RangeQueryResult() {}

  void Intersection(const RangeQueryResult& other) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    doc_bitmap_ &= other.doc_bitmap_;
  }

  void IntersectionWithNotIn(const RangeQueryResult& other) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (b_not_in_) {
      doc_bitmap_ &= other.doc_bitmap_;
    } else {
      doc_bitmap_ -= other.doc_bitmap_;
    }
  }

  void Union(const RangeQueryResult& other) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    doc_bitmap_ |= other.doc_bitmap_;
  }

  void UnionWithNotIn(const RangeQueryResult& other) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (b_not_in_) {
      doc_bitmap_ |= other.doc_bitmap_;
    } else {
      doc_bitmap_ -= other.doc_bitmap_;
    }
  }

  bool Has(int64_t doc) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (b_not_in_) {
      return !doc_bitmap_.contains(static_cast<uint64_t>(doc));
    } else {
      return doc_bitmap_.contains(static_cast<uint64_t>(doc));
    }
  }

  int64_t Cardinality() const {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return doc_bitmap_.cardinality();
  }

  std::vector<uint64_t> GetDocIDs(size_t topn) const {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<uint64_t> doc_ids;
    if (b_not_in_) {
      LOG(WARNING) << "NOT IN operation is not supported in GetDocIDs";
      return doc_ids;
    }

    doc_ids.reserve(
        doc_bitmap_.cardinality() > topn ? topn : doc_bitmap_.cardinality());

    for (uint64_t doc_id : doc_bitmap_) {
      doc_ids.push_back(doc_id);
    }

    return doc_ids;
  }

  void Clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    b_not_in_ = false;
    doc_bitmap_.clear();
  }

  void Add(int64_t doc) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    doc_bitmap_.add(static_cast<uint64_t>(doc));
  }

  void SetNotIn(bool b_not_in) { b_not_in_ = b_not_in; }

  bool NotIn() { return b_not_in_; }

 private:
  bool b_not_in_;
  roaring::Roaring64Map doc_bitmap_;
  mutable std::shared_mutex mutex_;
};

class MultiRangeQueryResults {
 public:
  MultiRangeQueryResults() { Clear(); }

  ~MultiRangeQueryResults() { Clear(); }

  // Take full advantage of multi-core while recalling
  bool Has(int64_t doc) const {
    if (all_results_.size() == 0) {
      return false;
    }
    bool ret = true;
    for (auto& result : all_results_) {
      ret = ret && result.Has(doc);
      if (not ret) break;
    }
    return ret;
  }

  void Clear() { all_results_.clear(); }

  size_t Size() { return all_results_.size(); }

  void Add(RangeQueryResult&& result) {
    all_results_.emplace_back(std::move(result));
  }

  std::vector<uint64_t> GetDocIDs(size_t topn) const {
    std::vector<uint64_t> doc_ids;
    if (all_results_.size() == 0) {
      return doc_ids;
    }

    doc_ids = all_results_[0].GetDocIDs(topn);
    if (doc_ids.size() >= topn) {
      return doc_ids;
    }

    for (size_t i = 1; i < all_results_.size(); ++i) {
      std::vector<uint64_t> tmp_doc_ids = all_results_[i].GetDocIDs(topn);
      std::vector<uint64_t> new_doc_ids;
      std::set_intersection(doc_ids.begin(), doc_ids.end(), tmp_doc_ids.begin(),
                            tmp_doc_ids.end(), std::back_inserter(new_doc_ids));
      doc_ids = new_doc_ids;
      if (doc_ids.size() >= topn) {
        break;
      }
    }

    return doc_ids;
  }

 private:
  std::vector<RangeQueryResult> all_results_;
};

}  // namespace vearch
