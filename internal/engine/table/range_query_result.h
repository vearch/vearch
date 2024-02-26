/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cassert>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "util/bitmap.h"
#include "util/log.h"

#define BM_OPERATE_TYPE long

namespace tig_gamma {

// do intersection immediately
class RangeQueryResult {
 public:
  RangeQueryResult() {
    bitmap_ = nullptr;
    Clear();
  }

  RangeQueryResult(RangeQueryResult &&other) { *this = std::move(other); }

  RangeQueryResult &operator=(RangeQueryResult &&other) {
    min_ = other.min_;
    max_ = other.max_;
    min_aligned_ = other.min_aligned_;
    max_aligned_ = other.max_aligned_;
    next_ = other.next_;
    n_doc_ = other.n_doc_;
    bitmap_ = other.bitmap_;
    other.bitmap_ = nullptr;
    b_not_in_ = other.b_not_in_;
    return *this;
  }

  ~RangeQueryResult() {
    if (bitmap_ != nullptr) {
      free(bitmap_);
      bitmap_ = nullptr;
    }
  }

  bool Has(int doc) const {
    if (b_not_in_) {
      if (doc < min_ || doc > max_) {
        return true;
      }
      doc -= min_aligned_;
      return !bitmap::test(bitmap_, doc);
    } else {
      if (doc < min_ || doc > max_) {
        return false;
      }
      doc -= min_aligned_;
      return bitmap::test(bitmap_, doc);
    }
  }

  /**
   * @return docID in order, -1 for the end
   */
  int Next() const {
    next_++;

    int size = max_aligned_ - min_aligned_ + 1;
    while (next_ < size && not bitmap::test(bitmap_, next_)) {
      next_++;
    }
    if (next_ >= size) {
      return -1;
    }

    int doc = next_ + min_aligned_;
    return doc;
  }

  /**
   * @return size of docID list
   */
  int Size() const { return n_doc_; }

  void Clear() {
    min_ = std::numeric_limits<int>::max();
    max_ = 0;
    next_ = -1;
    n_doc_ = -1;
    if (bitmap_ != nullptr) {
      free(bitmap_);
      bitmap_ = nullptr;
    }
    b_not_in_ = false;
  }

  void SetRange(int x, int y) {
    min_ = std::min(min_, x);
    max_ = std::max(max_, y);
    min_aligned_ = (min_ / 8) * 8;
    max_aligned_ = (max_ / 8 + 1) * 8 - 1;
  }

  void Resize() {
    int n = max_aligned_ - min_aligned_ + 1;
    assert(n > 0);
    if (bitmap_ != nullptr) {
      free(bitmap_);
      bitmap_ = nullptr;
    }

    int bytes_count = -1;
    if (bitmap::create(bitmap_, bytes_count, n) != 0) {
      LOG(ERROR) << "Cannot create bitmap!";
      return;
    }
  }

  void Set(int pos) { bitmap::set(bitmap_, pos); }

  int Min() const { return min_; }
  int Max() const { return max_; }

  int MinAligned() { return min_aligned_; }
  int MaxAligned() { return max_aligned_; }

  char *&Ref() { return bitmap_; }

  void SetDocNum(int num) { n_doc_ = num; }

  void SetNotIn(bool b_not_in) { b_not_in_ = b_not_in; }

  bool NotIn() { return b_not_in_; }

  /**
   * @return sorted docIDs
   */
  std::vector<int> ToDocs() const;  // WARNING: build dynamically
  void Output();

 private:
  int min_;
  int max_;
  int min_aligned_;
  int max_aligned_;

  mutable int next_;
  mutable int n_doc_;

  char *bitmap_;
  bool b_not_in_;
};

// do intersection lazily
class MultiRangeQueryResults {
 public:
  MultiRangeQueryResults() { Clear(); }

  ~MultiRangeQueryResults() { Clear(); }

  // Take full advantage of multi-core while recalling
  bool Has(int doc) const {
    if (all_results_.size() == 0) {
      return false;
    }
    bool ret = true;
    for (auto &result : all_results_) {
      ret = ret && result.Has(doc);
      if (not ret) break;
    }
    return ret;
  }

  void Clear() {
    min_ = 0;
    max_ = std::numeric_limits<int>::max();
    all_results_.clear();
  }

 public:
  size_t Size() { return all_results_.size(); }
  void Add(RangeQueryResult &&result) {
    // the maximum of the minimum(s)
    if (result.Min() > min_) {
      min_ = result.Min();
    }
    // the minimum of the maximum(s)
    if (result.Max() < max_) {
      max_ = result.Max();
    }
    all_results_.emplace_back(std::move(result));
  }

  int Min() const { return min_; }
  int Max() const { return max_; }

  /** WARNING: build dynamically
   * @return sorted docIDs
   */
  std::vector<int> ToDocs() const;

  const RangeQueryResult *GetAllResult() const { return &all_results_[0]; }

 private:
  int min_;
  int max_;

  std::vector<RangeQueryResult> all_results_;
};

}  // namespace tig_gamma
