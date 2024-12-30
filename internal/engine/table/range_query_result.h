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

namespace vearch {

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
    free(bitmap_);
    bitmap_ = nullptr;
  }

  bool Has(int64_t doc) const {
    if (b_not_in_) {
      if (doc < min_ || doc > max_) {
        return true;
      }
      return !bitmap::test(bitmap_, doc - min_aligned_);
    } else {
      if (doc < min_ || doc > max_) {
        return false;
      }
      return bitmap::test(bitmap_, doc - min_aligned_);
    }
  }

  /**
   * @return docID in order, -1 for the end
   */
  int64_t Next() const {
    next_++;

    int64_t size = max_aligned_ - min_aligned_ + 1;
    while (next_ < size && not bitmap::test(bitmap_, next_)) {
      next_++;
    }
    if (next_ >= size) {
      return -1;
    }

    int64_t doc = next_ + min_aligned_;
    return doc;
  }

  /**
   * @return size of docID list
   */
  int64_t Size() const { return n_doc_; }

  void Clear() {
    min_ = std::numeric_limits<int64_t>::max();
    max_ = 0;
    next_ = -1;
    n_doc_ = -1;
    if (bitmap_ != nullptr) {
      free(bitmap_);
      bitmap_ = nullptr;
    }
    b_not_in_ = false;
  }

  void SetRange(int64_t x, int64_t y) {
    min_ = std::min(min_, x);
    max_ = std::max(max_, y);
    min_aligned_ = (min_ / 8) * 8;
    max_aligned_ = (max_ / 8 + 1) * 8 - 1;
  }

  void Resize() {
    int64_t n = max_aligned_ - min_aligned_ + 1;
    if (n <= 0) {
      LOG(ERROR) << "max_aligned_ " << max_aligned_ << " min_aligned_ "
                 << min_aligned_ << " max_ " << max_ << " min_ " << min_;
    }
    assert(n > 0);
    if (bitmap_ != nullptr) {
      free(bitmap_);
      bitmap_ = nullptr;
    }

    int64_t bytes_count = -1;
    if (bitmap::create(bitmap_, bytes_count, n) != 0) {
      LOG(ERROR) << "Cannot create bitmap!";
      return;
    }
  }

  void Set(int64_t pos) { bitmap::set(bitmap_, pos); }

  int64_t Min() const { return min_; }
  int64_t Max() const { return max_; }

  int64_t MinAligned() { return min_aligned_; }
  int64_t MaxAligned() { return max_aligned_; }

  char *&Ref() { return bitmap_; }

  void SetDocNum(int64_t num) { n_doc_ = num; }

  void SetNotIn(bool b_not_in) { b_not_in_ = b_not_in; }

  bool NotIn() { return b_not_in_; }

 private:
  int64_t min_;
  int64_t max_;
  int64_t min_aligned_;
  int64_t max_aligned_;

  mutable int64_t next_;
  mutable int64_t n_doc_;

  char *bitmap_;
  bool b_not_in_;
};

// do intersection lazily
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
    for (auto &result : all_results_) {
      ret = ret && result.Has(doc);
      if (not ret) break;
    }
    return ret;
  }

  void Clear() { all_results_.clear(); }

  size_t Size() { return all_results_.size(); }

  void Add(RangeQueryResult &&result) {
    all_results_.emplace_back(std::move(result));
  }

 private:
  std::vector<RangeQueryResult> all_results_;
};

}  // namespace vearch
