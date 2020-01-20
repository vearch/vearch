/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef SRC_SEARCHER_INDEX_RANGE_QUERY_RESULT_H_
#define SRC_SEARCHER_INDEX_RANGE_QUERY_RESULT_H_

#include <cassert>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

namespace tig_gamma {

typedef std::vector<bool> BitmapType;

// do intersection immediately
class RangeQueryResult {
 public:
  RangeQueryResult() { Clear(); }

  bool Has(int doc) const {
    if (doc < min_ || doc > max_) {
      return false;
    }
    doc -= min_;
    return bitmap_[doc];
  }

  /**
   * @return docID in order, -1 for the end
   */
  int Next() const {
    next_++;

    int size = bitmap_.size();
    while (next_ < size && not bitmap_[next_]) {
      next_++;
    }
    if (next_ >= size) {
      return -1;
    }

    int doc = next_ + min_;
    return doc;
  }

  /**
   * @return size of docID list
   */
  int Size() const {
    if (n_doc_ >= 0) {
      return n_doc_;
    }

    n_doc_ = 0;
    size_t size = bitmap_.size();
    int n = 0;

#pragma omp parallel for reduction(+ : n)
    for (size_t i = 0; i < size; ++i) {
      if (bitmap_[i]) {
        ++n;
      }
    }

    n_doc_ = n;
    return n_doc_;
  }

  void Clear() {
    min_ = std::numeric_limits<int>::max();
    max_ = 0;
    next_ = -1;
    n_doc_ = -1;
    bitmap_.clear();
  }

 public:
  void SetRange(int x, int y) {
    min_ = std::min(min_, x);
    max_ = std::max(max_, y);
  }

  void Resize(bool init_value = false) {
    int n = max_ - min_ + 1;
    assert(n > 0);
    bitmap_.resize(n, init_value);
  }

  void Set(int pos) { bitmap_[pos] = true; }

  int Min() const { return min_; }
  int Max() const { return max_; }

  BitmapType &Ref() { return bitmap_; }

  /**
   * @return sorted docIDs
   */
  std::vector<int> ToDocs() const;  // WARNING: build dynamically
  void Output();

 private:
  int min_;
  int max_;

  mutable int next_;
  mutable int n_doc_;

  BitmapType bitmap_;
};
// do intersection lazily
class MultiRangeQueryResults {
 public:
  MultiRangeQueryResults() { Clear(); }

  ~MultiRangeQueryResults() {
    for (auto &result : all_results_) {
      delete result;
      result = nullptr;
    }
    all_results_.clear();
  }

  // Take full advantage of multi-core while recalling
  bool Has(int doc) const {
    bool ret = true;
    for (auto &result : all_results_) {
      ret &= result->Has(doc);
      if (ret == false) return ret;
    }
    return ret;
  }

  void Clear() {
    min_ = 0;
    max_ = std::numeric_limits<int>::max();
    all_results_.clear();
  }

 public:
  void Add(RangeQueryResult *r) {
    all_results_.emplace_back(r);

    // the maximum of the minimum(s)
    if (r->Min() > min_) {
      min_ = r->Min();
    }
    // the minimum of the maximum(s)
    if (r->Max() < max_) {
      max_ = r->Max();
    }
  }

  int Min() const { return min_; }
  int Max() const { return max_; }

  /** WARNING: build dynamically
   * @return sorted docIDs
   */
  std::vector<int> ToDocs() const;

  const std::vector<RangeQueryResult *> &GetAllResult() const {
    return all_results_;
  }

 private:
  int min_;
  int max_;

  std::vector<RangeQueryResult *> all_results_;
};

}  // namespace tig_gamma

#endif  // SRC_SEARCHER_INDEX_RANGE_QUERY_RESULT_H_
