/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <iterator>
#include <roaring/roaring64map.hh>
#include <string>
#include <vector>

#include "util/log.h"

namespace vearch {

class ScalarIndexResult {
 public:
 ScalarIndexResult() { Clear(); }

 ScalarIndexResult(ScalarIndexResult&& other) noexcept
      : b_not_in_(other.b_not_in_), doc_bitmap_(std::move(other.doc_bitmap_)) {}

  /**
   * Construct from a Roaring64Map with offset/limit applied in one pass.
   */
  ScalarIndexResult(const roaring::Roaring64Map& bitmap, int offset, int limit) {
    b_not_in_ = false;
    auto it = bitmap.begin();
    std::advance(it, offset);
    for (; it != bitmap.end(); ++it) {
      doc_bitmap_.add(*it);
      if (limit > 0 && doc_bitmap_.cardinality() >= static_cast<uint64_t>(limit)) {
        break;
      }
    }
  }

  /**
   * Construct from a Roaring64Map (copy).
   */
  explicit ScalarIndexResult(const roaring::Roaring64Map& bitmap)
      : b_not_in_(false), doc_bitmap_(bitmap) {}

  /**
   * Copy with offset/limit applied.
   */
  ScalarIndexResult(const ScalarIndexResult& other, int offset, int limit) {
    b_not_in_ = other.b_not_in_;
    if (limit == 0 || offset < 0) {
      return;
    }
    auto it = other.doc_bitmap_.begin();
    std::advance(it, offset);
    for (; it != other.doc_bitmap_.end(); ++it) {
      doc_bitmap_.add(*it);
      if (limit > 0 && doc_bitmap_.cardinality() >= static_cast<uint64_t>(limit)) {
        break;
      }
    }
  }

  ScalarIndexResult& operator=(ScalarIndexResult&& other) noexcept {
    if (this != &other) {
      b_not_in_ = other.b_not_in_;
      doc_bitmap_ = std::move(other.doc_bitmap_);
    }
    return *this;
  }

  ~ScalarIndexResult() {}

  void Intersection(const ScalarIndexResult& other) {
    doc_bitmap_ &= other.doc_bitmap_;
  }

  void IntersectionWithNotIn(const ScalarIndexResult& other) {
    if (b_not_in_) {
      doc_bitmap_ &= other.doc_bitmap_;
    } else {
      doc_bitmap_ -= other.doc_bitmap_;
    }
  }

  void Union(const ScalarIndexResult& other) {
    doc_bitmap_ |= other.doc_bitmap_;
  }

  void UnionWithNotIn(const ScalarIndexResult& other) {
    if (b_not_in_) {
      doc_bitmap_ |= other.doc_bitmap_;
    } else {
      doc_bitmap_ -= other.doc_bitmap_;
    }
  }

  bool Has(int64_t doc) const {
    if (b_not_in_) {
      return !doc_bitmap_.contains(static_cast<uint64_t>(doc));
    } else {
      return doc_bitmap_.contains(static_cast<uint64_t>(doc));
    }
  }

  int64_t Cardinality() const { return doc_bitmap_.cardinality(); }

  const roaring::Roaring64Map& GetDocBitmap() const { return doc_bitmap_; }

  std::vector<uint64_t> GetDocIDs(size_t topn) const {
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
    b_not_in_ = false;
    doc_bitmap_.clear();
  }

  void Add(int64_t doc) { doc_bitmap_.add(static_cast<uint64_t>(doc)); }
  void AddRange(int64_t mindoc, int64_t maxdoc) {
    doc_bitmap_.addRange(static_cast<uint64_t>(mindoc),
                         static_cast<uint64_t>(maxdoc));
  }

  void SetNotIn(bool b_not_in) { b_not_in_ = b_not_in; }

  bool NotIn() { return b_not_in_; }

 private:
  bool b_not_in_;
  roaring::Roaring64Map doc_bitmap_;
};

class ScalarIndexResults {
 public:
  ScalarIndexResults() { Clear(); }

  ~ScalarIndexResults() { Clear(); }

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

  void Add(ScalarIndexResult&& result) {
    all_results_.emplace_back(std::move(result));
  }

  std::vector<uint64_t> GetDocIDs(size_t topn) const {
    std::vector<uint64_t> doc_ids;
    if (all_results_.size() == 0) {
      return doc_ids;
    }
    return all_results_[0].GetDocIDs(topn);
  }

 private:
  std::vector<ScalarIndexResult> all_results_;
};

}  // namespace vearch
