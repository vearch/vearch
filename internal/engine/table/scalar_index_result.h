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

namespace vearch {

// Result of evaluating a single scalar filter condition (or the merged
// result of several conditions combined by AND/OR/NOT IN via the
// Intersection/Union/Difference ops below).
//
// Internally a Roaring64Map over docids.
class ScalarIndexResult {
 public:
 ScalarIndexResult() { Clear(); }

 ScalarIndexResult(ScalarIndexResult&& other) noexcept
      : doc_bitmap_(std::move(other.doc_bitmap_)) {}

  /**
   * Construct from a Roaring64Map with offset/limit applied in one pass.
   */
  ScalarIndexResult(roaring::Roaring64Map bitmap, int offset, int limit) {
    if (offset < 0 || limit < 0) {
      return;
    }
    // offset = 0 and limit = 0 means no offset and no limit
    if (offset == 0 && limit == 0) {
      doc_bitmap_ = std::move(bitmap);
      return;
    }
    auto it = bitmap.begin();
    if ((uint64_t)offset >= bitmap.cardinality()) {
      return;
    }
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
      : doc_bitmap_(bitmap) {}

  /**
   * Copy with offset/limit applied.
   */
  ScalarIndexResult(const ScalarIndexResult& other, int offset, int limit) {
    if (offset < 0 || limit < 0) {
      return;
    }
    // offset = 0 and limit = 0 means no offset and no limit
    if (offset == 0 && limit == 0) {
      doc_bitmap_ = other.doc_bitmap_;
      return;
    }
    auto it = other.doc_bitmap_.begin();
    if ((uint64_t)offset >= other.doc_bitmap_.cardinality()) {
      return;
    }
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
      doc_bitmap_ = std::move(other.doc_bitmap_);
    }
    return *this;
  }

  ~ScalarIndexResult() {}

  void Intersection(const ScalarIndexResult& other) {
    doc_bitmap_ &= other.doc_bitmap_;
  }

  // Set difference: remove docids present in `other`. Used to implement
  // NOT IN as "[0, total) minus matched".
  void Difference(const ScalarIndexResult& other) {
    doc_bitmap_ -= other.doc_bitmap_;
  }

  void Union(const ScalarIndexResult& other) {
    doc_bitmap_ |= other.doc_bitmap_;
  }

  bool Has(int64_t doc) const {
    return doc_bitmap_.contains(static_cast<uint64_t>(doc));
  }

  int64_t Cardinality() const { return doc_bitmap_.cardinality(); }

  const roaring::Roaring64Map& GetDocBitmap() const { return doc_bitmap_; }

  std::vector<uint64_t> GetDocIDs(size_t topn) const {
    std::vector<uint64_t> doc_ids;
    doc_ids.reserve(
        doc_bitmap_.cardinality() > topn ? topn : doc_bitmap_.cardinality());

    for (uint64_t doc_id : doc_bitmap_) {
      doc_ids.push_back(doc_id);
    }

    return doc_ids;
  }

  void Clear() { doc_bitmap_.clear(); }

  void Add(int64_t doc) { doc_bitmap_.add(static_cast<uint64_t>(doc)); }
  void AddRange(int64_t mindoc, int64_t maxdoc) {
    doc_bitmap_.addRange(static_cast<uint64_t>(mindoc),
                         static_cast<uint64_t>(maxdoc));
  }

 private:
  roaring::Roaring64Map doc_bitmap_;
};

// Container for the scalar-filter outcome of a single search request.
//
// Despite the plural name and the `std::vector<ScalarIndexResult>` member,
// in the current code path this holds **at most one** ScalarIndexResult:
// ScalarIndexManager::Search merges every per-filter result (via
// Intersection/Union according to the request's FilterOperator) into a
// single final ScalarIndexResult and pushes that one result via Add().
// Downstream consumers (filter-first vector search, GetDocIDs, etc.) only
// ever read `all_results_[0]`.
//
// The vector-of-results shape is a historical artifact from when each
// field's result was kept separate and intersected lazily during recall;
// kept for ABI/source compatibility with existing callers.
//
// Invariants assumed by callers:
//   - Size() is 0 or 1.
//   - GetCandidateBitmap()/Cardinality()/GetDocIDs() only consult
//     all_results_[0].
//
// TODO(refactor): Collapse this class. Since `all_results_` is provably
// 0-or-1, the wrapper buys nothing over a single
// `std::optional<ScalarIndexResult>` (or just a ScalarIndexResult with an
// "empty" state) -- and the misleading plural name keeps tempting readers
// to assume per-field results live here.
class ScalarIndexResults {
 public:
  ScalarIndexResults() { Clear(); }

  ~ScalarIndexResults() { Clear(); }

  // Per-doc predicate check used by recall paths.
  // Given the Size() <= 1 invariant above, this is effectively
  // `all_results_[0].Has(doc)` (or `false` when empty); the AND-loop is
  // retained for the historical multi-result case but degenerates to a
  // single call in current usage.
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

  // Direct bitmap accessor for filter-first paths that want to avoid
  // materializing the candidate set into a std::vector. Returns nullptr when
  // there is no result.
  // Invariant: only all_results_[0] is consulted (all_results_ holds at most
  // one already-intersected result).
  const roaring::Roaring64Map *GetCandidateBitmap() const {
    if (all_results_.size() == 0) return nullptr;
    return &all_results_[0].GetDocBitmap();
  }

  // Candidate cardinality (Roaring64Map::cardinality, O(#containers)), used by
  // filter-first gating to avoid materializing the candidate vector just to
  // read its size.
  int64_t Cardinality() const {
    if (all_results_.size() == 0) return 0;
    return all_results_[0].Cardinality();
  }

 private:
  std::vector<ScalarIndexResult> all_results_;
};

}  // namespace vearch
