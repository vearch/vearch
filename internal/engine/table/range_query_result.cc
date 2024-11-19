/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "range_query_result.h"

namespace vearch {

std::vector<int64_t> RangeQueryResult::ToDocs() const {
  if (n_doc_ >= 0) {
    std::vector<int64_t> docIDs(n_doc_);
    int64_t j = 0;

    int64_t n = max_aligned_ - min_aligned_ + 1;
    for (int64_t i = 0; i < n; i++) {
      if (bitmap::test(bitmap_, i)) {
        docIDs[j++] = i + min_aligned_;
      }
    }

    assert(j == n_doc_);
    return docIDs;
  } else {
    std::vector<int64_t> docIDs;
    int64_t n = max_aligned_ - min_aligned_ + 1;

    for (int64_t i = 0; i < n; i++) {
      if (bitmap::test(bitmap_, i)) {
        docIDs.emplace_back(i + min_aligned_);
      }
    }

    n_doc_ = static_cast<int64_t>(docIDs.size());
    return docIDs;
  }
}

void RangeQueryResult::Output() {
  std::stringstream ss;
  ss << "bitmap = [";
  int64_t n = max_aligned_ - min_aligned_ + 1;
  for (int64_t i = 0; i < n; i++) {
    if (bitmap::test(bitmap_, i)) {
      ss << " " << i;
    }
  }
  ss << "]";
  LOG(INFO) << ss.str();
}

std::vector<int64_t> MultiRangeQueryResults::ToDocs() const {
  std::vector<int64_t> docIDs;

  for (auto &result : all_results_) {
    for (int64_t id = min_; id <= max_; id++) {
      if (result.Has(id)) {
        docIDs.emplace_back(id);
      }
    }
  }

  return docIDs;
}

}  // namespace vearch
