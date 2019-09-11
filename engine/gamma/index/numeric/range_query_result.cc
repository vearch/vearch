/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "range_query_result.h"

namespace tig_gamma {
namespace NI {

std::vector<int> RangeQueryResultV1::ToDocs() const {
  if (n_doc_ >= 0) {
    std::vector<int> docIDs(n_doc_);
    int j = 0;

    for (size_t i = 0; i < bitmap_.size(); i++) {
      if (bitmap_[i]) {
        docIDs[j++] = i + min_;
      }
    }

    assert(j == n_doc_);
    return docIDs;
  } else {
    std::vector<int> docIDs;

    for (size_t i = 0; i < bitmap_.size(); i++) {
      if (bitmap_[i]) {
        docIDs.emplace_back(i + min_);
      }
    }

    n_doc_ = static_cast<int>(docIDs.size());
    return docIDs;
  }
}

void RangeQueryResultV1::Output() {
  std::cout << "bitmap = [";
  for (size_t i = 0; i < bitmap_.size(); i++) {
    if (bitmap_[i]) {
      std::cout << " " << i;
    }
  }
  std::cout << " ]\n";
}

std::vector<int> RangeQueryResult::ToDocs() const {
  std::vector<int> docIDs;

  if (not all_results_.empty()) {
    for (int id = min_; id <= max_; id++) {
      if (Has(id)) {
        docIDs.emplace_back(id);
      }
    }
  }

  return docIDs;
}

} // namespace NI
} // namespace tig_gamma
