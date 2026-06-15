/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <shared_mutex>
#include <string>
#include <vector>
#include <roaring/roaring64map.hh>

#include "scalar_index.h"

namespace vearch {

// ============================================================================
// BitmapIndex - Roaring bitmap-based inverted index
// ============================================================================
class BitmapIndex : public ScalarIndex {
 public:
  BitmapIndex(Table* table, StorageManager* storage_mgr, int cf_id, DataType data_type, int field_id);
  ~BitmapIndex();

  // init index from storage manager
  int Init();

  // Add a document to the index
  int AddDoc(int64_t docid) override;

  // Delete a document from the index
  int DeleteDoc(int64_t docid) override;

  // Get index data size
  size_t GetIndexDataSize() override;

  ScalarIndexResult In(const std::vector<std::string>& values, int offset, int limit) override;
  ScalarIndexResult NotIn(const std::vector<std::string>& values, int offset, int limit) override;
  ScalarIndexResult Equal(const std::string& value, int offset, int limit) override;
  ScalarIndexResult NotEqual(const std::string& value, int offset, int limit) override;
  ScalarIndexResult Range(const std::string& lower_value, bool lb_inclusive,
                         const std::string& upper_value, bool ub_inclusive, int offset, int limit) override;
  ScalarIndexResult LessThan(const std::string& value, int offset, int limit) override;
  ScalarIndexResult LessEqual(const std::string& value, int offset, int limit) override;
  ScalarIndexResult GreaterThan(const std::string& value, int offset, int limit) override;
  ScalarIndexResult GreaterEqual(const std::string& value, int offset, int limit) override;

  // Clear all data
  void Clear();

 private:
  // Convert roaring::Roaring64Map to ScalarIndexResult with offset/limit
  ScalarIndexResult BitmapToResultWithOffsetLimit(roaring::Roaring64Map bitmap, int offset, int limit);

  // Thread safety: shared_mutex for read-write lock
  mutable std::shared_mutex mutex_;

  // Data storage: map from sortable key to roaring bitmap
  std::map<std::string, roaring::Roaring64Map> data_;
};

}  // namespace vearch
