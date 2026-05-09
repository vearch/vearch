/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "scalar_index.h"
#include "storage/storage_manager.h"

namespace vearch {

// ============================================================================
// InvertedIndex - RocksDB-based inverted index (one-to-one mapping)
//
// Stores (field, value, docid) tuples in RocksDB.
// ============================================================================
class InvertedIndex : public ScalarIndex {
 public:
  InvertedIndex(Table* table, StorageManager* storage_mgr, int cf_id, DataType data_type, int field_id);
  ~InvertedIndex() override = default;

  // Add a document to the index
  int AddDoc(int64_t docid) override;

  // Delete a document from the index
  int DeleteDoc(int64_t docid) override;

  // Get index data size
  size_t GetIndexDataSize() override { return 0; }

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

  // RocksDB key construction helpers
  virtual std::string GenKeyPrefix(const std::string& index_value) const;
  virtual std::string GenKey(const std::string& index_value, int64_t docid) const;
 
 private:
  // Scan range from lower_key to upper_key and collect docids into ScalarIndexResult
  void ScanRange(const std::string& lower_key, const std::string& upper_key, ScalarIndexResult& result, int offset, int limit);
};

}  // namespace vearch
