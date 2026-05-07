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

#include "scalar_index_result.h"
#include "storage/storage_manager.h"
#include "scalar_index_utils.h"
#include "table.h"

namespace vearch {

// ============================================================================
// ScalarIndex - Base class for scalar indexes
//   - InvertedIndex: RocksDB (field, value, docid) -> null (one-to-one)
//   - InvertedListIndex: (field, value) -> [docids] (one-to-many, future)
//   - BitmapIndex: roaring bitmap-based in-memory index
//   - CompositeIndex: RocksDB composite key (multi-column)
// ============================================================================
class ScalarIndex {
 public:
  ScalarIndex(Table* table, StorageManager* storage_mgr, int cf_id, DataType data_type, int field_id)
      : table_(table),
        storage_mgr_(storage_mgr),
        cf_id_(cf_id),
        data_type_(data_type),
        field_id_(field_id) {}
  virtual ~ScalarIndex() = default;

  // Add a document to the index
  virtual int AddDoc(int64_t docid) = 0;

  // Delete a document from the index
  virtual int DeleteDoc(int64_t docid) = 0;

  // Get index data size
  virtual size_t GetIndexDataSize() = 0;

  virtual ScalarIndexResult In(const std::vector<std::string>& values, int offset, int limit) = 0;
  virtual ScalarIndexResult NotIn(const std::vector<std::string>& values, int offset, int limit) = 0;
  virtual ScalarIndexResult Equal(const std::string& value, int offset, int limit) = 0;
  virtual ScalarIndexResult NotEqual(const std::string& value, int offset, int limit) = 0;
  virtual ScalarIndexResult Range(const std::string& lower_value, bool lb_inclusive,
                                 const std::string& upper_value, bool ub_inclusive, int offset, int limit) = 0;
  virtual ScalarIndexResult LessThan(const std::string& value, int offset, int limit) = 0;
  virtual ScalarIndexResult LessEqual(const std::string& value, int offset, int limit) = 0;
  virtual ScalarIndexResult GreaterThan(const std::string& value, int offset, int limit) = 0;
  virtual ScalarIndexResult GreaterEqual(const std::string& value, int offset, int limit) = 0;

  bool IsNumeric() const {
    return data_type_ != DataType::STRING && data_type_ != DataType::STRINGARRAY;
  }

  virtual int GetFieldId() const { return field_id_; }
  virtual DataType GetDataType() const { return data_type_; }

 protected:
  Table* table_;
  StorageManager* storage_mgr_;
  int cf_id_;
  DataType data_type_;
  int field_id_;
};

}  // namespace vearch
