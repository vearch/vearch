/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <string>
#include <vector>

#include "range_query_result.h"
#include "storage/storage_manager.h"
#include "table.h"

namespace vearch {

enum class FilterOperator : uint8_t { And = 0, Or, Not };

typedef struct {
  int field;
  std::string lower_value;
  std::string upper_value;
  bool include_lower;
  bool include_upper;
  FilterOperator is_union;
} FilterInfo;

class FieldRangeIndex;
class MultiFieldsRangeIndex {
 public:
  MultiFieldsRangeIndex(Table *table, StorageManager *storage_mgr);
  ~MultiFieldsRangeIndex();

  int Init();

  int AddDoc(int64_t docid, int field);

  int Delete(int64_t docid, int field);

  int AddField(int field, enum DataType field_type, std::string &name);

  int RemoveField(int field);

  int64_t Search(FilterOperator query_filter_operator,
                 const std::vector<FilterInfo> &origin_filters,
                 MultiRangeQueryResults *out);

  int64_t Query(FilterOperator query_filter_operator,
                const std::vector<FilterInfo> &origin_filters,
                std::vector<uint64_t> &docids, size_t topn, size_t offset);

  int64_t DocCount() {
    int64_t doc_count = 0;
    auto &db = storage_mgr_->GetDB();
    rocksdb::ColumnFamilyHandle *cf_handler =
        storage_mgr_->GetColumnFamilyHandle(cf_id_);

    uint64_t count = 0;
    bool success =
        db->GetIntProperty(cf_handler, "rocksdb.estimate-num-keys", &count);
    if (!success) {
      LOG(ERROR) << "Failed to get CF size";
      return -1;
    }
    doc_count += count;
    LOG(INFO) << "Total doc count: " << doc_count;
    return doc_count;
  }

 private:
  int DeleteDoc(int64_t docid, int field, std::string &key);

  Table *table_;
  std::vector<std::shared_ptr<FieldRangeIndex>> fields_;
  StorageManager *storage_mgr_;
  int cf_id_;
};

}  // namespace vearch
