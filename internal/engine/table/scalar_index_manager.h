/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "scalar_index.h"
#include "inverted_index.h"
#include "bitmap_index.h"
#include "composite_index.h"
#include "scalar_index_result.h"
#include "storage/storage_manager.h"
#include "table.h"

namespace vearch {

enum class ResultStatus : int64_t { ZERO = 0, INTERNAL_ERR = -1, KILLED = -2 };

struct FilterIndexPair {
  ScalarIndex* index;
  CompositeIndex* composite_index;
  std::vector<FilterInfo> filters;
  bool is_composite;
  CompositeStrategy strategy;
  // For SCAN strategy only: how filters inside this bucket combine.
  // AND -> entry must satisfy every filter to be emitted.
  // OR  -> entry emitted if any filter matches.
  // Ignored when strategy != SCAN.
  FilterOperator inner_op = FilterOperator::And;
};

// ============================================================================
// ScalarIndexManager - Manages all scalar indexes for a table
// ============================================================================
class ScalarIndexManager {
 public:
  ScalarIndexManager(Table *table, StorageManager *storage_mgr);
  ~ScalarIndexManager();

  int Init(std::string space_name, std::vector<struct IndexInfo> indexes);

  int AddIndexes(std::vector<struct IndexInfo> &indexes);

  int AddDoc(int64_t docid);

  //update work as add and delete maybe only update one field
  int AddDoc(int64_t docid, int field);

  int DeleteDoc(int64_t docid);

  //update work as add and delete maybe only update one field
  int DeleteDoc(int64_t docid, int field);

  int AddIndex(int field, DataType data_type, const std::string &field_name,
               ScalarIndexType index_type);

  int RemoveIndex(int field);

  int OrganizeFiltersToIndex(const std::vector<FilterInfo>& filters,
    std::vector<FilterIndexPair>& filter_index_pairs, FilterOperator query_filter_operator);

  int Filter(ScalarIndex* scalar_idx, const FilterInfo &filter, ScalarIndexResult &result, int offset = 0, int limit = 0);

  /**
   * Try to use composite index for filtering.
   *
   */
  int CompositeFilter(CompositeIndex* composite_idx, const std::vector<FilterInfo>& filters,
    CompositeStrategy strategy, ScalarIndexResult& result);

  int64_t Search(FilterOperator query_filter_operator,
                 std::vector<FilterInfo> &origin_filters,
                 ScalarIndexResults *out);

  int64_t Query(FilterOperator query_filter_operator,
                std::vector<FilterInfo> &origin_filters,
                std::vector<uint64_t> &docids, size_t topn, size_t offset);

  // Rebuild bitmap index from storage for a specific field
  int RebuildBitmapIndex(int field_id);

  // Rebuild all bitmap fields from storage
  int RebuildAllBitmapIndexes();

 public:
  // Get field index by field id
  ScalarIndex* GetFieldIndex(int field) {
    auto it = field_indexes_.find(field);
    if (it != field_indexes_.end()) {
      return it->second.get();
    }
    return nullptr;
  }

  /**
   * Add a composite (multi-column) index. Registered by its first field ID.
   * @param field_ids   ordered list of field IDs (must have size >= 2)
   * @param field_types data types corresponding to each field_id
   * @return 0 on success
   */
  int AddCompositeIndex(const std::vector<int>& field_ids,
                        const std::vector<enum DataType>& data_types);

  /**
   * Get composite index by its header key.
   */
  ScalarIndex* GetCompositeIndex(const std::string& key);

 private:
  // Check if a field belongs to any composite index and update them.
  int UpdateCompositeIndexes(int64_t docid, int field_id, bool is_add);

  // OR-branch dispatch for OrganizeFiltersToIndex. Builds per-filter pairs:
  // scalar where available, else SCAN-bucket on a composite that owns the
  // field. Returns -1 when any field has no index that can serve it.
  int OrganizeFiltersForOr(const std::vector<FilterInfo>& filters,
                           std::vector<FilterIndexPair>& filter_index_pairs);

  /**
   * Execute EQUAL strategy: all composite fields have single-value filters (Eq mode).
   * Uses composite->Equal() for a single RocksDB seek.
   */
  void ExecuteEqualCase(CompositeIndex* composite_idx,
                        const std::vector<FilterInfo>& match_filters,
                        ScalarIndexResult& result);

  /**
   * Execute RANGE strategy: full composite match with at least one Range filter.
   * Uses composite->Range() with bounds on all fields; suffix fields use min/max.
   */
  void ExecuteRangeCase(CompositeIndex* composite_idx,
                        const std::vector<FilterInfo>& match_filters,
                        ScalarIndexResult& result);

  /**
   * Execute IN strategy: prefix + suffix Cartesian product of Eq/IN values.
   * Generates all combinations of field values and issues composite->In() or composite->Equal().
   */
  void ExecuteInCase(CompositeIndex* composite_idx,
                     const std::vector<FilterInfo>& match_filters,
                     ScalarIndexResult& result);

  /**
   * Execute NOT_IN strategy: NotIn on first field, other fields should be absent.
   */
  void ExecuteNotInCase(CompositeIndex* composite_idx,
                        const FilterInfo& filter,
                        ScalarIndexResult& result);

  /**
   * Execute NOT_EQUAL strategy: NotEqual on first field, other fields should be absent.
   */
  void ExecuteNotEqualCase(CompositeIndex* composite_idx,
                           const FilterInfo& filter,
                           ScalarIndexResult& result);

  /**
   * Execute SCAN strategy: full-iteration fallback when filters cannot form
   * a valid composite key prefix. Delegates to CompositeIndex::Scan.
   */
  void ExecuteScanCase(CompositeIndex* composite_idx,
                       const std::vector<FilterInfo>& match_filters,
                       FilterOperator inner_op,
                       ScalarIndexResult& result);

 public:
  Table *table_;
  std::map<int, std::shared_ptr<ScalarIndex>> field_indexes_;
  std::map<int, enum ScalarIndexType> field_index_types_;
  std::map<std::string, std::shared_ptr<CompositeIndex>> composite_indexes_;
  StorageManager *storage_mgr_;
  int cf_id_;
  std::string space_name_;
};

}  // namespace vearch
