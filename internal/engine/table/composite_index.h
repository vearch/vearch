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

#include "inverted_index.h"
#include "scalar_index.h"
#include "storage/storage_manager.h"

namespace vearch {

// ============================================================================
// CompositeIndex - RocksDB-based composite (multi-column) index
//
// RocksDB key format (binary):
//   [fid₁: 4B][fid₂: 4B]...[fidₙ: 4B]_[val₁][val₂]...[valₙ][docid: 8B]
//
//   field id is encoded to 4 bytes via ToRowKey in scalar_index_utils.h.
//   Numeric types are encoded to MemComparable via NumericToSortableStr.
//   STRINGARRAY fields are expanded into Cartesian-product key-value pairs.
//   STRING fields support only the IN operation. If a single value is provided,
//   it is equivalent to an Equal lookup. For an IN with multiple values,
//   each value is seeked independently and results are unioned.
//   STRING type are not fully MemComparable — they are encoded with a length prefix
//   to prevent false prefix matches (e.g. "abc" vs "abcd"). Range operations
//   are supported only on the last field.
//
// ============================================================================
class CompositeIndex : public InvertedIndex {
 public:
  /**
   * @param table          Table object
   * @param storage_mgr    Storage manager
   * @param cf_id          RocksDB column family ID for this index
   * @param data_types     Data types corresponding to each field_id
   * @param field_ids      Ordered list of field IDs (composite key fields)
   */
  CompositeIndex(Table* table, StorageManager* storage_mgr, int cf_id, std::vector<enum DataType> data_types, std::vector<int> field_ids);

  ~CompositeIndex() override = default;

  int AddDoc(int64_t docid) override;
  int DeleteDoc(int64_t docid) override;

  /**
   * Multi-field query methods — override inherited single-field signatures
   * to accept field-prefix-aware arguments.
   *
   * For a composite index with N fields (f0, f1, ..., fN-1):
   *   - Prefix length k (1 <= k <= N): f0 through f(k-1) are used as prefix
   *   - Equal: exact match on prefix fields
   *   - Range: exact match on prefix fields + range on next field only
   *   - In: exact match on prefix fields + cartesian product on remaining fields
   *
   * Range query constraint: Only the field immediately after the prefix can have
   * range constraints. This is due to RocksDB's lexicographic ordering.
   */

  /**
   * Match prefix fields with exact values.
   * @param prefix_values Values for prefix fields (f0, f1, ..., f(prefix_len-1))
   * @param offset Skip first N results
   * @param limit Return at most M results
   */
  ScalarIndexResult Equal(const std::vector<std::string>& prefix_values,
                         int offset, int limit);

  /**
   * Range query: exact match on prefix + range on next field only.
   *
   * IMPORTANT: Range can only handle fields up to the range field. Any fields
   * after the range field CANNOT be filtered correctly using this method,
   * because RocksDB lexicographic scan can only constrain the rightmost field.
   * Callers MUST NOT pass fields after range_idx to any query function.
   *
   * @param prefix_values Values for prefix fields (f0, f1, ..., f(prefix_len-1))
   * @param lower_value Lower bound for field at index prefix_len (empty = no lower bound)
   * @param upper_value Upper bound for field at index prefix_len (empty = no upper bound)
   * @param include_lower Whether lower bound is inclusive
   * @param include_upper Whether upper bound is inclusive
   * @param offset Skip first N results
   * @param limit Return at most M results
   */
  ScalarIndexResult Range(const std::vector<std::string>& prefix_values,
                        const std::string& lower_value,
                        const std::string& upper_value,
                        bool include_lower,
                        bool include_upper,
                        int offset, int limit);

  /**
   * Cartesian product query: exact match on prefix + IN on remaining fields.
   * @param prefix_values Values for prefix fields (f0, f1, ..., f(prefix_len-1))
   * @param field_values Values for fields from prefix_values.size() onwards [field_idx - prefix_values.size()][value_idx]
   * @param offset Skip first N results
   * @param limit Return at most M results
   */
  ScalarIndexResult In(const std::vector<std::string>& prefix_values,
                            const std::vector<std::vector<std::string>>& field_values,
                            int offset, int limit);

  const std::vector<int> GetFieldIds() const { return field_ids_; }
  const std::vector<enum DataType>& GetDataTypes() const { return data_types_; }

  /**
   * Get the number of fields in this composite index.
   */
  size_t NumFields() const { return field_ids_.size(); }

  bool IsIndexField(int field_idx) const { return std::find(field_ids_.begin(), field_ids_.end(), field_idx) != field_ids_.end(); }

  bool CanUseFilterMode(const std::vector<int>& field_ids, const std::vector<CompositeFilterMode>& modes, CompositeStrategy& strategy);

  /**
   * Check whether the given filter fields can be served by SCAN fallback.
   * SCAN does not require fields to form a key prefix; it iterates every
   * entry of the composite index and applies filters per entry.
   *
   * Requirements:
   *   - every query field must belong to this composite index
   *   - every mode must be point-decidable (Equal / Range / In / NotIn / NotEqual)
   *
   * @param query_field_ids fields appearing in the query (any order, may
   *                        repeat — e.g. OR-of-equals on the same field).
   * @param modes           corresponding filter modes, parallel to
   *                        query_field_ids.
   */
  bool CanUseScan(const std::vector<int>& query_field_ids,
                  const std::vector<CompositeFilterMode>& modes) const;

  /**
   * Filter scan over this composite index.
   *
   * Logically equivalent to iterating every entry under the composite
   * header, decoding each field value from the key, evaluating the
   * filters, and emitting matching docids. Internally narrows the
   * iterated range when the filter set permits: builds a leading-Equal
   * seek prefix from consecutive AND filters that pin a single value,
   * and may further split into per-value sub-scans (string IN) or a
   * range-bounded sub-scan (numeric Range). Cases that would compromise
   * per-docid aggregation (OR semantics, NotEqual / NotIn, multi-filter
   * on the same STRINGARRAY field) fall back to the full header scan.
   * Duplicates produced by STRINGARRAY cartesian expansion are folded
   * by ScalarIndexResult::Add.
   *
   * @param filters  filters to evaluate; field ids must belong to this index.
   * @param inner_op AND -> every filter must match; OR -> any filter matches.
   * @param offset/limit  applied to the final result set (post-filter).
   */
  ScalarIndexResult Scan(const std::vector<FilterInfo>& filters,
                         FilterOperator inner_op,
                         int offset, int limit);

  std::string GetHeaderKey() const { return header_key_; }

  // Drop all RocksDB entries that belong to this composite index.
  // Uses DeleteRange over [header_key_, AdvancePrefix(header_key_)). The upper
  // bound increments the prefix's last byte rather than appending 0xFF, because
  // a sortable-encoded field value can itself begin with 0xFF. header_key_ is
  // unique per composite, so this prefix range stays exclusive.
  int DropAll() override;

  // Override InvertedIndex's key helpers with composite binary format:
  // Format: [\xFF: 1B][fid₁: 4B][...][fidₙ: 4B][val₁][...][valₙ][docid: 8B]
  std::string GenKeyPrefix(const std::string& index_value) const override;
  std::string GenKey(const std::string& index_value, int64_t docid) const override;

 private:
  void GenCompositeKey(int64_t docid, std::vector<std::string>* composite_keys);

  /** Get the RocksDB column family handle. */
  rocksdb::ColumnFamilyHandle* CfHandler() const;

  std::vector<int> field_ids_;
  std::vector<enum DataType> data_types_;
  std::string header_key_;
};

}  // namespace vearch
